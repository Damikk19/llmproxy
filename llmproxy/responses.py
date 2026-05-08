import asyncio
import datetime
import decimal
import json
import urllib.parse
from dataclasses import dataclass

import aiohttp
import aiohttp.web

from . import auth, proxy
from .db import DatabaseError, get_db


SSE_READ_CHUNK_SIZE = 65536
MAX_SSE_EVENT_BYTES = 64 * 1024 * 1024
SSE_SEPARATORS = (b"\r\n\r\n", b"\n\n", b"\r\r")


class SSEEventTooLarge(Exception):
    pass


@dataclass
class StreamResult:
    response: aiohttp.web.StreamResponse
    usage: dict | None = None
    completed_seen: bool = False
    invalid_usage_seen: bool = False
    error_reason: str | None = None
    error: Exception | None = None


def validate_stateless_create(body):
    if body.get("background") is True:
        raise aiohttp.web.HTTPUnprocessableEntity(
            text="background=true is not supported by this proxy")
    if "previous_response_id" in body:
        raise aiohttp.web.HTTPUnprocessableEntity(
            text="previous_response_id is not supported by this proxy")
    if "conversation" in body:
        raise aiohttp.web.HTTPUnprocessableEntity(
            text="conversation is not supported by this proxy")


def is_sse_response(res):
    ctype = res.headers.get("Content-Type", "")
    return ctype.partition(";")[0].lower() == "text/event-stream"


def response_headers(res):
    return {"Content-Type":
        res.headers.get("Content-Type", "application/octet-stream")}


def find_sse_separator(buffer):
    found = None
    for sep in SSE_SEPARATORS:
        idx = buffer.find(sep)
        if idx == -1:
            continue
        if found is None or idx < found[0]:
            found = (idx, len(sep))

    return found


def iter_sse_blocks(buffer, chunk):
    buffer.extend(chunk)

    while (found := find_sse_separator(buffer)) is not None:
        idx, sep_len = found
        end = idx + sep_len
        if end > MAX_SSE_EVENT_BYTES:
            raise SSEEventTooLarge("SSE event exceeded maximum size")
        block = bytes(buffer[:end])
        del buffer[:end]
        yield block

    if len(buffer) > MAX_SSE_EVENT_BYTES:
        raise SSEEventTooLarge("SSE event exceeded maximum size")


def sse_field(line):
    if not line or line.startswith(":"):
        return None, None

    field, sep, value = line.partition(":")
    if not sep:
        return field, ""
    if value.startswith(" "):
        value = value[1:]

    return field, value


def usage_from_sse_block(block):
    event = None
    data = []

    for line in block.decode("utf-8", errors="replace").splitlines():
        field, value = sse_field(line)
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)

    if event != "response.completed":
        return False, None

    try:
        payload = json.loads("\n".join(data), parse_float=decimal.Decimal)
    except json.decoder.JSONDecodeError:
        return True, None

    if not isinstance(payload, dict):
        return True, None

    response = payload.get("response")
    if not isinstance(response, dict):
        return True, None

    return True, response.get("usage")


def validate_usage(usage):
    if not isinstance(usage, dict):
        return None

    try:
        input_tokens = usage["input_tokens"]
        output_tokens = usage["output_tokens"]
    except KeyError:
        return None

    if type(input_tokens) is not int or type(output_tokens) is not int:
        return None
    if input_tokens < 0 or output_tokens < 0:
        return None

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


def usage_resources(usage, b_name, b_cfg):
    usage = validate_usage(usage)
    if usage is None:
        raise KeyError("invalid Responses usage")

    return {
        "%s/%s/prompt" % (b_name, b_cfg["device"]): usage["input_tokens"],
        "%s/%s/completion" % (b_name, b_cfg["device"]):
            usage["output_tokens"],
    }


def has_billable_usage(usage):
    return validate_usage(usage) is not None


async def record_billing(
        f_req, user, usage, b_name, b_cfg, db_error_reason=None):
    usage = validate_usage(usage)
    if usage is None:
        raise aiohttp.web.HTTPBadGateway(text="Invalid usage information")

    db = await get_db(f_req.app["config"]["db"]["uri"], f_req)
    try:
        resources = usage_resources(usage, b_name, b_cfg)
    except KeyError as e:
        raise aiohttp.web.HTTPBadGateway(text="Missing usage information") from e

    try:
        await db.billing_record_add(
            user=user,
            time=datetime.datetime.now(datetime.UTC),
            resources=resources,
            request_id=f_req["request_id"],
        )
    except DatabaseError as e:
        if db_error_reason is not None:
            log_responses_billing_missing(
                f_req, user, b_name, b_cfg, db_error_reason, e)
        else:
            f_req.app.logger.critical(e)
        raise aiohttp.web.GracefulExit() from e

    f_req.app.logger.info("Client used: P:%d C:%d tokens of %s",
        usage["input_tokens"], usage["output_tokens"], b_name)


async def read_backend_body(f_req, b_res, b_name):
    try:
        return await b_res.content.read()
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        f_req.app.logger.error("Backend \"%s\" body read error: %s", b_name, e)
        raise aiohttp.web.HTTPBadGateway() from e


async def passthrough_response(f_req, b_res, b_name):
    body = await read_backend_body(f_req, b_res, b_name)
    return aiohttp.web.Response(
        body=body,
        status=b_res.status,
        headers=response_headers(b_res),
    )


async def handle_non_stream(f_req, b_res, user, b_name, b_cfg):
    body = await read_backend_body(f_req, b_res, b_name)
    try:
        data = json.loads(body, parse_float=decimal.Decimal)
        usage = validate_usage(data["usage"])
    except (json.decoder.JSONDecodeError, KeyError, TypeError,
            UnicodeDecodeError) as e:
        f_req.app.logger.error("Missing Responses usage information")
        raise aiohttp.web.HTTPBadGateway(
            text="Missing usage information") from e
    if usage is None:
        f_req.app.logger.error("Invalid Responses usage information")
        raise aiohttp.web.HTTPBadGateway(text="Invalid usage information")

    await record_billing(f_req, user, usage, b_name, b_cfg)

    return aiohttp.web.Response(body=body, headers=response_headers(b_res))


def update_stream_usage(result, block):
    completed, usage = usage_from_sse_block(block)
    if not completed:
        return

    if result.completed_seen:
        result.invalid_usage_seen = True
        return

    result.completed_seen = True
    if (valid := validate_usage(usage)) is not None:
        result.usage = valid
    else:
        result.invalid_usage_seen = True


def client_disconnected(f_req):
    transport = f_req.transport
    return transport is None or transport.is_closing()


def process_stream_chunk(app, result, buffer, chunk):
    try:
        for block in iter_sse_blocks(buffer, chunk):
            update_stream_usage(result, block)
    except SSEEventTooLarge as e:
        result.error_reason = "sse_event_too_large"
        result.error = e
        app.logger.error("Backend SSE event too large: %s", e)
        return False

    return True


async def drain_backend_stream(app, result, buffer, b_res):
    try:
        async for c in b_res.content.iter_chunked(SSE_READ_CHUNK_SIZE):
            if not process_stream_chunk(app, result, buffer, c):
                break
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        result.error_reason = "backend_stream_read_error"
        result.error = e
        app.logger.error("Backend stream read error: %s", e)


async def handle_resp_stream(f_req, b_res):
    app = f_req.app
    f_res = aiohttp.web.StreamResponse(headers=response_headers(b_res))

    f_res.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    if o := app["config"].get("http_origin"):
        f_res.headers["Access-Control-Allow-Origin"] = o

    result = StreamResult(response=f_res)
    buffer = bytearray()
    client_connected = True

    try:
        await f_res.prepare(f_req)
    except OSError as e:
        client_connected = False
        app.logger.info("Client disconnected before stream prepare: %s", e)
    except asyncio.CancelledError as e:
        if not client_disconnected(f_req):
            raise
        client_connected = False
        app.logger.info("Client disconnected before stream prepare: %s", e)

    try:
        async for c in b_res.content.iter_chunked(SSE_READ_CHUNK_SIZE):
            if not process_stream_chunk(app, result, buffer, c):
                break

            if client_connected:
                try:
                    await f_res.write(c)
                except OSError as e:
                    client_connected = False
                    app.logger.info("Client disconnected: %s", e)
                except asyncio.CancelledError as e:
                    if not client_disconnected(f_req):
                        raise
                    client_connected = False
                    app.logger.info("Client disconnected: %s", e)
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        result.error_reason = "backend_stream_read_error"
        result.error = e
        app.logger.error("Backend stream read error: %s", e)
    except asyncio.CancelledError as e:
        if not client_disconnected(f_req):
            raise
        client_connected = False
        app.logger.info("Client disconnected during backend stream read: %s", e)
        await drain_backend_stream(app, result, buffer, b_res)

    if client_connected:
        try:
            await f_res.write_eof()
        except OSError as e:
            app.logger.info("Client disconnected before stream EOF: %s", e)
        except asyncio.CancelledError as e:
            if not client_disconnected(f_req):
                raise
            app.logger.info("Client disconnected before stream EOF: %s", e)

    return result


def sanitized_backend_url(b_cfg):
    url = b_cfg.get("url", "")
    try:
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname or ""
        if ":" in host and not host.startswith("["):
            host = "[%s]" % host
        if parts.port is not None:
            host = "%s:%d" % (host, parts.port)
        return urllib.parse.urlunsplit(
            (parts.scheme, host, parts.path, "", ""))
    except ValueError:
        url = url.split("?", 1)[0].split("#", 1)[0]
        scheme, sep, rest = url.partition("://")
        if sep:
            rest = rest.rsplit("@", 1)[-1]
            return "%s://%s" % (scheme, rest)
        return url.rsplit("@", 1)[-1]


def log_responses_billing_missing(
        f_req, user, b_name, b_cfg, reason, error=None):
    payload = {
        "event": "responses_stream_billing_missing",
        "request_id": str(f_req["request_id"]),
        "api_key_id": user["id"],
        "model": b_name,
        "backend": sanitized_backend_url(b_cfg),
        "backend_model": b_cfg.get("model", b_name),
        "billing_recorded": False,
        "reason": reason,
    }
    if error is not None:
        payload["error"] = type(error).__name__
    f_req.app.logger.critical(json.dumps(payload))


def stream_missing_reason(result):
    if result.error_reason == "sse_event_too_large":
        return result.error_reason
    if result.invalid_usage_seen:
        return "usage_invalid"
    if result.error_reason is not None:
        return result.error_reason
    if result.completed_seen:
        return "usage_missing"
    return "usage_not_found"


def backend_request_exception(e):
    if isinstance(e, (aiohttp.ServerTimeoutError, TimeoutError)):
        return aiohttp.web.HTTPGatewayTimeout()
    if isinstance(e, (aiohttp.ClientError, OSError)):
        return aiohttp.web.HTTPBadGateway()
    return None


async def handle_backend_response(f_req, b_res, user, b_name, b_cfg,
        stream_requested):
    f_req.app.logger.debug("Backend request completed")

    if b_res.status != 200:
        f_req.app.logger.error("Backend \"%s\" error: %d", b_name,
            b_res.status)
        return await passthrough_response(f_req, b_res, b_name)

    if is_sse_response(b_res):
        result = await handle_resp_stream(f_req, b_res)
        if (result.error_reason == "sse_event_too_large"
                or result.invalid_usage_seen
                or not has_billable_usage(result.usage)):
            log_responses_billing_missing(
                f_req, user, b_name, b_cfg, stream_missing_reason(result),
                result.error)
            return result.response

        await record_billing(
            f_req, user, result.usage, b_name, b_cfg,
            db_error_reason="billing_db_error")
        return result.response

    if stream_requested:
        f_req.app.logger.warning("Responses stream requested but backend returned %s",
            b_res.headers.get("Content-Type", ""))

    return await handle_non_stream(f_req, b_res, user, b_name, b_cfg)


# Frontend related variables are prefixed with f_.
# Backend related variables are prefixed with b_.
async def responses(f_req):
    user = await auth.require_auth(f_req)
    stream_requested = False

    def prepare_body(body):
        nonlocal stream_requested
        validate_stateless_create(body)
        stream_requested = body.get("stream") is True

    b_req, b_name, b_cfg = await proxy.request(f_req, prepare_body)
    try:
        async with b_req as b_res:
            return await handle_backend_response(
                f_req, b_res, user, b_name, b_cfg, stream_requested)
    except (aiohttp.ClientError, TimeoutError, OSError) as e:
        f_req.app.logger.error("Backend \"%s\" request error: %s", b_name, e)
        http_exc = backend_request_exception(e)
        if http_exc is not None:
            raise http_exc from e
        raise
