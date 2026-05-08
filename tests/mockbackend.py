import asyncio
import json

import aiohttp
import aiohttp.web_exceptions


async def health(req):
    return aiohttp.web.Response()


def close_transport(req):
    if req.transport is not None:
        req.transport.close()


async def chat(req):
    b = await req.json()
    if b["model"] != "mymodel":
        raise aiohttp.web_exceptions.HTTPBadRequest(body="bad model")

    msg = b["messages"][0]["content"]

    return aiohttp.web.json_response({
        "usage": {"prompt_tokens": 1, "completion_tokens": 2},
        "choices": [{"message": {"content": "you said: %s" % msg}}]
    })


async def responses(req):
    b = await req.json()
    req.app["responses_calls"].append(b)

    if b["model"] != "mymodel":
        raise aiohttp.web_exceptions.HTTPBadRequest(body="bad model")

    if b.get("input") == "upstream_error":
        return aiohttp.web.Response(
            body=b'{"error":"bad responses request"}',
            status=429,
            headers={"Content-Type": "application/json"},
        )

    if b.get("input") == "upstream_error_body_error":
        res = aiohttp.web.StreamResponse(
            status=429,
            headers={"Content-Type": "application/json"})
        await res.prepare(req)
        await res.write(b'{"error":')
        close_transport(req)
        return res

    if b.get("input") == "missing_usage":
        return aiohttp.web.Response(
            body=b'{"id":"resp_missing","object":"response"}',
            headers={"Content-Type": "application/json"},
        )

    if b.get("input") == "invalid_usage":
        return aiohttp.web.Response(
            body=b'{"id":"resp_invalid","usage":{"input_tokens":"3",'
                 b'"output_tokens":5}}',
            headers={"Content-Type": "application/json"},
        )

    if b.get("input") == "negative_usage":
        return aiohttp.web.Response(
            body=b'{"id":"resp_invalid","usage":{"input_tokens":-1,'
                 b'"output_tokens":5}}',
            headers={"Content-Type": "application/json"},
        )

    if b.get("input") == "non_stream_body_error":
        res = aiohttp.web.StreamResponse(
            headers={"Content-Type": "application/json"})
        await res.prepare(req)
        await res.write(b'{"id":"resp_partial",')
        close_transport(req)
        return res

    if b.get("input") == "chunked_json":
        res = aiohttp.web.StreamResponse(
            headers={"Content-Type": "application/json"})
        await res.prepare(req)
        await res.write(b'{"id":"resp_chunked",')
        await res.write(b'"usage":{"input_tokens":7,"output_tokens":11}}')
        await res.write_eof()
        return res

    if b.get("stream"):
        res = aiohttp.web.StreamResponse(
            headers={"Content-Type": "text/event-stream"})
        await res.prepare(req)
        await res.write(
            b'event: response.output_text.delta\n'
            b'data: {"delta":"hello"}\n\n')

        if b.get("input") == "backend_error_before_completed":
            close_transport(req)
            return res

        if b.get("input") == "client_disconnect_stream":
            await asyncio.sleep(0.05)

        if b.get("input") == "large_event_stream":
            await res.write(
                b"event: response.completed\n"
                b"data: " + (b"x" * 128) + b"\n\n")
        elif b.get("input") == "completed_null_stream":
            await res.write(
                b"event: response.completed\n"
                b"data: null\n\n")
        elif b.get("input") == "completed_response_null_stream":
            await res.write(
                b"event: response.completed\n"
                b'data: {"response": null}\n\n')
        elif b.get("input") == "completed_array_stream":
            await res.write(
                b"event: response.completed\n"
                b"data: []\n\n")
        elif b.get("input") == "multiline_usage_stream":
            await res.write(
                b'event: response.completed\n'
                b'data: {"response":\n'
                b'data: {"usage":{"input_tokens":13,\n'
                b'data: "output_tokens":17}}}\n\n')
        elif b.get("input") == "invalid_usage_stream":
            await res.write(
                b'event: response.completed\n'
                b'data: {"response":{"usage":{"input_tokens":"3",'
                b'"output_tokens":5}}}\n\n')
        elif b.get("input") == "negative_usage_stream":
            await res.write(
                b'event: response.completed\n'
                b'data: {"response":{"usage":{"input_tokens":-1,'
                b'"output_tokens":5}}}\n\n')
        elif b.get("input") != "missing_usage_stream":
            await res.write(
                b'event: response.completed\n'
                b'data: {"response":{"usage":{"input_tokens":3,'
                b'"output_tokens":5}}}\n\n')

        if b.get("input") == "backend_error_after_completed":
            close_transport(req)
            return res

        await res.write_eof()
        return res

    body = {
        "id": "resp_123",
        "object": "response",
        "output_text": "you said: %s" % b["input"],
        "usage": {"input_tokens": 3, "output_tokens": 5},
    }
    return aiohttp.web.Response(
        body=json.dumps(body, separators=(",", ":")).encode(),
        headers={"Content-Type": "application/json"},
    )


def create_app():
    app = aiohttp.web.Application()
    app["responses_calls"] = []
    app.add_routes([
        aiohttp.web.get("/health", health),
        aiohttp.web.post("/v1/chat/completions", chat),
        aiohttp.web.post("/v1/responses", responses),
    ])
    return app
