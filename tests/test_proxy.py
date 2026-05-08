import asyncio
import hashlib
import importlib
import importlib.resources
import os
import sqlite3
import tempfile
import warnings

import aiohttp
import aiohttp.test_utils

from llmproxy import responses as responses_module
from llmproxy.app import create_app
from llmproxy.db import get_db

from . import mockbackend


class LLMProxyAppTestCase(aiohttp.test_utils.AioHTTPTestCase):
    async def asyncSetUp(self):
        # Don't care about type checkers
        warnings.simplefilter("ignore", category=aiohttp.web.NotAppKeyWarning)

        self.backend = aiohttp.test_utils.TestServer(mockbackend.create_app())
        await self.backend.start_server()

        await super().asyncSetUp()

    async def asyncTearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)
        await super().asyncTearDown()

    async def get_application(self):
        self.db_fd, self.db_path = tempfile.mkstemp()

        app = await create_app({
            "timeout_connect": 1,
            "timeout_read": 1,
            "db": {"uri": "sqlite://%s" % self.db_path},
            "backends": {"mymodel":
                {"url": "http://%s:%d" % (self.backend.host, self.backend.port),
                    "token": "mybackendtoken", "device": "none"},
            },
        })

        # Insert test user
        secret = hashlib.sha256("mytoken".encode()).hexdigest()
        db = await get_db(app["config"]["db"]["uri"])
        await db.db.execute("""
            INSERT INTO api_key (id, secret, type) VALUES ('myuser', ?, 'LLM')
            """, (secret,))
        await db.db.commit()
        await db.close()

        return app

    async def get_events(self):
        db = await get_db(self.app["config"]["db"]["uri"])
        cur = await db.db.execute("SELECT product, quantity FROM event_oneoff")
        rows = await cur.fetchall()
        await db.close()

        return rows

    async def wait_for_events(self, count):
        for _ in range(20):
            rows = await self.get_events()
            if len(rows) == count:
                return rows
            await asyncio.sleep(0.05)

        return await self.get_events()


class TestChat(LLMProxyAppTestCase):
    async def test_simple(self):
        body = {"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}
        req = self.client.request("POST", "/v1/chat/completions",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            data = await res.json()

        self.assertEqual(data["choices"][0]["message"]["content"], "you said: hi")

        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 1},
            {"product": "mymodel/none/completion", "quantity": 2},
        ])

    async def test_unknown_token(self):
        body = {"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}
        req = self.client.request("POST", "/v1/chat/completions",
            headers={"Authorization": "Bearer badtoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 401)

        self.assertListEqual(await self.get_events(), [])

    async def test_blank_token(self):
        body = {"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}
        req = self.client.request("POST", "/v1/chat/completions",
            headers={"Authorization": "Bearer "}, json=body)

        async with req as res:
            self.assertEqual(res.status, 401)

        self.assertListEqual(await self.get_events(), [])


class TestResponses(LLMProxyAppTestCase):
    async def test_sanitized_backend_url_removes_credentials_and_query(self):
        url = "http://user:pass@example.com:8080/backend?token=secret#frag"
        self.assertEqual(
            responses_module.sanitized_backend_url({"url": url}),
            "http://example.com:8080/backend")

    async def test_sse_parser_accepts_supported_event_separators(self):
        for separator in [b"\n\n", b"\r\n\r\n", b"\r\r"]:
            with self.subTest(separator=separator):
                buffer = bytearray()
                raw = b"event: one" + separator + b"event: two" + separator
                self.assertEqual(
                    list(responses_module.iter_sse_blocks(buffer, raw)),
                    [b"event: one" + separator, b"event: two" + separator])
                self.assertEqual(buffer, bytearray())

    async def test_non_stream_simple(self):
        body = {
            "model": "mymodel",
            "input": "hi",
            "text": {"format": {"type": "text"}},
            "reasoning": {"effort": "low"},
        }
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            self.assertEqual(
                res.headers["Content-Type"].partition(";")[0],
                "application/json")
            raw = await res.read()

        self.assertEqual(raw,
            b'{"id":"resp_123","object":"response","output_text":'
            b'"you said: hi","usage":{"input_tokens":3,"output_tokens":5}}')
        self.assertEqual(self.backend.app["responses_calls"], [body])
        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 3},
            {"product": "mymodel/none/completion", "quantity": 5},
        ])

    async def test_stream_simple(self):
        body = {"model": "mymodel", "input": "hi", "stream": True}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            self.assertEqual(
                res.headers["Content-Type"].partition(";")[0],
                "text/event-stream")
            raw = await res.read()

        self.assertIn(
            b'event: response.output_text.delta\n'
            b'data: {"delta":"hello"}\n\n',
            raw)
        self.assertIn(b"event: response.completed\n", raw)
        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 3},
            {"product": "mymodel/none/completion", "quantity": 5},
        ])

    async def test_stream_multiline_completed_usage(self):
        body = {
            "model": "mymodel",
            "input": "multiline_usage_stream",
            "stream": True,
        }
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            raw = await res.read()

        self.assertIn(b"event: response.completed\n", raw)
        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 13},
            {"product": "mymodel/none/completion", "quantity": 17},
        ])

    async def test_chunked_json_is_not_treated_as_sse(self):
        body = {
            "model": "mymodel",
            "input": "chunked_json",
            "stream": True,
        }
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            self.assertEqual(
                res.headers["Content-Type"].partition(";")[0],
                "application/json")
            raw = await res.read()

        self.assertEqual(raw,
            b'{"id":"resp_chunked","usage":{"input_tokens":7,'
            b'"output_tokens":11}}')
        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 7},
            {"product": "mymodel/none/completion", "quantity": 11},
        ])

    async def test_rejects_stateful_or_background_requests(self):
        bodies = [
            {"model": "mymodel", "input": "hi", "background": True},
            {"model": "mymodel", "input": "hi",
                "previous_response_id": "resp_123"},
            {"model": "mymodel", "input": "hi", "conversation": "conv_123"},
        ]

        for body in bodies:
            with self.subTest(body=body):
                req = self.client.request("POST", "/v1/responses",
                    headers={"Authorization": "Bearer mytoken"}, json=body)

                async with req as res:
                    self.assertEqual(res.status, 422)

        self.assertEqual(self.backend.app["responses_calls"], [])
        self.assertListEqual(await self.get_events(), [])

    async def test_unknown_token(self):
        body = {"model": "mymodel", "input": "hi"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer badtoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 401)

        self.assertEqual(self.backend.app["responses_calls"], [])
        self.assertListEqual(await self.get_events(), [])

    async def test_blank_token(self):
        body = {"model": "mymodel", "input": "hi"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer "}, json=body)

        async with req as res:
            self.assertEqual(res.status, 401)

        self.assertEqual(self.backend.app["responses_calls"], [])
        self.assertListEqual(await self.get_events(), [])

    async def test_unknown_model(self):
        body = {"model": "unknown", "input": "hi"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 401)

        self.assertEqual(self.backend.app["responses_calls"], [])
        self.assertListEqual(await self.get_events(), [])

    async def test_upstream_error_is_passed_through(self):
        body = {"model": "mymodel", "input": "upstream_error"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 429)
            self.assertEqual(
                res.headers["Content-Type"].partition(";")[0],
                "application/json")
            raw = await res.read()

        self.assertEqual(raw, b'{"error":"bad responses request"}')
        self.assertListEqual(await self.get_events(), [])

    async def test_upstream_error_body_read_failure_returns_502(self):
        body = {"model": "mymodel", "input": "upstream_error_body_error"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 502)

        self.assertListEqual(await self.get_events(), [])

    async def test_non_stream_missing_usage_returns_502(self):
        body = {"model": "mymodel", "input": "missing_usage"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 502)

        self.assertListEqual(await self.get_events(), [])

    async def test_non_stream_invalid_usage_returns_502(self):
        for input_ in ["invalid_usage", "negative_usage"]:
            with self.subTest(input=input_):
                body = {"model": "mymodel", "input": input_}
                req = self.client.request("POST", "/v1/responses",
                    headers={"Authorization": "Bearer mytoken"}, json=body)

                async with req as res:
                    self.assertEqual(res.status, 502)

        self.assertListEqual(await self.get_events(), [])

    async def test_non_stream_body_read_failure_returns_502(self):
        body = {"model": "mymodel", "input": "non_stream_body_error"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 502)

        self.assertListEqual(await self.get_events(), [])

    async def test_stream_missing_usage_logs_critical_without_billing(self):
        body = {
            "model": "mymodel",
            "input": "missing_usage_stream",
            "stream": True,
        }

        with self.assertLogs(self.app.logger, level="CRITICAL") as logs:
            req = self.client.request("POST", "/v1/responses",
                headers={"Authorization": "Bearer mytoken"}, json=body)

            async with req as res:
                self.assertEqual(res.status, 200)
                raw = await res.read()

            await asyncio.sleep(0)

        self.assertIn(b"response.output_text.delta", raw)
        self.assertIn("responses_stream_billing_missing", "\n".join(logs.output))
        self.assertIn('"api_key_id": "myuser"', "\n".join(logs.output))
        self.assertIn('"backend_model": "mymodel"', "\n".join(logs.output))
        self.assertListEqual(await self.get_events(), [])

    async def test_stream_backend_failure_before_completed_logs_critical(self):
        body = {
            "model": "mymodel",
            "input": "backend_error_before_completed",
            "stream": True,
        }

        with self.assertLogs(self.app.logger, level="CRITICAL") as logs:
            req = self.client.request("POST", "/v1/responses",
                headers={"Authorization": "Bearer mytoken"}, json=body)

            async with req as res:
                self.assertEqual(res.status, 200)
                raw = await res.read()

        self.assertIn(b"response.output_text.delta", raw)
        log_output = "\n".join(logs.output)
        self.assertIn("responses_stream_billing_missing", log_output)
        self.assertIn('"billing_recorded": false', log_output)
        self.assertListEqual(await self.get_events(), [])

    async def test_stream_backend_failure_after_completed_still_bills(self):
        body = {
            "model": "mymodel",
            "input": "backend_error_after_completed",
            "stream": True,
        }
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            raw = await res.read()

        self.assertIn(b"event: response.completed\n", raw)
        self.assertListEqual(await self.get_events(), [
            {"product": "mymodel/none/prompt", "quantity": 3},
            {"product": "mymodel/none/completion", "quantity": 5},
        ])

    async def test_stream_invalid_usage_logs_critical_without_billing(self):
        for input_ in [
                "invalid_usage_stream",
                "negative_usage_stream",
                "completed_null_stream",
                "completed_response_null_stream",
                "completed_array_stream",
                ]:
            with self.subTest(input=input_):
                body = {"model": "mymodel", "input": input_, "stream": True}

                with self.assertLogs(self.app.logger, level="CRITICAL") as logs:
                    req = self.client.request("POST", "/v1/responses",
                        headers={"Authorization": "Bearer mytoken"}, json=body)

                    async with req as res:
                        self.assertEqual(res.status, 200)
                        await res.read()

                log_output = "\n".join(logs.output)
                self.assertIn("responses_stream_billing_missing", log_output)
                self.assertIn('"reason": "usage_invalid"', log_output)

        self.assertListEqual(await self.get_events(), [])

    async def test_stream_large_event_logs_critical_without_billing(self):
        body = {"model": "mymodel", "input": "large_event_stream", "stream": True}
        old_limit = responses_module.MAX_SSE_EVENT_BYTES
        responses_module.MAX_SSE_EVENT_BYTES = 64
        try:
            with self.assertLogs(self.app.logger, level="CRITICAL") as logs:
                req = self.client.request("POST", "/v1/responses",
                    headers={"Authorization": "Bearer mytoken"}, json=body)

                async with req as res:
                    self.assertEqual(res.status, 200)
                    raw = await res.read()
        finally:
            responses_module.MAX_SSE_EVENT_BYTES = old_limit

        self.assertIn(b"response.output_text.delta", raw)
        log_output = "\n".join(logs.output)
        self.assertIn("responses_stream_billing_missing", log_output)
        self.assertIn('"reason": "sse_event_too_large"', log_output)
        self.assertListEqual(await self.get_events(), [])

    async def test_stream_client_disconnect_still_bills_from_completed(self):
        body = {
            "model": "mymodel",
            "input": "client_disconnect_stream",
            "stream": True,
        }
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 200)
            first = await res.content.read(1024)
            self.assertIn(b"response.output_text.delta", first)

        self.assertListEqual(await self.wait_for_events(2), [
            {"product": "mymodel/none/prompt", "quantity": 3},
            {"product": "mymodel/none/completion", "quantity": 5},
        ])

    async def test_backend_connection_error_returns_502(self):
        self.app["config"]["backends"]["mymodel"]["url"] = "http://127.0.0.1:1"
        body = {"model": "mymodel", "input": "hi"}
        req = self.client.request("POST", "/v1/responses",
            headers={"Authorization": "Bearer mytoken"}, json=body)

        async with req as res:
            self.assertEqual(res.status, 502)

        self.assertListEqual(await self.get_events(), [])
