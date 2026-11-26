import asyncio
import unittest
from types import SimpleNamespace
from flask import Flask
from signaldeck_sdk import ValueProvider
from signaldeck_core.routes import register_routes
from signaldeck_core.services.publish_service import PublisherRegistry, PublishService


class Receiver:
    name = "receiver"
    def processHTTPCall(self, **payload):
        return {"payload": payload}


class HttpTests(unittest.TestCase):
    def test_form_query_and_json(self):
        vp = ValueProvider()
        vp.registerProcessor(Receiver(), {"http": [
            {"name": "legacy", "values": {}},
            {"name": "new", "values": {}, "receive": True},
        ]})
        app = Flask(__name__)
        app.extensions["signaldeck.manager"] = SimpleNamespace(valueProvider=vp)
        register_routes(app)
        with app.test_client() as client:
            self.assertEqual(client.get("/http/legacy?a=1").json, {"payload": {"a": "1"}})
            self.assertEqual(client.post("/http/legacy", data={"a": "1"}).json, {"payload": {"a": "1"}})
            self.assertEqual(client.post("/http/new", json={"a": 1}).json, {"payload": {"a": 1}})
            self.assertEqual(client.get("/http/new?a=1").status_code, 405)
            self.assertEqual(client.post("/http/new", json=[1]).status_code, 400)
            self.assertEqual(client.post("/http/missing", json={"a": 1}).status_code, 404)


class PublishTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_and_cancel(self):
        event = asyncio.Event()
        sent = []
        class Transport:
            async def publish(self, data):
                sent.append(data)
                event.set()
            async def shutdown(self):
                sent.append("closed")
        registry = PublisherRegistry()
        registry.register("test", lambda config: Transport())
        values = SimpleNamespace(
            values={"x": None},
            publish_definitions={("source", "out"): {
                "type": "test", "interval_seconds": 10, "values": {"a": "x"}
            }},
            getPublishValues=lambda mapping: {"a": 42},
        )
        service = PublishService(values, registry)
        self.assertEqual(service.tasks(False), [])
        task = asyncio.create_task(service.tasks(True)[0])
        await asyncio.wait_for(event.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(sent, [{"a": 42}, "closed"])
