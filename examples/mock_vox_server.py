"""A local Vox server built from the published AsyncAPI spec, so demos and tests need no key.

It answers like Vox (connected -> audio_chunk... -> complete, or error) but speaks a
tone per voice instead of words. Two switches inject the failures that matter:
busy_first (the plan's concurrency slots are taken) and no_credits.

    python examples/mock_vox_server.py   # ws://127.0.0.1:8765/api/v1/get_speech, key "mock-key"
"""

import asyncio
import base64
import json
import math
import struct
from http import HTTPStatus

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

VOICE_HZ = {"Ogbs15oBevLzXsUuTtA1": 196, "Owbs15oBevLzXsUurdA_": 220, "PAbs15oBevLzXsUu4dCi": 262,
            "PQbt15oBevLzXsUuNtD3": 294, "Pgbt15oBevLzXsUubdA6": 330}
SECONDS_PER_CHAR = 0.012


def tone(hz: float, rate: int, seconds: float) -> bytes:
    """16-bit mono PCM sine wave: the mock's stand-in for speech."""
    n = int(rate * seconds)
    return struct.pack(f"<{n}h", *(int(8000 * math.sin(2 * math.pi * hz * i / rate)) for i in range(n)))


class MockVox:
    def __init__(self, api_key="mock-key", port=0, chunk_delay_s=0.0, busy_first=0, no_credits=False):
        self.api_key, self.port, self.chunk_delay_s = api_key, port, chunk_delay_s
        self.busy_first, self.no_credits = busy_first, no_credits
        self.requests, self.errors, self.connections, self.active, self.max_active = [], [], 0, 0, 0

    async def __aenter__(self):
        self._server = await serve(self._handle, "127.0.0.1", self.port, process_request=self._auth)
        self.url = f"ws://127.0.0.1:{self._server.sockets[0].getsockname()[1]}/api/v1/get_speech"
        return self

    async def __aexit__(self, *exc):
        self._server.close()
        await self._server.wait_closed()

    def _auth(self, connection, request):
        if request.headers.get("Authorization") != f"Bearer {self.api_key}":
            return connection.respond(HTTPStatus.UNAUTHORIZED, "invalid API key\n")

    async def _handle(self, ws):
        self.connections += 1
        await ws.send(json.dumps({"type": "connected", "connection_id": f"conn_{self.connections}",
                                  "message": "Connected successfully"}))
        async for raw in ws:  # requests may overlap, so each gets its own task
            asyncio.create_task(self._speak(ws, json.loads(raw)))

    async def _speak(self, ws, req):
        self.requests.append(req)
        rid = req["request_id"]
        error = "insufficient_credits" if self.no_credits else None
        if self.busy_first and not error:
            self.busy_first, error = self.busy_first - 1, "concurrency_limit_exceeded"
        if error:
            self.errors.append(error)
            return await ws.send(json.dumps({"type": "error", "request_id": rid, "error": error,
                                             "message": error.replace("_", " ")}))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            rate = req.get("sample_rate", 24000)
            audio = tone(VOICE_HZ[req["voice_id"]], rate, len(req["text"]) * SECONDS_PER_CHAR)
            step = rate * 2 * 40 // 1000  # 40 ms of 16-bit audio per chunk
            for i in range(0, len(audio), step):
                data = base64.b64encode(audio[i : i + step]).decode()
                await ws.send(json.dumps({"type": "audio_chunk", "request_id": rid,
                                          "chunk_index": i // step, "data": data}))
                await asyncio.sleep(self.chunk_delay_s)
            await ws.send(json.dumps({"type": "complete", "request_id": rid,
                                      "total_chunks": -(-len(audio) // step), "characters": len(req["text"])}))
        except ConnectionClosed:
            pass  # barge-in: the client hung up mid-sentence
        finally:
            self.active -= 1


if __name__ == "__main__":
    async def main():
        async with MockVox(port=8765, chunk_delay_s=0.04) as vox:
            print(f"Mock Vox on {vox.url} (API key: {vox.api_key})")
            await asyncio.Future()

    asyncio.run(main())
