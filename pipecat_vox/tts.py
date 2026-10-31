"""timepay.ai Vox text-to-speech for Pipecat. Three Vox facts shape this file:
1. Every audio_chunk names its request_id: route audio by ID, drop stale chunks.
2. No cancel message: on barge-in, InterruptibleTTSService reconnects, which stops
   the server synthesizing a sentence nobody will hear.
3. Plans cap concurrent requests: one sentence at a time from a queue, and
   concurrency_limit_exceeded retried with exponential backoff.
"""

import asyncio
import base64
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum

from pipecat.frames.frames import StartFrame, TTSAudioRawFrame, TTSStoppedFrame
from pipecat.services.settings import TTSSettings
from pipecat.services.tts_service import InterruptibleTTSService
from pipecat.transcriptions.language import Language
from pipecat.utils.types import NOT_GIVEN, NotGiven
from websockets.protocol import State

VOX_URL = "wss://api.tts.timepay.ai/api/v1/get_speech"
VOX_SAMPLE_RATES = (8000, 16000, 24000)  # 8 kHz for phone calls, 24 kHz for web
VOX_LANGUAGES = {"en", "hi", "mr", "ta", "te", "gu", "kn", "ml", "bn", "pa", "od", "as"}


class VoxVoice(StrEnum):  # the voices in Vox's API reference
    KARTIK = "Ogbs15oBevLzXsUuTtA1"
    RAHUL = "Owbs15oBevLzXsUurdA_"
    NISHA = "PAbs15oBevLzXsUu4dCi"
    TULSI = "PQbt15oBevLzXsUuNtD3"
    SEEMA = "Pgbt15oBevLzXsUubdA6"


@dataclass
class VoxTTSSettings(TTSSettings):  # voice, language, speed (0.5-2.0); sent with each sentence
    speed: float | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)


@dataclass
class _Job:  # one queue item: a sentence to speak, or (text=None) the end of a turn
    context_id: str
    text: str | None
    request_id: str = ""
    sent_at: float = 0.0
    first_audio_at: float = 0.0
    error: str | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


class VoxTTSService(InterruptibleTTSService):  # streams Vox speech, one sentence at a time
    Settings = VoxTTSSettings
    MAX_RETRIES, BACKOFF_S, REQUEST_TIMEOUT_S = 3, 0.25, 15.0  # tune per instance if needed

    def __init__(self, *, api_key: str, url: str = VOX_URL, settings: VoxTTSSettings | None = None, **kwargs):
        defaults = self.Settings(model="vox-v1", voice=VoxVoice.NISHA, language="en", speed=1.0)
        defaults.apply_update(settings or self.Settings())
        super().__init__(push_start_frame=True, push_text_frames=True, settings=defaults, **kwargs)
        self._api_key, self._url = api_key, url
        self._queue: asyncio.Queue[_Job] = asyncio.Queue()
        self._inflight = self._sender_task = self._receive_task = None
        self._register_event_handler("on_request_completed")

    def language_to_service_language(self, language: Language) -> str | None:
        code = str(language.value).split("-")[0]  # hi-IN -> hi
        code = "od" if code == "or" else code  # Vox writes Odia as "od", not ISO "or"
        return code if code in VOX_LANGUAGES else None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        if self.sample_rate not in VOX_SAMPLE_RATES:
            raise ValueError(f"Vox synthesizes at {VOX_SAMPLE_RATES} Hz, not {self.sample_rate}")
        await self._connect()

    async def run_tts(self, text: str, context_id: str):
        await self._queue.put(_Job(context_id, text))
        yield None

    async def flush_audio(self, context_id: str | None = None):  # the turn's text is all in
        if context_id := context_id or self.get_active_audio_context_id():
            await self._queue.put(_Job(context_id, None))

    async def _sender(self):
        while True:
            job = await self._queue.get()
            if not self.audio_context_available(job.context_id):
                continue  # interrupted before we got to it
            if job.text is None:
                await self.append_to_audio_context(job.context_id, TTSStoppedFrame(context_id=job.context_id))
                await self.remove_audio_context(job.context_id)
            else:
                await self._speak(job)

    async def _speak(self, job: _Job):
        for attempt in range(self.MAX_RETRIES + 1):
            job.request_id, job.error, job.first_audio_at = f"vox-{uuid.uuid4().hex[:12]}", None, 0.0
            job.done.clear()
            job.sent_at = time.monotonic()
            if not (self._websocket and self._websocket.state is State.OPEN):
                job.error = "not connected"
                break
            self._inflight = job
            s = self._settings
            await self._websocket.send(json.dumps({
                "type": "speech", "request_id": job.request_id, "text": job.text,
                "voice_id": str(s.voice), "language": str(s.language or "en"),
                "speed": float(s.speed or 1.0), "sample_rate": self.sample_rate,
                "add_wav_header": False,  # raw PCM; a header mid-stream plays as a click
            }))
            try:
                await asyncio.wait_for(job.done.wait(), self.REQUEST_TIMEOUT_S)
            except TimeoutError:
                job.error = f"no response in {self.REQUEST_TIMEOUT_S:.0f}s"
            if job.error != "concurrency_limit_exceeded":
                break
            await asyncio.sleep(self.BACKOFF_S * 2**attempt)  # the plan's slots are busy
        self._inflight = None
        if job.error:
            permanent = job.error == "insufficient_credits"  # nothing more will work
            await self.push_error(f"Vox {job.error} on [{job.text}]", force_treat_as_permanent=permanent)
        ttfb = (job.first_audio_at - job.sent_at) * 1000 if job.first_audio_at else None
        await self._call_event_handler("on_request_completed", {
            "request_id": job.request_id, "ttfb_ms": ttfb, "attempts": attempt + 1,
            "sample_rate": self.sample_rate, "error": job.error,
        })

    async def _receive_messages(self):
        async for raw in self._websocket:
            msg, job = json.loads(raw), self._inflight
            # Stale chunks (a request we gave up on) are dropped. An error may omit request_id;
            if not job or msg.get("request_id", job.request_id) != job.request_id:  # then it's ours
                continue
            if msg["type"] == "audio_chunk":
                job.first_audio_at = job.first_audio_at or time.monotonic()
                audio = base64.b64decode(msg["data"])
                frame = TTSAudioRawFrame(audio, self.sample_rate, 1, context_id=job.context_id)
                await self.append_to_audio_context(job.context_id, frame)
            elif msg["type"] in ("complete", "error"):
                job.error = msg.get("error")
                job.done.set()

    async def _connect(self):
        await super()._connect()
        await self._connect_websocket()
        if self._websocket and not self._receive_task:
            self._receive_task = self.create_task(self._receive_task_handler(self._report_error))
        if not self._sender_task:
            self._sender_task = self.create_task(self._sender())

    async def _disconnect(self):
        await super()._disconnect()  # also closes this connection's audio contexts
        for task in filter(None, (self._receive_task, self._sender_task)):
            await self.cancel_task(task)
        self._receive_task = self._sender_task = None
        self._queue = asyncio.Queue()  # barge-in: nothing queued is wanted any more
        await self._disconnect_websocket()

    async def _connect_websocket(self):
        try:
            if self._websocket and self._websocket.state is State.OPEN:
                return
            auth = {"Authorization": f"Bearer {self._api_key}"}
            ws = await self._websocket_connect(self._url, additional_headers=auth)
            hello = json.loads(await asyncio.wait_for(ws.recv(), 10))
            if hello.get("type") != "connected":
                raise ConnectionError(hello.get("message", hello))
            self._websocket = ws
        except Exception as e:
            self._websocket = None
            await self.push_error(error_msg=f"{self}: can't connect to Vox: {e}", exception=e)

    async def _disconnect_websocket(self):
        if job := self._inflight:  # a dropped socket never completes the request on it
            job.error = "connection lost"
            job.done.set()
        if self._websocket:
            await self._websocket.close()
        self._websocket = None
