"""Real Pipecat frames through VoxTTSService against the local mock Vox server."""

import sys
from pathlib import Path

import pytest
from pipecat.frames.frames import ErrorFrame, InterruptionFrame, TTSAudioRawFrame, TTSSpeakFrame, TTSStoppedFrame
from pipecat.tests.utils import SleepFrame, run_test

from pipecat_vox import VoxTTSService, VoxVoice

sys.path.insert(0, str(Path(__file__).parents[1] / "examples"))
from mock_vox_server import SECONDS_PER_CHAR, MockVox, tone  # noqa: E402


def make_tts(vox, rate=16000):
    tts = VoxTTSService(api_key=vox.api_key, url=vox.url, sample_rate=rate)
    tts.BACKOFF_S = 0.01
    return tts


def audio(frames) -> bytes:
    return b"".join(f.audio for f in frames if isinstance(f, TTSAudioRawFrame))


def spoken(text, rate=16000) -> bytes:  # what the mock "says" for this text (Nisha's tone)
    return tone(262, rate, len(text) * SECONDS_PER_CHAR)


def errors(up) -> list[str]:
    return [f.error for f in up if isinstance(f, ErrorFrame)]


async def test_happy_path_streams_raw_pcm():
    async with MockVox() as vox:
        down, up = await run_test(make_tts(vox), frames_to_send=[TTSSpeakFrame("Namaste Rohan ji."), SleepFrame(0.3)])
    sent = vox.requests[0]
    assert sent["voice_id"] == VoxVoice.NISHA and sent["add_wav_header"] is False
    assert audio(down) == spoken("Namaste Rohan ji.")
    assert any(isinstance(f, TTSStoppedFrame) for f in down) and not errors(up)


async def test_barge_in_drops_old_audio_and_reconnects():
    async with MockVox(chunk_delay_s=0.04) as vox:  # paced like real speech
        down, up = await run_test(make_tts(vox), frames_to_send=[
            TTSSpeakFrame("This reminder is long enough to be interrupted halfway through."),
            SleepFrame(0.3), InterruptionFrame(), SleepFrame(0.1),
            TTSSpeakFrame("Sure, go ahead."), SleepFrame(0.6),
        ])
    after = down[next(i for i, f in enumerate(down) if isinstance(f, InterruptionFrame)):]
    assert vox.connections == 2, "reconnecting is how Vox is told to stop"
    assert audio(after) == spoken("Sure, go ahead."), "no stale audio after barge-in"
    assert not errors(up)


async def test_concurrency_limit_backs_off_and_retries():
    async with MockVox(busy_first=2) as vox:  # another worker holds the plan's slots, twice
        down, up = await run_test(make_tts(vox), frames_to_send=[TTSSpeakFrame("Your EMI is due."), SleepFrame(0.4)])
    assert vox.errors == ["concurrency_limit_exceeded"] * 2
    assert audio(down) == spoken("Your EMI is due.") and not errors(up)
    assert vox.max_active == 1, "one sentence at a time"


async def test_insufficient_credits_is_permanent_and_the_turn_still_ends():
    async with MockVox(no_credits=True) as vox:
        down, up = await run_test(make_tts(vox), frames_to_send=[TTSSpeakFrame("Hello."), SleepFrame(0.3)])
    assert any("insufficient_credits" in e for e in errors(up))
    assert any(isinstance(f, TTSStoppedFrame) for f in down), "no hung turn"


@pytest.mark.parametrize("rate", [8000, 24000])
async def test_8khz_for_phone_24khz_for_web(rate):
    async with MockVox() as vox:
        frames = [TTSSpeakFrame("Payment link sent."), SleepFrame(0.3)]
        down, _ = await run_test(make_tts(vox, rate), frames_to_send=frames)
    assert vox.requests[0]["sample_rate"] == rate
    assert all(f.sample_rate == rate for f in down if isinstance(f, TTSAudioRawFrame))
    assert audio(down) == spoken("Payment link sent.", rate)  # 16 KB/s at 8 kHz, 48 KB/s at 24 kHz
