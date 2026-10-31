"""A payment-reminder call on Vox: 4 BFSI tools, a barge-in, 8 kHz vs 24 kHz, TTFB logged to Redis.

The caller and the LLM are scripted, so it runs with no keys against the mock server:

    uv run examples/collections_demo.py
    VOX_API_KEY=... uv run examples/collections_demo.py            # real Vox, real TTFB
    REDIS_URL=redis://localhost:6379 uv run examples/collections_demo.py  # XADD vox:ttfb:<rate>
"""

import asyncio
import os
import statistics
import sys
from pathlib import Path

from pipecat.adapters.llm.schemas.function_schema import FunctionSchema
from pipecat.adapters.llm.schemas.tools_schema import ToolsSchema
from pipecat.frames.frames import InterruptionFrame, TTSAudioRawFrame, TTSSpeakFrame
from pipecat.tests.utils import SleepFrame, run_test
from pipecat.transcriptions.language import Language

from pipecat_vox import VoxTTSService, VoxVoice
from pipecat_vox.tts import VOX_URL

sys.path.insert(0, str(Path(__file__).parent))
from mock_vox_server import SECONDS_PER_CHAR, MockVox  # noqa: E402

# --- The 4 tools a collections agent needs. Wire them to any Pipecat LLM with
#     llm.register_function(name, handler) and LLMContext(messages, tools=TOOLS).
LEDGER = {"LN-4471": {"name": "Rohan", "emi": "four thousand two hundred", "due": "the fifth"}}
CALL_LOG: list[str] = []


async def lookup_account(account_id: str) -> dict:
    CALL_LOG.append("lookup_account")
    return LEDGER[account_id]

async def log_promise_to_pay(account_id: str, date: str) -> dict:
    CALL_LOG.append("log_promise_to_pay")
    return {"status": "logged", "date": date}

async def send_payment_link(account_id: str, channel: str = "sms") -> dict:
    CALL_LOG.append("send_payment_link")
    return {"sent_via": channel}

async def transfer_to_human(account_id: str, reason: str) -> dict:
    CALL_LOG.append("transfer_to_human")
    return {"queue": "collections-l2"}


def _tool(name: str, description: str, **props: str) -> FunctionSchema:
    properties = {p: {"type": "string", "description": d} for p, d in props.items()}
    return FunctionSchema(name=name, description=description, properties=properties, required=list(props))


TOOLS = ToolsSchema(standard_tools=[
    _tool("lookup_account", "Get the borrower's name, EMI and due date", account_id="Loan account ID"),
    _tool("log_promise_to_pay", "Record the date the borrower promises to pay", account_id="Loan ID", date="Day"),
    _tool("send_payment_link", "Send a UPI payment link", account_id="Loan account ID", channel="sms or whatsapp"),
    _tool("transfer_to_human", "Hand a dispute to a person", account_id="Loan account ID", reason="Why"),
])


# --- One scripted call. Each line is what the LLM would say after the tool result.
async def script(pace: float) -> list:  # pace: seconds of speech per character
    acct = await lookup_account("LN-4471")
    promise = await log_promise_to_pay("LN-4471", "Friday")
    link = await send_payment_link("LN-4471")
    await transfer_to_human("LN-4471", "disputes the late fee")
    say = lambda text, pause=None: [TTSSpeakFrame(text), SleepFrame(pause or len(text) * pace + 0.2)]  # noqa: E731
    return [
        *say(f"Namaste {acct['name']} ji, Demo Finance se Priya bol rahi hoon."),
        *say(f"Aapka EMI {acct['emi']} rupees ka hai, jo {acct['due']} ko due tha."),
        # The caller talks over a long sentence: barge-in.
        *say("Main aapko payment ke saare options detail mein batati hoon, UPI, net banking, card, aur", 0.4),
        InterruptionFrame(), SleepFrame(0.1),
        *say(f"Theek hai, {promise['date']} ka promise maine note kar liya."),
        *say(f"Maine aapko {link['sent_via'].upper()} par payment link bhej diya hai."),
        *say("Late fee ke baare mein main aapko senior colleague se connect karti hoon."),
    ]


class LatencyLog:
    """Keeps per-sentence TTFB; also XADDs it to a Redis stream when REDIS_URL is set."""

    def __init__(self):
        self.rows, self.redis = [], None
        if url := os.getenv("REDIS_URL"):
            import redis.asyncio as redis

            self.redis = redis.from_url(url)

    async def record(self, _tts, stats: dict):  # the on_request_completed handler
        self.rows.append(stats)
        if self.redis and stats["ttfb_ms"] is not None:
            fields = {"ttfb_ms": round(stats["ttfb_ms"], 1), "request_id": stats["request_id"]}
            await self.redis.xadd(f"vox:ttfb:{stats['sample_rate']}", fields)


async def run_call(url: str, api_key: str, rate: int, pace: float) -> tuple[list[dict], bytes]:
    tts = VoxTTSService(api_key=api_key, url=url, sample_rate=rate,
                        settings=VoxTTSService.Settings(voice=VoxVoice.NISHA, language=Language.HI))
    log = LatencyLog()
    tts.add_event_handler("on_request_completed", log.record)
    down, _ = await run_test(tts, frames_to_send=await script(pace))
    return log.rows, b"".join(f.audio for f in down if isinstance(f, TTSAudioRawFrame))


async def main():
    live_key = os.getenv("VOX_API_KEY")
    print(f"{'rate':>6}  {'spoken':>6}  {'p50 TTFB':>9}  {'audio':>8}  {'bandwidth':>9}")
    for rate, use in ((8000, "phone (SIP)"), (24000, "web (WebRTC)")):
        if live_key:
            rows, audio = await run_call(VOX_URL, live_key, rate, pace=0.07)  # ~14 chars/s spoken
        else:
            async with MockVox(chunk_delay_s=0.04) as vox:
                rows, audio = await run_call(vox.url, vox.api_key, rate, pace=SECONDS_PER_CHAR)
        ttfbs = [r["ttfb_ms"] for r in rows if r["ttfb_ms"] is not None]
        p50 = f"{statistics.median(ttfbs):.0f} ms" if ttfbs else "-"
        print(f"{rate // 1000:>4}kHz  {len(rows):>3} / 6  {p50:>9}  {len(audio) // 1024:>5} KB  "
              f"{rate * 2 // 1000:>4} KB/s   {use}")
    print(f"\nBarge-in cut {6 - len(rows)} sentence(s) short; Vox has no cancel, so the socket was reset.")
    print(f"Tools called: {', '.join(dict.fromkeys(CALL_LOG))}")
    if not live_key:
        print("Mock TTFB is near zero; set VOX_API_KEY for real numbers.")


if __name__ == "__main__":
    asyncio.run(main())
