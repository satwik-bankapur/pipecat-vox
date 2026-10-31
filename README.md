# pipecat-vox

[timepay.ai](https://timepay.ai)'s Vox TTS as a [Pipecat](https://github.com/pipecat-ai/pipecat) service. One file, 180 lines, 5 tests, runs with no API key.

```python
tts = VoxTTSService(
    api_key=os.getenv("VOX_API_KEY"),
    sample_rate=8000,  # phone; 24000 for web
    settings=VoxTTSService.Settings(voice=VoxVoice.NISHA, language=Language.HI),
)
```

## Three things about Vox's API, and what [`tts.py`](pipecat_vox/tts.py) does about them

| Vox's API | What goes wrong if you ignore it | What `tts.py` does |
| --- | --- | --- |
| No cancel message | The caller interrupts, but the bot keeps talking: Vox finishes the sentence and old audio plays over the reply | On barge-in, reconnect (via `InterruptibleTTSService`), which stops synthesis; drop any chunk whose `request_id` isn't the one in flight |
| Plans cap concurrent requests | Sentences fail with `concurrency_limit_exceeded` mid-call | Send one sentence at a time from a queue; retry the cap error with exponential backoff |
| `insufficient_credits` | The bot goes silent and the turn never ends | Report it as a permanent error; the turn still closes |

Audio goes out as raw PCM (`add_wav_header: false`) at 8 kHz for SIP or 24 kHz for WebRTC.

## Run it (no keys)

```bash
uv sync --group dev
uv run pytest                          # 5 tests against a local mock of Vox
uv run examples/collections_demo.py    # a payment-reminder call with a barge-in
```

```text
  rate  spoken   p50 TTFB     audio  bandwidth
   8kHz    5 / 6       2 ms     62 KB    16 KB/s   phone (SIP)
  24kHz    5 / 6       4 ms    186 KB    48 KB/s   web (WebRTC)

Barge-in cut 1 sentence(s) short; Vox has no cancel, so the socket was reset.
Tools called: lookup_account, log_promise_to_pay, send_payment_link, transfer_to_human
```

The demo scripts the caller and the LLM, so it needs no keys. Its four BFSI tools are real handlers with Pipecat `FunctionSchema`s, ready to register with an LLM. Set `VOX_API_KEY` to run it against real Vox, and `REDIS_URL` to log per-sentence TTFB to Redis streams (`vox:ttfb:8000`, `vox:ttfb:24000`).

The mock ([`examples/mock_vox_server.py`](examples/mock_vox_server.py)) follows Vox's [published AsyncAPI spec](https://docs.timepay.ai/api-reference/endpoint/text-to-speech-ws) and plays a tone instead of speech.

## Trade-offs I chose

- **One sentence at a time.** Vox's docs call sequential use safe, and it keeps audio in order for free. The cost is a TTFB-sized gap between sentences. Next step: let sentence N+1 synthesize while N plays, buffering it until N finishes.
- **Reconnect on every barge-in.** It costs one WebSocket handshake, but that happens while the caller is talking, so the caller doesn't wait on it.
- **8, 16 or 24 kHz only.** Those are Vox's rates; anything else raises an error instead of being resampled.
- **Not built yet:** Vox's emotion tags (`<sad>`, `[sigh]`) through Pipecat's expressive conversation, and a run against live Vox. This was built from the public spec without a key.
