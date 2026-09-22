"""Optional codec/framing sanity measurement. No timing-based pass/fail thresholds."""

import argparse
import json
from time import perf_counter

from protocol import (
    FrameDecoder,
    WorkerGoodbye,
    decode_message,
    encode_message,
    frame_payload,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=10000)
    parser.add_argument("--chunk-size", type=int, default=8192)
    args = parser.parse_args()
    if args.messages < 1 or args.chunk_size < 1:
        parser.error("counts must be positive")
    message = WorkerGoodbye(
        "benchmark-worker", "measurement", message_id="fixture-message"
    )
    start = perf_counter()
    payloads = [encode_message(message) for _ in range(args.messages)]
    encode_seconds = perf_counter() - start
    stream = b"".join(frame_payload(payload) for payload in payloads)
    decoder = FrameDecoder()
    count = 0
    maximum_buffered = 0
    start = perf_counter()
    for offset in range(0, len(stream), args.chunk_size):
        for payload in decoder.feed(stream[offset : offset + args.chunk_size]):
            assert decode_message(payload) == message
            count += 1
        maximum_buffered = max(maximum_buffered, decoder.buffered_bytes)
    decoder.finish()
    decode_seconds = perf_counter() - start
    assert count == args.messages
    print(
        json.dumps(
            {
                "messages": count,
                "stream_bytes": len(stream),
                "chunk_size": args.chunk_size,
                "maximum_buffered_bytes": maximum_buffered,
                "encode_seconds": encode_seconds,
                "framing_and_decode_seconds": decode_seconds,
            },
            sort_keys=True,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
