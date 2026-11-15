"""Deterministic log streams for the Loki fixture, as /loki/api/v1/push bodies.

    python3 test/fixture/gen_push.py > push.json     # one JSON array of push bodies

All timestamps are fixed (2026-01-01 00:00-02:00 UTC), so recordings made from
any run are identical. The streams cover what the SQL tests need:

  checkout-0  120 logfmt info lines, structured metadata trace_id (+ user_id)
  checkout-1  20 error lines (level stream label = error)
  payments-0  30 JSON lines (level only inside the JSON) and one multi-line entry
  bulk        6,200 lines in one stream: more than the 5,000-line server limit
  ties        9 entries sharing two timestamps: exercises page-boundary dedup
  weird       label and line values with quotes, backslashes, tabs and Unicode
"""

import json

T0 = 1767225600  # 2026-01-01T00:00:00Z
NS = 1_000_000_000


def ns(sec, frac_ns=0):
    return str(sec * NS + frac_ns)


def stream(labels, values):
    return {"stream": labels, "values": values}


def main():
    bodies = []
    shop = {"namespace": "shop"}

    vals = []
    for i in range(120):
        md = {"trace_id": f"trace-{i:04d}"}
        if i % 2 == 0:
            md["user_id"] = f"u{i % 7}"
        line = f"method=GET path=/cart/{i % 5} status=200 duration_ms={(i * 37) % 500}"
        vals.append([ns(T0 + 30 * i), line, md])
    bodies.append([stream({**shop, "app": "checkout", "pod": "checkout-0", "job": "shop/checkout", "level": "info"}, vals)])

    vals = []
    for i in range(20):
        err = "payment declined" if i % 3 else "upstream timeout"
        vals.append([ns(T0 + 15 + 180 * i, 250_000), f'method=POST path=/checkout status=500 error="{err}"', {"trace_id": f"err-{i:03d}"}])
    bodies.append([stream({**shop, "app": "checkout", "pod": "checkout-1", "job": "shop/checkout", "level": "error"}, vals)])

    vals = []
    for i in range(30):
        lvl = "warn" if i % 4 == 0 else "info"
        doc = {"level": lvl, "msg": "charge" if i % 2 else "refund", "amount": (i * 13) % 97, "currency": "EUR"}
        vals.append([ns(T0 + 60 + 120 * i, 123), json.dumps(doc, separators=(",", ":"))])
    vals.append([ns(T0 + 601), "panic: card network unreachable\n\tat charge.go:42\n\tat main.go:7"])
    bodies.append([stream({**shop, "app": "payments", "pod": "payments-0", "job": "shop/payments"}, vals)])

    # 6,200 lines in one stream, 01:00:00 to 01:51:39.5, plus a few failures.
    vals = []
    for i in range(6200):
        status = "failed" if i % 1000 == 999 else "ok"
        vals.append([ns(T0 + 3600 + i // 2, (i % 2) * 500_000_000), f"batch row {i:05d} status={status}"])
    for k in range(0, 6200, 500):
        bodies.append([stream({"namespace": "batch", "app": "bulk", "job": "batch/bulk"}, vals[k:k + 500])])

    vals = [[ns(T0 + 100), f"tie-{j}"] for j in range(6)] + [[ns(T0 + 101), f"tie-late-{j}"] for j in range(3)]
    bodies.append([stream({"namespace": "batch", "app": "ties", "job": "batch/ties"}, vals)])

    vals = [
        [ns(T0 + 200), 'say "hello" to C:\\temp\\new'],
        [ns(T0 + 201), "tab\there and caf\u00e9 \u2603"],
        [ns(T0 + 202), "100% done_ok (literal LIKE wildcards)"],
    ]
    bodies.append([stream({"namespace": "misc", "app": "weird", "job": "misc/weird", "path": 'C:\\temp "x"'}, vals)])

    print(json.dumps([{"streams": b} for b in bodies]))


if __name__ == "__main__":
    main()
