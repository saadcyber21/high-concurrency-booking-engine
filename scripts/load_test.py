import argparse
import concurrent.futures
import time

import httpx


def main():
    parser = argparse.ArgumentParser(description="Have concurrent clients compete for one seat")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--seat", type=int, default=1)
    parser.add_argument("--users", type=int, default=100)
    args = parser.parse_args()

    def compete(index: int) -> int:
        try:
            response = httpx.post(f"{args.base_url}/v1/events/{args.event_id}/seats/{args.seat}/hold", headers={"Idempotency-Key": f"load-test-{index}"}, timeout=10)
            return response.status_code
        except httpx.HTTPError:
            return 0

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.users) as pool:
        statuses = list(pool.map(compete, range(args.users)))
    elapsed = time.perf_counter() - start
    print({"users": args.users, "elapsed_seconds": round(elapsed, 3), "status_counts": {str(s): statuses.count(s) for s in sorted(set(statuses))}})
    if statuses.count(200) + statuses.count(201) != 1:
        raise SystemExit("Expected exactly one successful hold for a single seat")


if __name__ == "__main__":
    main()
