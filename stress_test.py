import asyncio
import websockets
import json
import time
import random
import statistics
import csv
from datetime import datetime

URL = "wss://pdc-pit.onrender.com/ws"

COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

# Detailed metrics tracked per user
all_metrics = []   # list of dicts with full timing breakdown
success_count = 0
error_count = 0


async def simulate_user(user_id: int):
    """
    Simulate one user with detailed timing breakdown:
      - connection time: how long to establish the WebSocket
      - response time: how long the server took to reply
      - total time: end-to-end
    """
    global success_count, error_count

    name = f"StressUser{user_id}"
    metrics = {
        "user_id": user_id,
        "name": name,
        "connect_ms": None,
        "response_ms": None,
        "total_ms": None,
        "status": "unknown",
        "court": None,
        "slot": None,
    }

    overall_start = time.perf_counter()

    try:
        #Stage 1: Connect
        connect_start = time.perf_counter()
        ws = await websockets.connect(URL, open_timeout=10)
        metrics["connect_ms"] = (time.perf_counter() - connect_start) * 1000

        try:
            # Drain initial messages from server (state, activity_init, stats)
            for _ in range(3):
                try:
                    await asyncio.wait_for(ws.recv(), timeout=2)
                except asyncio.TimeoutError:
                    break  # no more init messages

            #Stage 2: Book a slot
            court = random.choice(COURTS)
            slot  = random.choice(SLOTS)
            metrics["court"] = court
            metrics["slot"]  = slot

            request_start = time.perf_counter()
            await ws.send(json.dumps({
                "type":  "book",
                "court": court,
                "slot":  slot,
                "name":  name,
                "session_id": f"stress_session_{user_id}"
            }))

            # Keep reading until we get a state, error, or activity update for our booking
            response = None
            for _ in range(5):  # try up to 5 messages
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(raw)
                    if data["type"] in ("state", "error"):
                        response = data
                        break
                except asyncio.TimeoutError:
                    break

            metrics["response_ms"] = (time.perf_counter() - request_start) * 1000

            if response and response["type"] == "state":
                metrics["status"] = "success"
                success_count += 1
            elif response and response["type"] == "error":
                metrics["status"] = "conflict"
                error_count += 1
            else:
                metrics["status"] = "no_response"
                error_count += 1

        finally:
            await ws.close()

    except Exception as e:
        metrics["status"] = f"error: {type(e).__name__}"
        error_count += 1

    metrics["total_ms"] = (time.perf_counter() - overall_start) * 1000
    all_metrics.append(metrics)


async def run_stress_test(num_users: int):
    """Launch num_users connections all at the same time."""
    global success_count, error_count
    all_metrics.clear()
    success_count = 0
    error_count = 0

    print(f"\n{'─'*55}")
    print(f"  Starting stress test: {num_users} CONCURRENT users")
    print(f"{'─'*55}")

    wall_start = time.perf_counter()

    # asyncio.gather launches ALL user tasks simultaneously
    tasks = [simulate_user(i) for i in range(1, num_users + 1)]
    await asyncio.gather(*tasks)

    total_ms = (time.perf_counter() - wall_start) * 1000

    #Compute statistics
    response_times = [m["response_ms"] for m in all_metrics if m["response_ms"] is not None]
    connect_times  = [m["connect_ms"]  for m in all_metrics if m["connect_ms"]  is not None]
    total_times    = [m["total_ms"]    for m in all_metrics if m["total_ms"]    is not None]

    summary = {
        "users":          num_users,
        "successful":     success_count,
        "conflicts":      error_count,
        "total_wall_ms":  round(total_ms, 1),
        "throughput_rps": round((num_users / total_ms) * 1000, 2),
        "connect_avg_ms":  round(statistics.mean(connect_times),  1) if connect_times  else 0,
        "response_avg_ms": round(statistics.mean(response_times), 1) if response_times else 0,
        "response_med_ms": round(statistics.median(response_times), 1) if response_times else 0,
        "response_min_ms": round(min(response_times), 1) if response_times else 0,
        "response_max_ms": round(max(response_times), 1) if response_times else 0,
        "total_avg_ms":    round(statistics.mean(total_times), 1) if total_times else 0,
    }

    #Print results
    print(f"  Concurrent users     : {summary['users']}")
    print(f"  Successful bookings  : {summary['successful']}")
    print(f"  Slot conflicts       : {summary['conflicts']}  (expected — lock is working!)")
    print(f"  Total wall time      : {summary['total_wall_ms']:.0f} ms")
    print(f"  Throughput           : {summary['throughput_rps']} req/sec")
    print(f"  Avg connect time     : {summary['connect_avg_ms']:.0f} ms")
    print(f"  Avg response time    : {summary['response_avg_ms']:.0f} ms")
    print(f"  Median response time : {summary['response_med_ms']:.0f} ms")
    print(f"  Min response time    : {summary['response_min_ms']:.0f} ms")
    print(f"  Max response time    : {summary['response_max_ms']:.0f} ms")
    print(f"{'─'*55}")

    return summary, list(all_metrics)


def export_to_csv(all_summaries, all_user_metrics):
    """Export full test results to CSV files for analysis and graphing."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Summary CSV — one row per test run
    summary_file = f"stress_results_summary_{timestamp}.csv"
    with open(summary_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_summaries[0].keys())
        writer.writeheader()
        writer.writerows(all_summaries)
    print(f"\n  ✓ Summary saved to: {summary_file}")

    # Detailed CSV — one row per simulated user
    detail_file = f"stress_results_detail_{timestamp}.csv"
    with open(detail_file, 'w', newline='') as f:
        if all_user_metrics:
            writer = csv.DictWriter(f, fieldnames=all_user_metrics[0].keys())
            writer.writeheader()
            writer.writerows(all_user_metrics)
    print(f"  ✓ Per-user details saved to: {detail_file}")
    print(f"\n  Open these CSVs in Excel or Google Sheets to make graphs!")


async def main():
    print("\n" + "="*55)
    print("  PICKLEBALL RESERVATION — STRESS TEST SUITE")
    print("  Make sure server is running: uvicorn server:app")
    print("="*55)

    all_summaries = []
    all_user_metrics = []

    # Test at three load levels matching the brochure (5, 10, 20 users)
    for user_count in [5, 10, 20]:
        summary, user_metrics = await run_stress_test(user_count)
        all_summaries.append(summary)
        all_user_metrics.extend(user_metrics)
        await asyncio.sleep(1)  # brief pause between test runs

    #Print summary table for the brochure
    print("\n\n  SUMMARY TABLE (copy this into your brochure)")
    print("  " + "─"*52)
    print(f"  {'Users':<10} {'Avg (ms)':<12} {'Max (ms)':<12} {'Success':<10} {'RPS':<8}")
    print("  " + "─"*52)
    for r in all_summaries:
        print(f"  {r['users']:<10} {r['response_avg_ms']:<12} {r['response_max_ms']:<12} {r['successful']:<10} {r['throughput_rps']:<8}")
    print("  " + "─"*52)

    #Export to CSV
    if all_summaries:
        export_to_csv(all_summaries, all_user_metrics)

    print("\n  Tip: screenshot this output for our brochure!\n")


if __name__ == "__main__":
    asyncio.run(main())
