import asyncio
import websockets
import json
import time
import random
import statistics

URL = "ws://localhost:8000/ws"

COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

# Shared results (safe because asyncio is single-threaded)
response_times_ms = []
success_count = 0
error_count = 0

async def simulate_user(user_id: int):
    """Simulate one user: connect, receive state, try to book a random slot."""
    global success_count, error_count

    name = f"StressUser{user_id}"
    start = time.perf_counter()

    try:
        async with websockets.connect(URL, open_timeout=10) as ws:

            # Step 1: Receive the initial court state
            await ws.recv()

            # Step 2: Try to book a random court/slot
            court = random.choice(COURTS)
            slot  = random.choice(SLOTS)
            await ws.send(json.dumps({
                "type":  "book",
                "court": court,
                "slot":  slot,
                "name":  name
            }))

            # Step 3: Wait for server response
            response = await asyncio.wait_for(ws.recv(), timeout=5)
            elapsed_ms = (time.perf_counter() - start) * 1000
            response_times_ms.append(elapsed_ms)

            data = json.loads(response)
            if data["type"] == "state":
                success_count += 1
            else:
                error_count += 1  # slot was already taken (expected behaviour!)

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        response_times_ms.append(elapsed_ms)
        error_count += 1
        print(f"  [!] User {user_id} error: {e}")


async def run_stress_test(num_users: int):
    """Launch num_users connections all at the same time."""
    global success_count, error_count
    response_times_ms.clear()
    success_count = 0
    error_count = 0

    print(f"\n{'─'*50}")
    print(f"  Starting stress test: {num_users} CONCURRENT users")
    print(f"{'─'*50}")

    wall_start = time.perf_counter()

    # asyncio.gather launches ALL user tasks simultaneously — this is the
    # parallel/distributed concept: true concurrency at the client side
    tasks = [simulate_user(i) for i in range(1, num_users + 1)]
    await asyncio.gather(*tasks)

    total_ms = (time.perf_counter() - wall_start) * 1000

    #Print results
    if response_times_ms:
        avg   = statistics.mean(response_times_ms)
        med   = statistics.median(response_times_ms)
        maxt  = max(response_times_ms)
        mint  = min(response_times_ms)
        thrpt = (num_users / total_ms) * 1000  # requests per second

        print(f"  Concurrent users    : {num_users}")
        print(f"  Successful bookings : {success_count}")
        print(f"  Slot conflicts      : {error_count}  (expected — lock is working!)")
        print(f"  Total wall time     : {total_ms:.0f} ms")
        print(f"  Throughput          : {thrpt:.1f} req/sec")
        print(f"  Response time avg   : {avg:.0f} ms")
        print(f"  Response time median: {med:.0f} ms")
        print(f"  Response time min   : {mint:.0f} ms")
        print(f"  Response time max   : {maxt:.0f} ms")
    else:
        print("  No results — is the server running?")

    print(f"{'─'*50}")
    return {
        "users": num_users,
        "success": success_count,
        "errors": error_count,
        "avg_ms": round(statistics.mean(response_times_ms), 1) if response_times_ms else None,
        "max_ms": round(max(response_times_ms), 1) if response_times_ms else None,
        "total_ms": round(total_ms, 1),
    }


async def main():
    print("\n" + "="*50)
    print("  PICKLEBALL RESERVATION — STRESS TEST SUITE")
    print("  Make sure server is running: uvicorn server:app")
    print("="*50)

    all_results = []

    # Test at increasing load levels
    for user_count in [5, 10, 20]:
        result = await run_stress_test(user_count)
        all_results.append(result)
        await asyncio.sleep(1)  # brief pause between test runs

    # Summary table for the brochure
    print("\n\n  SUMMARY TABLE (copy this into your brochure)")
    print("  " + "─"*46)
    print(f"  {'Users':<10} {'Avg (ms)':<12} {'Max (ms)':<12} {'Success':<10}")
    print("  " + "─"*46)
    for r in all_results:
        print(f"  {r['users']:<10} {r['avg_ms']:<12} {r['max_ms']:<12} {r['success']:<10}")
    print("  " + "─"*46)
    print("\n  Tip: screenshot this output for your brochure!")


if __name__ == "__main__":
    asyncio.run(main())