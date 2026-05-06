from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import asyncio
import json
from datetime import datetime
from collections import Counter

app = FastAPI()


async def stats_heartbeat_worker():
    """Background worker — broadcasts live stats to all clients every 10 seconds.
    Runs in parallel with the main connection handler via asyncio."""
    while True:
        await asyncio.sleep(10)
        if connected_clients:
            await broadcast_stats()


@app.on_event("startup")
async def start_background_workers():
    """Launch background worker(s) when the server starts."""
    asyncio.create_task(stats_heartbeat_worker())
    print("[WORKER] Stats heartbeat worker started (broadcasts every 10s)")

COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

bookings: dict = {}
history: list = []
recent_activity: list = []
booking_lock = asyncio.Lock()
connected_clients: list[WebSocket] = []


def init_date(date_str: str):
    if date_str not in bookings:
        bookings[date_str] = {court: {slot: None for slot in SLOTS} for court in COURTS}


def calc_stats():
    total_bookings = sum(1 for h in history if h["action"] == "Booked")
    total_cancels  = sum(1 for h in history if h["action"] == "Cancelled")
    active = total_bookings - total_cancels

    court_counter = Counter(h["court"] for h in history if h["action"] == "Booked")
    popular = court_counter.most_common(1)[0][0] if court_counter else "—"

    users = len({h["name"] for h in history})

    return {
        "online":     len(connected_clients),
        "active":     active,
        "total":      total_bookings,
        "cancelled":  total_cancels,
        "popular":    popular,
        "users":      users,
    }


async def broadcast(message: dict):
    payload = json.dumps(message)
    dead = []
    for client in connected_clients:
        try:
            await client.send_text(payload)
        except Exception:
            dead.append(client)
    for client in dead:
        if client in connected_clients:
            connected_clients.remove(client)


async def broadcast_state(date_str: str):
    await broadcast({"type": "state", "date": date_str, "bookings": bookings[date_str]})


async def broadcast_stats():
    await broadcast({"type": "stats", "stats": calc_stats()})


async def add_activity(action: str, name: str, court: str, slot: str):
    event = {
        "action":    action,
        "name":      name,
        "court":     court,
        "slot":      slot,
        "timestamp": datetime.now().strftime("%I:%M:%S %p"),
    }
    recent_activity.insert(0, event)
    if len(recent_activity) > 8:
        recent_activity.pop()
    await broadcast({"type": "activity", "event": event})


@app.get("/api/history")
async def get_history():
    return JSONResponse(history)


@app.get("/api/stats")
async def get_stats():
    return JSONResponse(calc_stats())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"[+] Client connected | Total: {len(connected_clients)}")

    today = datetime.now().strftime("%Y-%m-%d")
    init_date(today)

    await websocket.send_text(json.dumps({
        "type": "state", "date": today, "bookings": bookings[today]
    }))
    await websocket.send_text(json.dumps({
        "type": "activity_init", "events": recent_activity
    }))

    await broadcast_stats()

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg["type"] == "get_date":
                date_str = msg["date"]
                init_date(date_str)
                await websocket.send_text(json.dumps({
                    "type": "state", "date": date_str, "bookings": bookings[date_str]
                }))

            elif msg["type"] == "book":
                date_str, court, slot, name = msg["date"], msg["court"], msg["slot"], msg["name"]
                init_date(date_str)

                async with booking_lock:
                    if bookings[date_str][court][slot] is None:
                        bookings[date_str][court][slot] = name
                        history.append({
                            "action": "Booked", "name": name, "court": court,
                            "slot": slot, "date": date_str,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        print(f"[BOOKED] {name} → {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                        await add_activity("booked", name, court, slot)
                        await broadcast_stats()
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "msg": f"Sorry! {court} at {slot} is already taken by {bookings[date_str][court][slot]}."
                        }))

            elif msg["type"] == "cancel":
                date_str, court, slot, name = msg["date"], msg["court"], msg["slot"], msg["name"]

                async with booking_lock:
                    if bookings[date_str][court][slot] == name:
                        bookings[date_str][court][slot] = None
                        history.append({
                            "action": "Cancelled", "name": name, "court": court,
                            "slot": slot, "date": date_str,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        print(f"[CANCEL] {name} cancelled {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                        await add_activity("cancelled", name, court, slot)
                        await broadcast_stats()
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "msg": "You can only cancel your own bookings!"
                        }))

    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
        print(f"[-] Client disconnected | Total: {len(connected_clients)}")
        await broadcast_stats()


app.mount("/", StaticFiles(directory="static", html=True), name="static")

