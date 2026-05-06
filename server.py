from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import asyncio
import json
from datetime import datetime

app = FastAPI()

COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

# bookings[date][court][slot] = "PlayerName" or None
bookings: dict = {}

# Full log of every booking and cancellation
history: list = []

# asyncio.Lock — prevents double-booking when 2 users book at the same time
booking_lock = asyncio.Lock()

# All currently connected WebSocket clients
connected_clients: list[WebSocket] = []


def init_date(date_str: str):
    """Create empty booking slots for a date if not done yet."""
    if date_str not in bookings:
        bookings[date_str] = {
            court: {slot: None for slot in SLOTS}
            for court in COURTS
        }


async def broadcast_state(date_str: str):
    """Push booking state for a date to ALL connected clients."""
    message = json.dumps({
        "type": "state",
        "date": date_str,
        "bookings": bookings[date_str]
    })
    dead = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except Exception:
            dead.append(client)
    for client in dead:
        connected_clients.remove(client)


async def send_error(ws: WebSocket, msg: str):
    await ws.send_text(json.dumps({"type": "error", "msg": msg}))


# API Routes 

@app.get("/api/history")
async def get_history():
    return JSONResponse(history)


#WebSocket endpoint 

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"[+] Client connected | Total: {len(connected_clients)}")

    today = datetime.now().strftime("%Y-%m-%d")
    init_date(today)
    await websocket.send_text(json.dumps({
        "type": "state",
        "date": today,
        "bookings": bookings[today]
    }))

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg["type"] == "get_date":
                date_str = msg["date"]
                init_date(date_str)
                await websocket.send_text(json.dumps({
                    "type": "state",
                    "date": date_str,
                    "bookings": bookings[date_str]
                }))

            elif msg["type"] == "book":
                date_str = msg["date"]
                court    = msg["court"]
                slot     = msg["slot"]
                name     = msg["name"]
                init_date(date_str)

                async with booking_lock:
                    if bookings[date_str][court][slot] is None:
                        bookings[date_str][court][slot] = name
                        history.append({
                            "action":    "Booked",
                            "name":      name,
                            "court":     court,
                            "slot":      slot,
                            "date":      date_str,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        print(f"[BOOKED] {name} → {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                    else:
                        await send_error(websocket,
                            f"Sorry! {court} at {slot} is already taken by {bookings[date_str][court][slot]}.")

            elif msg["type"] == "cancel":
                date_str = msg["date"]
                court    = msg["court"]
                slot     = msg["slot"]
                name     = msg["name"]

                async with booking_lock:
                    if bookings[date_str][court][slot] == name:
                        bookings[date_str][court][slot] = None
                        history.append({
                            "action":    "Cancelled",
                            "name":      name,
                            "court":     court,
                            "slot":      slot,
                            "date":      date_str,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        print(f"[CANCEL] {name} cancelled {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                    else:
                        await send_error(websocket, "You can only cancel your own bookings!")

    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"[-] Client disconnected | Total: {len(connected_clients)}")


app.mount("/", StaticFiles(directory="static", html=True), name="static")