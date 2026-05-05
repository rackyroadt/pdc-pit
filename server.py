
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import asyncio
import json

app = FastAPI()

# Shared in-memory data 
COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

# bookings[court][slot] = None (available) or "PlayerName" (taken)
bookings: dict = {
    court: {slot: None for slot in SLOTS}
    for court in COURTS
}

# asyncio.Lock — critical for preventing double-booking under concurrency
# When 2 users try to book the same slot at the same millisecond,
# the lock makes them take turns, so only the first one succeeds.
booking_lock = asyncio.Lock()

# All currently connected WebSocket clients
connected_clients: list[WebSocket] = []

#Helpers

async def broadcast_state():
    """Push the full booking state to every connected client."""
    message = json.dumps({"type": "state", "bookings": bookings})
    dead_clients = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except Exception:
            dead_clients.append(client)  # client disconnected
    for client in dead_clients:
        connected_clients.remove(client)

async def send_error(ws: WebSocket, msg: str):
    """Send an error message to a single client."""
    await ws.send_text(json.dumps({"type": "error", "msg": msg}))

#WebSocket endpoint

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Each user connects here. The server handles all users CONCURRENTLY
    using asyncio — no user has to wait for another to finish.
    """
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"[+] Client connected  | Total: {len(connected_clients)}")

    # Send current state immediately so the new user sees the grid
    await websocket.send_text(json.dumps({"type": "state", "bookings": bookings}))

    try:
        while True:
            # Wait for a message from this client (non-blocking for other clients)
            raw = await websocket.receive_text()
            message = json.loads(raw)

            if message["type"] == "book":
                court = message["court"]
                slot  = message["slot"]
                name  = message["name"]

                # CRITICAL SECTION — only one booking can happen at a time
                async with booking_lock:
                    if bookings[court][slot] is None:
                        bookings[court][slot] = name
                        print(f"[BOOKED] {name} → {court} @ {slot}")
                        await broadcast_state()          # tell everyone!
                    else:
                        await send_error(websocket,
                            f"Sorry! {court} at {slot} is already taken by {bookings[court][slot]}.")

            elif message["type"] == "cancel":
                court = message["court"]
                slot  = message["slot"]
                name  = message["name"]

                async with booking_lock:
                    if bookings[court][slot] == name:
                        bookings[court][slot] = None
                        print(f"[CANCEL] {name} cancelled {court} @ {slot}")
                        await broadcast_state()
                    else:
                        await send_error(websocket, "You can only cancel your own bookings!")

    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"[-] Client disconnected | Total: {len(connected_clients)}")

#Serve the frontend 
app.mount("/", StaticFiles(directory="static", html=True), name="static")