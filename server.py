from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter

# Philippines timezone (UTC+8)
PH_TZ = timezone(timedelta(hours=8))

def now_ph():
    """Returns current time in Philippines timezone (UTC+8)."""
    return datetime.now(PH_TZ)

#LOGGING SETUP 
# Detailed logging configuration for debugging and monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("pickleball-server")

#SERVER STATE 
SERVER_START_TIME = now_ph()  # used for uptime tracking

app = FastAPI(
    title="Pickleball Court Reservation System",
    description="Real-time multi-user reservation system - CS 323 PIT",
    version="1.0.0"
)


#Background Worker (PDC: Task Distribution)
async def stats_heartbeat_worker():
    """Background worker — broadcasts live stats every 10 seconds.
    Runs in parallel with the main connection handler via asyncio."""
    logger.info("[WORKER] Stats heartbeat worker started (interval: 10s)")
    while True:
        await asyncio.sleep(10)
        if connected_clients:
            await broadcast_stats()
            logger.debug(f"[WORKER] Heartbeat broadcast to {len(connected_clients)} clients")


@app.on_event("startup")
async def start_background_workers():
    """Launch background workers when the server starts."""
    load_data()  # Restore previous bookings from disk
    asyncio.create_task(stats_heartbeat_worker())
    logger.info("=" * 55)
    logger.info("Pickleball Court Reservation Server STARTED")
    logger.info(f"Server start time: {SERVER_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)


COURTS = ["Court 1", "Court 2", "Court 3", "Court 4"]
SLOTS  = ["8:00 AM", "9:00 AM", "10:00 AM", "11:00 AM",
          "1:00 PM", "2:00 PM",  "3:00 PM",  "4:00 PM",
          "5:00 PM", "6:00 PM"]

bookings: dict = {}
# Parallel structure: stores session_id of the booker for ownership verification
# Prevents anyone with the name from cancelling someone else's booking
session_owners: dict = {}
history: list = []
recent_activity: list = []
booking_lock = asyncio.Lock()
connected_clients: list[WebSocket] = []

#DATA PERSISTENCE
import os
DATA_FILE = "data.json"

def save_data():
    """Save bookings and history to disk so they survive server restarts."""
    try:
        with open(DATA_FILE, 'w') as f:
            json.dump({
                "bookings": bookings,
                "session_owners": session_owners,
                "history": history,
                "recent_activity": recent_activity
            }, f)
    except Exception as e:
        logger.warning(f"Failed to save data: {e}")

def load_data():
    """Load bookings and history from disk on server startup."""
    global bookings, session_owners, history, recent_activity
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
                bookings = data.get("bookings", {})
                session_owners = data.get("session_owners", {})
                history = data.get("history", [])
                recent_activity = data.get("recent_activity", [])
            logger.info(f"Loaded data: {len(history)} history entries, {len(bookings)} dates")
        except Exception as e:
            logger.warning(f"Failed to load data: {e}")


def init_date(date_str: str):
    """Initialize empty booking slots for a given date."""
    if date_str not in bookings:
        bookings[date_str] = {court: {slot: None for slot in SLOTS} for court in COURTS}
        logger.debug(f"Initialized bookings for date: {date_str}")
    # Always ensure session_owners has matching structure (handles legacy data)
    if date_str not in session_owners:
        session_owners[date_str] = {court: {slot: None for slot in SLOTS} for court in COURTS}


def calc_stats():
    """Compute live system statistics."""
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


def get_uptime():
    """Calculate server uptime since startup."""
    delta = now_ph() - SERVER_START_TIME
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        return f"{days}d {hours}h {minutes}m {seconds}s"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


async def broadcast(message: dict):
    """Send a message to all connected WebSocket clients concurrently."""
    payload = json.dumps(message)
    dead = []
    for client in connected_clients:
        try:
            await client.send_text(payload)
        except Exception as e:
            logger.warning(f"Failed to broadcast to client: {type(e).__name__}")
            dead.append(client)
    for client in dead:
        if client in connected_clients:
            connected_clients.remove(client)
    if dead:
        logger.info(f"Removed {len(dead)} disconnected clients")


async def broadcast_state(date_str: str):
    """Broadcast booking state for a specific date."""
    await broadcast({"type": "state", "date": date_str, "bookings": bookings[date_str]})


async def broadcast_stats():
    """Broadcast current statistics to all clients."""
    await broadcast({"type": "stats", "stats": calc_stats()})


async def add_activity(action: str, name: str, court: str, slot: str):
    """Add an event to the activity feed and broadcast it."""
    event = {
        "action":    action,
        "name":      name,
        "court":     court,
        "slot":      slot,
        "timestamp": now_ph().strftime("%I:%M:%S %p"),
    }
    recent_activity.insert(0, event)
    if len(recent_activity) > 8:
        recent_activity.pop()
    await broadcast({"type": "activity", "event": event})


#API ROUTES

@app.get("/api/health")
async def health_check():
    """
    Health check endpoint — returns server status, uptime, and key metrics.
    Useful for monitoring and verifying the server is responding.
    """
    return JSONResponse({
        "status":           "healthy",
        "server_start":     SERVER_START_TIME.isoformat(),
        "uptime":           get_uptime(),
        "connected_clients": len(connected_clients),
        "total_bookings":   sum(1 for h in history if h["action"] == "Booked"),
        "system":           "Pickleball Court Reservation",
        "version":          "1.0.0",
    })


@app.get("/api/history")
async def get_history():
    """Return full booking history as JSON."""
    return JSONResponse(history)


@app.get("/api/stats")
async def get_stats():
    """Return live system statistics."""
    return JSONResponse(calc_stats())


#WebSocket endpoint

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Each user connects here. asyncio handles ALL users concurrently —
    no user has to wait for another to finish.
    """
    await websocket.accept()
    connected_clients.append(websocket)
    client_host = websocket.client.host if websocket.client else "unknown"
    logger.info(f"[+] Client connected from {client_host} | Total: {len(connected_clients)}")

    today = now_ph().strftime("%Y-%m-%d")
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
                logger.debug(f"Date switch: {date_str}")

            elif msg["type"] == "book":
                date_str, court, slot, name = msg["date"], msg["court"], msg["slot"], msg["name"]
                session_id = msg.get("session_id")  # Browser's session identifier

                # SECURITY: Reject bookings for past dates
                today_ph = now_ph().strftime("%Y-%m-%d")
                if date_str < today_ph:
                    logger.warning(f"[REJECTED] {name} tried to book past date {date_str}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "msg": "You cannot book courts for past dates."
                    }))
                    continue

                init_date(date_str)

                async with booking_lock:
                    if bookings[date_str][court][slot] is None:
                        bookings[date_str][court][slot] = name
                        session_owners[date_str][court][slot] = session_id
                        history.append({
                            "action": "Booked", "name": name, "court": court,
                            "slot": slot, "date": date_str,
                            "timestamp": now_ph().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        logger.info(f"[BOOKED] {name} → {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                        await add_activity("booked", name, court, slot)
                        await broadcast_stats()
                        save_data()
                    else:
                        existing = bookings[date_str][court][slot]
                        logger.warning(f"[CONFLICT] {name} tried to book {court} @ {slot} (already taken by {existing})")
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "msg": f"Sorry! {court} at {slot} is already taken by {existing}."
                        }))

            elif msg["type"] == "cancel":
                date_str, court, slot, name = msg["date"], msg["court"], msg["slot"], msg["name"]
                session_id = msg.get("session_id")
                init_date(date_str)

                async with booking_lock:
                    booked_name = bookings[date_str][court][slot]
                    booked_session = session_owners.get(date_str, {}).get(court, {}).get(slot)

                    # STRICT: Both name AND session_id must match the booking
                    # No legacy fallback — protects against anyone knowing the name
                    name_ok = (booked_name == name)
                    session_ok = (booked_session is not None) and (booked_session == session_id)

                    if name_ok and session_ok:
                        bookings[date_str][court][slot] = None
                        session_owners[date_str][court][slot] = None
                        history.append({
                            "action": "Cancelled", "name": name, "court": court,
                            "slot": slot, "date": date_str,
                            "timestamp": now_ph().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        logger.info(f"[CANCEL] {name} cancelled {court} @ {slot} on {date_str}")
                        await broadcast_state(date_str)
                        await add_activity("cancelled", name, court, slot)
                        await broadcast_stats()
                        save_data()
                    else:
                        if not name_ok:
                            reason = "name mismatch"
                            err_msg = "You can only cancel your own bookings!"
                        else:
                            reason = "session mismatch (different browser)"
                            err_msg = "Only the browser that made this booking can cancel it. Each booking is tied to a unique session."
                        logger.warning(f"[DENIED] {name} tried to cancel {court} @ {slot} ({reason})")
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "msg": err_msg
                        }))

    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
        logger.info(f"[-] Client disconnected from {client_host} | Total: {len(connected_clients)}")
        await broadcast_stats()


app.mount("/", StaticFiles(directory="static", html=True), name="static")


