# 🏓 Pickleball Court Reservation System
### CS 323 — Parallel and Distributed Computing

---

## How to Run

**Step 1 — Install dependencies:**
```
pip install -r requirements.txt
```

**Step 2 — Start the server:**
```
uvicorn server:app --reload
```

**Step 3 — Open the app:**
```
http://localhost:8000
```

**Step 4 — Test with multiple users:**
Open `http://localhost:8000` in multiple browser tabs.
Enter different names in each tab. Try booking the same slot from two tabs at once!

---

## Stress Test

Make sure the server is running, then:
```
python stress_test.py
```

This simulates 5, 10, and 20 concurrent users all booking at the same time.
Screenshot the output — it goes in your brochure!

---

## PDC Concepts in This System

| Concept | Where it appears |
|---|---|
| **asyncio** | Server handles multiple WebSocket connections at the same time |
| **asyncio.Lock** | Prevents two users from booking the same slot simultaneously |
| **WebSocket** | Real-time bidirectional communication (no page refresh) |
| **Concurrent request handling** | `asyncio.gather()` in stress test simulates parallel users |
| **Shared state** | `bookings` dict is shared across all connections |
| **Broadcast** | One booking update is pushed to ALL connected clients instantly |

---

## File Structure

```
pickleball/
├── server.py          ← FastAPI backend (PDC concepts here!)
├── stress_test.py     ← Simulates concurrent users
├── requirements.txt   ← Python dependencies
├── README.md          ← This file
└── static/
    └── index.html     ← Frontend (HTML/CSS/JS)
```
