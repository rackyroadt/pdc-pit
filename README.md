# 🏓 Pickleball Court Reservation System

A real-time, multi-user web application for booking pickleball courts. Built for **CS 323 — Parallel and Distributed Computing** Performance Innovative Task (PIT).

**Live Demo:** https://pdc-pit.onrender.com

---

## Team Members

| Name | Role |
|------|------|
| Samuel Vincent Aque | Documentation & Project Management |
| John Lloyd Arvin Bajolo | Frontend Development & UI/UX Design |
| Abraham Ronaldson Roxas | Stress Testing & Performance Metrics |
| Jiane Rackyle Sarting | Backend Development & Deployment |

---

## Features

- **Real-time booking** — instant updates across all connected users via WebSockets
- **Double-booking prevention** — asyncio.Lock prevents race conditions
- **Live stats dashboard** — active bookings, totals, and most popular court
- **Activity feed** — real-time notifications of all booking events
- **Booking history page** — full audit trail at `/history.html`
- **Multi-date support** — date picker for booking any day
- **Mobile responsive** — works on phones, tablets, and desktops

---

##  Technology Stack

**Backend:**
- Python 3
- FastAPI (web framework)
- Uvicorn (ASGI server)
- asyncio (concurrency)
- WebSockets (real-time communication)

**Frontend:**
- HTML5, CSS3, Vanilla JavaScript

**Deployment:**
- GitHub for version control
- Render.com for hosting

---

##  How to Run Locally

**Step 1 — Install dependencies:**
```
pip install -r requirements.txt
```

**Step 2 — Start the server:**
```
python -m uvicorn server:app
```

**Step 3 — Open the app:**
```
http://localhost:8000
```

**Step 4 — Test with multiple users:**
Open `http://localhost:8000` in multiple browser tabs, enter different names in each, and try booking the same slot from two tabs at once!

---

##  Stress Testing

Make sure the server is running, then in a separate terminal:
```
python stress_test.py
```

This simulates 5, 10, and 20 concurrent users all booking at the same time.

### Test Results

| Concurrent Users | Avg Response (ms) | Max Response (ms) | Success Rate |
|------------------|-------------------|-------------------|--------------|
| 5 | 2,110 | 2,198 | 5/5 ✓ |
| 10 | 2,073 | 2,076 | 10/10 ✓ |
| 20 | 2,132 | 2,136 | 20/20 ✓ |

Response time stays consistent across 5, 10, and 20 users — proving strong horizontal scalability.

---

##  PDC Concepts Demonstrated

| Concept | Where It's Applied |
|---------|--------------------|
| **asyncio** | Server handles multiple WebSocket connections concurrently |
| **asyncio.Lock** | Prevents two users from booking the same slot simultaneously |
| **WebSocket** | Real-time bidirectional communication (no page refresh) |
| **Concurrent request handling** | `asyncio.gather()` in stress test simulates parallel users |
| **Shared state management** | `bookings` dict is shared across all connections |
| **Broadcast pattern** | One booking update is pushed to ALL connected clients instantly |
| **Background worker** | `stats_heartbeat_worker` runs in parallel with request handlers |

---

##  File Structure

```
pdc-pit/
├── server.py          ← FastAPI backend (PDC concepts here!)
├── stress_test.py     ← Simulates concurrent users
├── requirements.txt   ← Python dependencies
├── README.md          ← This file
└── static/
    ├── index.html     ← Main booking interface
    └── history.html   ← Booking history page
```

---

##  Course Information

- **Course:** CS 323 — Parallel and Distributed Computing
- **Academic Year:** 2025–2026
- **Institution:** University of Science and Technology of Southern Philippines (USTP)
- **Project Type:** Performance Innovative Task (PIT)
