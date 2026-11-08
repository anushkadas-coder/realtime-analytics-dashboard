# Real-Time Analytics Dashboard

⭐ A real-time analytics platform that ingests a **live event stream**, stores it in PostgreSQL, fans it out through Redis pub/sub, and shows it on a React dashboard that updates within about a second.

**Live demo:** https://realtime-analytics-dashboard-lime.vercel.app
**Author:** [Anushka](https://anushkadas-coder.github.io) · [GitHub](https://github.com/anushkadas-coder) · anushkadas05das@gmail.com

---

## What it does

| Stream | What it is | What the dashboard answers |
|---|---|---|
| **Wikipedia Live** (`source=wikipedia`) | Every edit, page creation, log action and category change across all Wikimedia projects, from the public [EventStreams](https://stream.wikimedia.org/v2/ui/) `recentchange` feed (about 30 events/s). | How much is changing right now? Which wikis are busiest? What share of edits are made by bots? Who is editing? |
| **Website tracker** (`source=site`, API only) | A first-party, dependency-free tracker (`tracker/rta.js`) for a personal website. | How many people are on my site right now? Where do they come from (LinkedIn, GitHub, Google...)? Which pages do they read and which links do they click? |
| **Simulator** (`source=demo`, API only) | Synthetic traffic from `scripts/simulate.py`. | Load and latency benchmarks. |

**Stack:** Python · Flask · gevent/gunicorn · flask-sock (WebSocket) · Redis (pub/sub, HyperLogLog, counters) · PostgreSQL · React + Vite + Recharts · Docker · Kubernetes (HPA, PDB, Ingress)

## Architecture

```mermaid
flowchart LR
  WM[Wikimedia EventStreams<br/>SSE, ~30 ev/s] --> CONN[wikipedia connector<br/>Last-Event-ID resume, batching]
  SITE[personal website<br/>rta.js tracker] -->|sendBeacon /api/collect| ING
  CONN -->|POST /api/events| ING
  SIM[simulator / load tests] -->|POST /api/events| ING
  subgraph Pod["backend pod (N replicas, HPA 2-10)"]
    ING[Ingest API] -->|1 txn: raw insert + rollup upsert| PG[(PostgreSQL)]
    ING -->|pipeline: INCR, PFADD, HINCRBY, PUBLISH| R[(Redis)]
    R -->|SUBSCRIBE, one per worker| HUB[Fan-out hub<br/>10 ms coalescing,<br/>per-source subscriptions]
    HUB --> WS[/ws?source=.../]
    API[GET /api/stats/*] --> PG
    API --> R
  end
  WS --> UI[React dashboard]
  API --> UI
```

## How it works

| Concern | Design |
|---|---|
| **Wikipedia connector** | Async SSE client (`connectors/wikipedia.py`). Tracks the stream's `id:` and reconnects with `Last-Event-ID`, so a dropped connection resumes where it stopped. Jittered exponential back-off. Batches to the ingest API every 250 ms or 200 events, with a bounded buffer if the backend is down. Runs as a single-replica `Recreate` Deployment so events are not double-counted during a rollout. |
| **Website tracker** | `rta.js` (about 150 lines, no dependencies): `page_view` (including SPA route changes), `link_click` classified as github / linkedin / resume / email / project / external, and `engaged` after 30 s of visible time. Traffic source comes from `utm_source` or the referrer. Sends `text/plain` beacons, so there is no CORS preflight. No cookies, no IPs, no fingerprinting; Do Not Track / GPC and automated browsers are skipped. |
| **Ingest** | `POST /api/events` (trusted producers) and `POST /api/collect` (browser beacons: origin allow-list, bot user-agent filter, per-IP rate limit, source forced to `site`). Raw rows and the per-minute rollup upsert are written in **one transaction**. |
| **Live counters** | One Redis pipeline per batch, keyed by source: per-minute and daily counters, HyperLogLog for unique users, per-minute hashes for top-N breakdowns, the recent-events feed, and `PUBLISH`. |
| **Fan-out** | Each gunicorn worker holds one Redis subscription, not one per client. A flusher coalesces messages for 10 ms, groups them by source and serializes once per watched source. Each client has a bounded queue, so slow consumers drop frames instead of slowing everyone else. Any pod can ingest and every pod's viewers see the event, so the WebSocket tier scales horizontally without sticky sessions. |
| **Storage** | `events(source, event_type, user_id, value, props jsonb, occurred_at, ...)` with `BRIN(occurred_at)`, `B-tree(source, event_type, occurred_at DESC)` and `B-tree(user_id, occurred_at DESC)`, plus `event_rollup_minute` (PK `source, bucket, event_type`). Charts read the rollup and never scan raw events. |
| **Frontend** | One WebSocket subscription per tab. Frames are buffered and committed to React state every 250 ms. Latency is measured when a frame arrives. The 60-minute aggregates are re-synced from REST every minute. Colors are fixed per event type and come from a color-blind-friendly palette, in light and dark. |
| **Ops** | `/healthz` (liveness), `/readyz` (DB + Redis), `/metrics` (Prometheus: ingest latency histogram, ws clients gauge, frames sent). |
| **Kubernetes** | Readiness/liveness probes, `maxUnavailable: 0` rolling updates, `preStop` drain, topology spread, PDB `minAvailable: 1`, HPA on CPU 65% / memory 75% (2 to 10 pods, fast scale-up, slow scale-down so sockets are not dropped needlessly). Also: Wikipedia connector, Postgres StatefulSet + PVC, Redis, frontend (+HPA), and ingress-nginx with 1 h socket timeouts. |

## Deployment modes

The same codebase runs in two modes.

| Always-on (Docker / Kubernetes / local) | Vercel (serverless demo, what the live link uses) |
|---|---|
| WebSocket push, Redis pub/sub fan-out | HTTP polling of `/api/live` with an id cursor (about 1 s latency) |
| Always-on Wikipedia connector | `/api/ingest/wikipedia`: pulled on demand while someone has the dashboard open. A Postgres advisory lock lets concurrent viewers share one pull, and back-to-back pulls resume via Last-Event-ID |
| Redis counters, HyperLogLog, top-N hashes | The same numbers computed in Postgres (Redis is used if `REDIS_URL` is set) |
| Raw events kept | Raw Wikipedia rows pruned after `WIKI_RETENTION_HOURS` (default 48); rollups kept |

Vercel has no WebSockets or always-on processes, so the backend switches modes automatically there.

## Run it

### Deploy on Vercel (what the live demo uses)

1. Import this repo in Vercel. Set **Application Preset** to **Other** and keep the root directory as `./`. The included `vercel.json` handles the build.
2. In the project, go to **Storage → Create Database → Neon (Postgres)**. Turn Auth off and connect it with the env prefix `DATABASE`. This sets `DATABASE_URL` and `DATABASE_URL_UNPOOLED` (the second is used for the pull lock).
3. **Redeploy.** Tables are created on the first request, and the dashboard shows **Live** once data starts flowing.

### Docker Compose (full version with WebSockets and Redis)

```bash
docker compose up --build -d          # dashboard on http://localhost:8080, Wikipedia connector included
docker compose --profile demo up simulator   # optional synthetic traffic
docker compose up --scale backend=3   # watch cross-pod fan-out
```

### macOS, one double-click

Double-click `start.command`. If Docker is running it uses `docker compose`; otherwise it sets everything up locally (Python 3.12 via `uv`, PostgreSQL via the `pgserver` wheel, Redis built from source) and opens http://localhost:5050.

### Local, manual (no Docker)

```bash
# Postgres on :5432 (db "analytics") and Redis on :6379 must be running
cd backend && pip install -r requirements.txt
gunicorn -c gunicorn.conf.py wsgi:app                      # :5000, schema auto-applied
cd ../frontend && npm install && npm run dev               # :5173, proxies /api and /ws
pip install aiohttp
python connectors/wikipedia.py                             # live Wikipedia stream
#   offline: python connectors/wikipedia.py --replay connectors/fixtures/wikimedia_recentchange_sample.jsonl
python scripts/simulate.py --rate 15 --batch 3             # optional synthetic traffic
```

### Kubernetes (minikube / kind / EKS)

```bash
docker build -f backend/Dockerfile -t ghcr.io/anushkadas-coder/rta-backend:latest .
docker build -t ghcr.io/anushkadas-coder/rta-frontend:latest frontend
docker build -t ghcr.io/anushkadas-coder/rta-wikipedia-connector:latest connectors
# push all three; k8s/ already points at ghcr.io/anushkadas-coder
kubectl apply -f k8s/
kubectl -n analytics get hpa -w        # needs metrics-server
```

## Add the tracker to your own website

1. Deploy the backend somewhere with **HTTPS** (browsers block beacons to plain HTTP from an HTTPS site).
2. Make sure `COLLECT_ORIGINS` includes your site. It defaults to `https://anushkadas-coder.github.io`.
3. Add one line before `</body>`:
   ```html
   <script defer src="https://YOUR-DASHBOARD-HOST/rta.js"
           data-endpoint="https://YOUR-DASHBOARD-HOST/api/collect"></script>
   ```
4. Tag links in your posts, for example `https://anushkadas-coder.github.io/?utm_source=linkedin&utm_campaign=github-post`. LinkedIn often strips the referrer, so the UTM tag makes attribution reliable.

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/api/events` | `{source?, type, user_id, value?, props?, ts?}` or `{events:[...]}` → `202 {accepted}` |
| POST | `/api/collect` | browser beacons (text/plain JSON `{events:[...]}`, at most 20), source forced to `site` |
| GET | `/api/stats/summary` | live KPIs for every source |
| GET | `/api/stats/timeseries?source=&minutes=60` | per-minute counts by type (rollup) |
| GET | `/api/stats/breakdown?source=&minutes=60` | totals by type (rollup) |
| GET | `/api/stats/dims?source=&key=&minutes=60` | top-N for `ref_source`, `path`, `target`, `wiki`, `bot` |
| GET | `/api/events/recent?source=&limit=50` | latest events |
| GET | `/rta.js` | website tracker |
| WS | `/ws?source=` | frames: `hello`, `events` (batched, that source only), `stats` (1 Hz, all sources), `ping` |

## Benchmarks

The repo includes scripts to measure the system end to end. Run them on your own machine and fill in the table (use the full Docker version, since Vercel mode has no WebSockets).

| What | Command | My result |
|---|---|---|
| Ingest throughput | `python scripts/simulate.py --rate 5000 --batch 100 --total 100000` | _to be measured_ |
| WebSocket latency (1 client) | `python scripts/bench_ws.py --clients 1 --events 300` | _to be measured_ |
| Concurrent connections | `python scripts/bench_ws.py --clients 1000 --events 100` | _to be measured_ |
| Query time, 5M rows (raw vs indexed vs rollup) | `python scripts/bench_query.py --seed 5000000` | _to be measured_ |
| Uptime under rollouts on Kubernetes | `python scripts/uptime_probe.py --url https://<host> --minutes 60` | _not yet run_ |

When you record results, note the hardware (CPU cores, RAM, OS) and how the stack was run.

## Design notes and trade-offs

- **Why not pull data from LinkedIn?** There is no public real-time API, and scraping breaks LinkedIn's terms. The tracker measures what LinkedIn sends to my site (visits and clicks) with first-party data instead.
- **Why Redis pub/sub and not Kafka?** Pub/sub is fire-and-forget, so a pod that is down misses messages. That is acceptable here because Postgres is the source of truth and clients re-seed from REST when they reconnect. If replay or consumer groups mattered, I would move to Redis Streams or Kafka. The Wikipedia side gets replay from `Last-Event-ID` instead.
- **Why a rollup table and not just indexes?** Indexes speed up time-window queries, but the rollup is far faster because the dashboard reads pre-aggregated rows instead of every raw event. The cost is one extra upsert per (minute, source, type) per batch.
- **Why coalesce for 10 ms, and why per-source subscriptions?** At high client counts the cost is socket writes and serialization. Batching trades at most 10 ms of latency for far fewer writes, and subscriptions stop clients from receiving streams they do not render.
- **Scaling the write path further:** partition `events` by day (`PARTITION BY RANGE`), switch to `COPY`, or put a queue in front and batch-insert asynchronously.
- **HPA and WebSockets:** CPU-based HPA works, but scaling down kills sockets. Hence the 5-minute stabilization window, the PDB and client back-off. A custom metric (`ws_clients` per pod via prometheus-adapter) would be a better scaling signal.

## Layout

```
api/         Vercel serverless entrypoint (Flask app as a Python function)
backend/     Flask app (app/), gunicorn config, Dockerfile (build context = repo root)
frontend/    React dashboard, nginx.conf, Dockerfile
connectors/  wikipedia.py (EventStreams -> ingest), recorded sample for offline replay
tracker/     rta.js, the website tracker served at /rta.js
k8s/         namespace, config, postgres, redis, backend (+HPA, PDB), connector, frontend (+HPA), ingress
scripts/     simulate.py, bench_ws.py, bench_query.py, uptime_probe.py
```

## Contact

Anushka · https://anushkadas-coder.github.io · https://github.com/anushkadas-coder · anushkadas05das@gmail.com
