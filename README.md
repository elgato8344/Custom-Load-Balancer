# Asyncio HTTP Load Balancer

A small reverse-proxy load balancer written in pure Python (standard library only). It accepts HTTP connections, spreads them across a pool of backend servers in round-robin order, watches backend health, and writes a live Markdown status dashboard.

It's built as a learning project and a handy local dev tool. It is **not** a production replacement for nginx, HAProxy or Envoy (see [Limitations](#what-it-does-not-do)).

## Quick start

Requirements: Python 3.9+ and no third-party packages.

```bash
# 1. Start some backends (any HTTP servers will do)
python -m http.server 8001
python -m http.server 8002
python -m http.server 8003

# 2. Start the load balancer (defaults: listen on 127.0.0.1:8080, backends 8001-8003)
python load_balancer.py

# 3. Send traffic
curl http://127.0.0.1:8080
```

Custom address and backends:

```bash
python load_balancer.py --host 0.0.0.0 --port 9000 \
    --backends 127.0.0.1:8001 127.0.0.1:8002 10.0.0.5:8000
```

| Option | Default | Meaning |
| :--- | :--- | :--- |
| `--host` | `127.0.0.1` | Address the load balancer listens on |
| `--port` | `8080` | Port the load balancer listens on |
| `--backends` | `127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003` | One or more `HOST:PORT` backends |

Tunables such as timeouts and health-check thresholds are constants at the top of `load_balancer.py`.

## What it does

**Traffic routing**
- Distributes each new connection across healthy backends in **round-robin** order.
- Skips backends that are down. If a connect fails, it marks that backend down immediately and tries the next one (**passive health checking**).
- Returns `503 Service Unavailable` when every backend is already marked down, `502 Bad Gateway` when it tried and all connects or responses failed, and `504 Gateway Timeout` if a backend never starts responding.

**Request handling**
- Reads the full request head before choosing a backend, so slow or empty connections don't tie up backend resources.
- Injects `X-Forwarded-For` (the real client IP) and `X-Load-Balancer` into every request. Any client-supplied `X-Forwarded-For` is replaced, so clients can't spoof their address.
- Forces `Connection: close` in both directions so every request passes through the header injector. WebSocket upgrades are detected and passed through untouched.
- Streams request bodies to the backend while waiting for the response, so large uploads work.
- Rejects bad input: `400` for malformed requests, `408` for clients that send headers too slowly, `431` for oversized headers.

**Health checking**
- Every 5 seconds, probes all backends **concurrently** with a real HTTP request (`GET /`), so one slow backend doesn't delay the others.
- Uses **hysteresis** to avoid flapping: 2 consecutive failures to mark a backend down, 2 consecutive successes to bring it back.

**Connection hygiene**
- Closes both sides of every connection, including on errors, so sockets don't leak.
- Passes half-closes through correctly, and tears down connections that are idle in both directions for 30 seconds.

**Dashboard**
- Writes `dashboard.md` with each backend's health, active connections and total routed requests.
- Rewrites it only when something changes, off the event loop, and atomically (temp file plus `os.replace`), so a reader never sees a half-written file.

Example `dashboard.md`:

```markdown
# System Infrastructure Dashboard

**Last Update:** `2026-10-21 03:20:31`

**Load Balancer Listening Address:** `http://127.0.0.1:9000`

### Cluster Backend Nodes Status

| Node Target Address | System Health Status | Active Connections | Total Routed Requests |
| :--- | :---: | :---: | :---: |
| `127.0.0.1:8001` | 🟢 HEALTHY | 0 | 9 |
| `127.0.0.1:8002` | 🔴 DOWN | 0 | 9 |
| `127.0.0.1:8003` | 🟢 HEALTHY | 0 | 9 |
```

## What it does not do

- **No keep-alive to clients.** Each connection carries exactly one request. This keeps header injection reliable but costs a new TCP connection per request.
- **No TLS/HTTPS.** Traffic is plain HTTP. Put it behind a TLS terminator, or use a production proxy if you need encryption.
- **Round-robin only.** No weights, sticky sessions, or least-connections (the per-backend `active` counter is already tracked, so adding least-connections is a small change).
- **No request routing rules.** It doesn't route by path, host or header. Every request goes to the pool.
- **No retry of requests that failed mid-flight.** It retries connection failures, not a request that a backend dropped halfway through.
- **No rate limiting, authentication or caching.**
- **No HTTP/2 or HTTP/3.**
- **Basic protection only** against slow or oversized requests (header timeout, header size cap, idle timeout). It isn't hardened against deliberate abuse.
- **Single process, dynamic config not supported.** Backends are set at startup; changing them means a restart.
- **Health checks accept any response below 500 on `GET /`.** It confirms a server is answering, not that your application logic is healthy. Point `HEALTH_PATH` at a real `/health` endpoint for stricter checks.

## Testing

### 1. Basic round-robin

Start three backends and the load balancer (see Quick start), then:

```bash
for i in 1 2 3 4 5 6; do curl -s http://127.0.0.1:8080 -o /dev/null -w "%{http_code}\n"; done
```

The log shows requests alternating across ports 8001, 8002 and 8003.

### 2. Verify the injected headers

`python -m http.server` doesn't show request headers, so use a backend that echoes them. Save as `echo_backend.py`:

```python
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = f"backend {port}\n" + "".join(f"{k}: {v}\n" for k, v in self.headers.items())
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
```

Run `python echo_backend.py 8001` (and 8002, 8003), then:

```bash
curl http://127.0.0.1:8080
```

Expected output includes:

```
backend 8001
X-Forwarded-For: 127.0.0.1
X-Load-Balancer: Python-Asyncio-Engine
Connection: close
```

Spoofing check: this should still show `127.0.0.1`, not `6.6.6.6`:

```bash
curl -H "X-Forwarded-For: 6.6.6.6" http://127.0.0.1:8080
```

### 3. Failure and recovery

1. Stop one backend (Ctrl+C). Requests keep succeeding immediately because the failed connect marks it down and the request is retried on another backend.
2. Within about 10 seconds the health check confirms it and logs `Server 127.0.0.1:800X is DOWN`. `dashboard.md` flips to 🔴 DOWN.
3. Restart the backend. After two good probes (about 10 seconds) the log shows `RECOVERED` and the dashboard returns to 🟢 HEALTHY.
4. Stop all backends. The first request returns `502 Bad Gateway`, and later ones return `503 Service Unavailable` until a backend returns.

### 4. Error handling

```bash
# Oversized headers -> 431
python -c "import socket; s=socket.create_connection(('127.0.0.1',8080)); s.sendall(b'GET / HTTP/1.1\r\nX: '+b'a'*20000+b'\r\n\r\n'); print(s.recv(100))"

# Malformed request -> 400
printf 'hello\r\n\r\n' | nc 127.0.0.1 8080
```

### 5. Large upload

With a backend that accepts POST, upload a large body to confirm streaming works:

```bash
curl -X POST --data-binary @bigfile.bin http://127.0.0.1:8080
```

## What it could be used for

- **Learning:** a readable example of asyncio streams, TCP proxying, HTTP framing, health-check hysteresis and graceful failure handling.
- **Local development:** run several instances of your app and test how it behaves behind a load balancer, including what happens when an instance dies.
- **Demoing failover:** the live dashboard makes node failures and recoveries easy to show.
- **Testing client behavior:** check how your clients handle `502`/`503`/`504` responses, `X-Forwarded-For` handling, or connection closes.
- **Small internal tools or home-lab setups** where a few HTTP services need simple round-robin distribution without installing a full proxy.
- **A base to extend:** weighted or least-connections balancing, path-based routing, `/health` endpoint checks, or Prometheus-style metrics.

## Project layout

```
load_balancer.py   # the load balancer (single file, standard library only)
dashboard.md       # generated at runtime
README.md
```

## How it works (short version)

1. A client connects. The balancer reads the request head (with a timeout and size cap).
2. It rewrites the head: adds forwarding headers and `Connection: close`.
3. It picks the next healthy backend in round-robin order and connects, retrying on others if the connect fails.
4. It forwards the request, reads the backend's response head, adds `Connection: close`, and sends it to the client.
5. It streams the rest of the response (and any request body) until the backend is done, then closes both sockets.
6. In the background, one task probes the backends and updates health state, and another keeps `dashboard.md` current.
