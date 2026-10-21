#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime

log = logging.getLogger("lb")

# ---------------------------------------------------------------- config ----
HEADER_READ_TIMEOUT = 10.0       # seconds to receive a full request head
MAX_HEADER_BYTES = 16 * 1024     # reject bigger request heads (431)
RESPONSE_HEAD_TIMEOUT = 30.0     # max wait for a backend to start responding (504)
CONNECT_TIMEOUT = 2.0            # per-backend connect timeout
IDLE_TIMEOUT = 30.0              # close a connection idle in BOTH directions
CHUNK = 64 * 1024

HEALTH_INTERVAL = 5.0            # seconds between probe rounds
HEALTH_TIMEOUT = 1.0             # connect/response timeout per probe
HEALTH_PATH = "/"                # change to "/health" if your backends have one
HEALTH_FALL = 2                  # consecutive failures before marking DOWN
HEALTH_RISE = 2                  # consecutive successes before marking UP

DASHBOARD_FILE = "dashboard.md"
DASHBOARD_INTERVAL = 2.0         # how often to check whether a rewrite is needed


# ----------------------------------------------------------------- state ----
@dataclass
class Backend:
    host: str
    port: int
    healthy: bool = True
    routed: int = 0      # total requests routed here
    active: int = 0      # connections open right now
    fails: int = 0       # consecutive failed probes
    passes: int = 0      # consecutive successful probes

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


class LoadBalancer:
    def __init__(self, backends: list[Backend]):
        self.backends = backends
        self._index = 0

    def next_healthy(self) -> Backend | None:
        """Round-robin pick. Returns None if every backend is down."""
        n = len(self.backends)
        for _ in range(n):
            backend = self.backends[self._index]
            self._index = (self._index + 1) % n
            if backend.healthy:
                return backend
        return None

    def record_probe(self, backend: Backend, ok: bool) -> None:
        """Active health result, with hysteresis to avoid flapping."""
        if ok:
            backend.passes += 1
            backend.fails = 0
            if not backend.healthy and backend.passes >= HEALTH_RISE:
                backend.healthy = True
                log.warning("Server %s RECOVERED", backend.addr)
        else:
            backend.fails += 1
            backend.passes = 0
            if backend.healthy and backend.fails >= HEALTH_FALL:
                backend.healthy = False
                log.warning("Server %s is DOWN (failed %d probes)", backend.addr, backend.fails)

    def record_connect_failure(self, backend: Backend) -> None:
        """Passive health: a real client connect failed, so pull it now."""
        backend.passes = 0
        backend.fails = max(backend.fails + 1, HEALTH_FALL)
        if backend.healthy:
            backend.healthy = False
            log.warning("Server %s is DOWN (connect failed)", backend.addr)


# --------------------------------------------------------------- helpers ----
class RequestError(Exception):
    def __init__(self, status: int, reason: str):
        super().__init__(f"{status} {reason}")
        self.status = status
        self.reason = reason


async def close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def send_error(writer: asyncio.StreamWriter, status: int, reason: str) -> None:
    body = f"{status} {reason}\n".encode()
    writer.write(
        b"HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\n"
        b"Content-Length: %d\r\nConnection: close\r\n\r\n%s"
        % (status, reason.encode(), len(body), body)
    )
    try:
        await writer.drain()
    except OSError:
        pass


class HeadError(Exception):
    """Problem while reading an HTTP head. kind: too_large | truncated | timeout."""
    def __init__(self, kind: str, received: bool):
        super().__init__(kind)
        self.kind = kind
        self.received = received  # did any bytes arrive before the problem?


async def read_head(reader: asyncio.StreamReader, timeout: float) -> tuple[bytes, bytes] | None:
    """Read until the blank line that ends an HTTP head.

    Returns (head, leftover): head excludes the blank line, leftover is any body
    bytes that arrived in the same reads. Returns None if the peer closed
    without sending anything.
    """
    buf = b""

    async def fill() -> None:
        nonlocal buf
        while b"\r\n\r\n" not in buf:
            if len(buf) > MAX_HEADER_BYTES:
                raise HeadError("too_large", True)
            chunk = await reader.read(CHUNK)
            if not chunk:
                if buf:
                    raise HeadError("truncated", True)
                return
            buf += chunk

    try:
        await asyncio.wait_for(fill(), timeout)
    except asyncio.TimeoutError:
        raise HeadError("timeout", bool(buf)) from None

    if not buf:
        return None
    head, _, leftover = buf.partition(b"\r\n\r\n")
    if len(head) > MAX_HEADER_BYTES:
        raise HeadError("too_large", True)
    return head, leftover


async def read_request_head(reader: asyncio.StreamReader) -> tuple[bytes, bytes] | None:
    """Read a client request head. None means: close silently (nothing was sent)."""
    try:
        return await read_head(reader, HEADER_READ_TIMEOUT)
    except HeadError as e:
        if e.kind == "timeout" and not e.received:
            return None
        status = {
            "too_large": (431, "Request Header Fields Too Large"),
            "truncated": (400, "Bad Request"),
            "timeout": (408, "Request Timeout"),
        }[e.kind]
        raise RequestError(*status) from None


async def read_response_head(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """Read a backend response head, mapping failures to 502/504."""
    try:
        result = await read_head(reader, RESPONSE_HEAD_TIMEOUT)
    except HeadError as e:
        if e.kind == "timeout":
            raise RequestError(504, "Gateway Timeout") from None
        raise RequestError(502, "Bad Gateway") from None
    if result is None:
        raise RequestError(502, "Bad Gateway")
    return result


def rewrite_request_head(head: bytes, client_ip: str) -> tuple[bytes, bool]:
    """Inject forwarding headers and force one request per connection.

    Any client-supplied X-Forwarded-For / X-Load-Balancer is dropped so clients
    can't spoof them. Connection is forced to 'close' (so every request passes
    through this function) unless the request is an Upgrade (e.g. WebSocket).
    Returns (new_head, is_upgrade).
    """
    lines = head.split(b"\r\n")
    request_line, headers = lines[0], lines[1:]

    parts = request_line.split(b" ")
    if len(parts) != 3 or not parts[2].startswith(b"HTTP/"):
        raise RequestError(400, "Bad Request")

    kept: list[bytes] = []
    connection_lines: list[bytes] = []
    is_upgrade = False
    for line in headers:
        name = line.split(b":", 1)[0].strip().lower()
        if name in (b"x-forwarded-for", b"x-load-balancer"):
            continue
        if name == b"connection":
            connection_lines.append(line)
            continue
        if name == b"upgrade":
            is_upgrade = True
        kept.append(line)

    kept.extend(connection_lines if is_upgrade else [b"Connection: close"])
    extra = [
        b"X-Forwarded-For: " + client_ip.encode(),
        b"X-Load-Balancer: Python-Asyncio-Engine",
    ]
    return b"\r\n".join([request_line, *extra, *kept]), is_upgrade


def rewrite_response_head(head: bytes) -> bytes:
    """Tell the client we will close after this response.

    Without this, a client may try to reuse the connection for a second request
    and hit a socket we've already closed. 101 (protocol switch) is left alone.
    """
    lines = head.split(b"\r\n")
    status_line, headers = lines[0], lines[1:]
    parts = status_line.split(b" ", 2)
    if len(parts) < 2 or not parts[0].startswith(b"HTTP/"):
        raise RequestError(502, "Bad Gateway")
    if parts[1] == b"101":
        return head
    kept = [h for h in headers if h.split(b":", 1)[0].strip().lower() not in (b"connection", b"keep-alive")]
    kept.append(b"Connection: close")
    return b"\r\n".join([status_line, *kept])


async def connect_backend(lb: LoadBalancer):
    """Try healthy backends in turn. Raises RequestError(502/503) on failure."""
    attempted = False
    for _ in range(len(lb.backends)):
        backend = lb.next_healthy()
        if backend is None:
            break
        attempted = True
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port), CONNECT_TIMEOUT
            )
            return backend, reader, writer
        except (OSError, asyncio.TimeoutError):
            lb.record_connect_failure(backend)
    if attempted:
        raise RequestError(502, "Bad Gateway")
    raise RequestError(503, "Service Unavailable")


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, activity: list[float]) -> None:
    """Copy bytes one way. Half-closes on EOF; tears down if both directions idle."""
    loop = asyncio.get_running_loop()
    try:
        while True:
            try:
                data = await asyncio.wait_for(reader.read(CHUNK), IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                if loop.time() - activity[0] < IDLE_TIMEOUT:
                    continue  # the other direction is still moving data
                log.info("Idle timeout, closing connection")
                writer.close()
                return
            if not data:
                if writer.can_write_eof():
                    writer.write_eof()
                return
            activity[0] = loop.time()
            writer.write(data)
            await writer.drain()
    except (OSError, RuntimeError):
        writer.close()


# -------------------------------------------------------------- handlers ----
async def handle_client(lb: LoadBalancer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    client_ip = peer[0] if peer else "unknown"
    backend: Backend | None = None
    backend_writer: asyncio.StreamWriter | None = None

    try:
        request = await read_request_head(reader)
        if request is None:
            return
        head, leftover = request
        new_head, is_upgrade = rewrite_request_head(head, client_ip)
        payload = new_head + b"\r\n\r\n" + leftover

        backend, backend_reader, backend_writer = await connect_backend(lb)
        backend.routed += 1
        backend.active += 1
        log.info("Routed %s ---> %s", peer, backend.addr)

        backend_writer.write(payload)
        await backend_writer.drain()

        activity = [asyncio.get_running_loop().time()]
        # Stream the request body upstream while we wait for the response, so
        # large uploads can't deadlock against a backend that answers early.
        upstream = asyncio.create_task(pipe(reader, backend_writer, activity))
        try:
            if not is_upgrade:
                resp_head, resp_leftover = await read_response_head(backend_reader)
                writer.write(rewrite_response_head(resp_head) + b"\r\n\r\n" + resp_leftover)
                await writer.drain()
            await pipe(backend_reader, writer, activity)  # returns when the backend is done
        finally:
            upstream.cancel()
            await asyncio.gather(upstream, return_exceptions=True)
    except RequestError as e:
        if e.status in (502, 503, 504):
            log.error("%s (backend: %s)", e, backend.addr if backend else "none")
        else:
            log.warning("Rejected request from %s: %s", client_ip, e)
        await send_error(writer, e.status, e.reason)
    except (OSError, asyncio.TimeoutError) as e:
        log.warning("Connection error (%s): %s", backend.addr if backend else "no backend", e)
    finally:
        if backend is not None:
            backend.active -= 1
        await close_writer(backend_writer)
        await close_writer(writer)


# ------------------------------------------------------- background tasks ----
async def probe(backend: Backend) -> bool:
    """HTTP health probe. Any response below 500 counts as alive.

    For a strict check, point HEALTH_PATH at a /health endpoint and require
    status == 200 below.
    """
    writer = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port), HEALTH_TIMEOUT
        )
        request = (
            f"GET {HEALTH_PATH} HTTP/1.1\r\nHost: {backend.addr}\r\n"
            "User-Agent: lb-health-check\r\nConnection: close\r\n\r\n"
        )
        writer.write(request.encode())
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), HEALTH_TIMEOUT)
        parts = status_line.split()
        return len(parts) >= 2 and parts[0].startswith(b"HTTP/") and parts[1].isdigit() and int(parts[1]) < 500
    except (OSError, asyncio.TimeoutError, ValueError):
        return False
    finally:
        await close_writer(writer)


async def health_check_loop(lb: LoadBalancer) -> None:
    log.info("Health check loop started (every %.0fs)", HEALTH_INTERVAL)
    while True:
        try:
            results = await asyncio.gather(*(probe(b) for b in lb.backends))
            for backend, ok in zip(lb.backends, results):
                lb.record_probe(backend, ok)
        except Exception:
            log.exception("Health check round failed")
        await asyncio.sleep(HEALTH_INTERVAL)


def render_dashboard_body(lb: LoadBalancer, listen_url: str) -> str:
    rows = [
        f"**Load Balancer Listening Address:** `{listen_url}`\n",
        "### Cluster Backend Nodes Status\n",
        "| Node Target Address | System Health Status | Active Connections | Total Routed Requests |",
        "| :--- | :---: | :---: | :---: |",
    ]
    for b in lb.backends:
        status = "🟢 HEALTHY" if b.healthy else "🔴 DOWN"
        rows.append(f"| `{b.addr}` | {status} | {b.active} | {b.routed} |")
    return "\n".join(rows) + "\n"


def write_atomic(path: str, text: str) -> None:
    """Write to a temp file, then swap it in so readers never see a partial file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


async def dashboard_loop(lb: LoadBalancer, listen_url: str) -> None:
    """Rewrites the dashboard only when its contents changed."""
    last_body = None
    while True:
        try:
            body = render_dashboard_body(lb, listen_url)
            if body != last_body:
                stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                text = f"# System Infrastructure Dashboard\n\n**Last Update:** `{stamp}`\n\n{body}"
                await asyncio.to_thread(write_atomic, DASHBOARD_FILE, text)  # keep disk I/O off the event loop
                last_body = body
        except Exception:
            log.exception("Dashboard write failed")
        await asyncio.sleep(DASHBOARD_INTERVAL)


# ------------------------------------------------------------------ main ----
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Asyncio HTTP load balancer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--backends", nargs="+", metavar="HOST:PORT",
        default=["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"],
    )
    return parser.parse_args()


def parse_backend(spec: str) -> Backend:
    host, _, port = spec.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"Invalid backend '{spec}', expected HOST:PORT")
    return Backend(host, int(port))


async def main(args: argparse.Namespace) -> None:
    lb = LoadBalancer([parse_backend(s) for s in args.backends])
    listen_url = f"http://{args.host}:{args.port}"

    # Keep references so the tasks can't be garbage collected mid-run.
    tasks = [
        asyncio.create_task(health_check_loop(lb), name="health-check"),
        asyncio.create_task(dashboard_loop(lb, listen_url), name="dashboard"),
    ]
    server = await asyncio.start_server(
        lambda r, w: handle_client(lb, r, w), args.host, args.port
    )
    log.info("Load balancer listening on %s -> %s", listen_url, ", ".join(b.addr for b in lb.backends))

    try:
        async with server:
            await server.serve_forever()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        log.info("Shutting down load balancer safely.")
