"""Persistent mailbox storage for a site's front end.

The front end stores a message with POST /mail and reads mail with
GET /mailbox/<user> or GET /mailbox/<user>/<id>. PUT /mailbox/<user>/<id>
sets the read flag and DELETE /mailbox/<user>/<id> removes a message. Each
backend keeps its own append-only log. When configured with a peer, it pulls
that peer's log so a node returning from downtime can catch up.

"""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


PORT = 8000
MAX_BODY_SIZE = 1024 * 1024
USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class StorageNode:
    def __init__(self, data_file):
        self.data_file = Path(data_file)
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        self.messages = {}
        self.log = []
        self.lock = threading.RLock()
        self.peer_reachable = None
        self._load()

    def _load(self):
        if not self.data_file.exists():
            return
        raw = self.data_file.read_bytes()
        offset = 0
        for line_number, line in enumerate(raw.splitlines(keepends=True), start=1):
            if line.strip():
                try:
                    message = json.loads(line)
                    self._validate_message(message)
                    self._apply_message(message, persist=False)
                except (json.JSONDecodeError, ValueError) as error:
                    if not line.endswith(b"\n") and offset + len(line) == len(raw):
                        # A crash left a partial last line; drop it.
                        print(f"dropping partial last entry of {self.data_file}", flush=True)
                        self.data_file.write_bytes(raw[:offset])
                        return
                    raise ValueError(
                        f"invalid mailbox log entry at {self.data_file}:{line_number}: {error}"
                    ) from error
            offset += len(line)
        if raw and not raw.endswith(b"\n"):
            with self.data_file.open("ab") as data:
                data.write(b"\n")

    @staticmethod
    def _validate_message(message):
        required = ("id", "from", "to", "subject", "body")
        if not isinstance(message, dict) or any(
            not isinstance(message.get(field), str) for field in required
        ):
            raise ValueError("message must contain string id, from, to, subject, and body")
        if not message["id"] or not USER_RE.fullmatch(message["to"]):
            raise ValueError("message id and recipient must be valid non-empty values")
        if "read" in message and not isinstance(message["read"], bool):
            raise ValueError("message read field must be a boolean")
        if "deleted" in message and not isinstance(message["deleted"], bool):
            raise ValueError("message deleted field must be a boolean")
        rv = message.get("rv", 0)
        if isinstance(rv, bool) or not isinstance(rv, int):
            raise ValueError("message rv field must be an integer")

    @staticmethod
    def _merge(existing, incoming):
        if existing["to"] != incoming["to"]:
            raise ValueError(f"message id {incoming['id']!r} has conflicting contents")
        if not existing["deleted"] and not incoming["deleted"]:
            for field in ("from", "subject", "body"):
                if existing[field] != incoming[field]:
                    raise ValueError(f"message id {incoming['id']!r} has conflicting contents")
        merged = dict(existing)
        if incoming["deleted"]:
            merged["deleted"] = True
        if (incoming["rv"], incoming["read"]) > (existing["rv"], existing["read"]):
            merged["rv"] = incoming["rv"]
            merged["read"] = incoming["read"]
        return merged

    def _apply_message(self, message, persist=True):
        message = dict(message)
        message.setdefault("read", False)
        message.setdefault("rv", 0)
        message.setdefault("deleted", False)
        existing = self.messages.get(message["id"])
        if existing is not None:
            message = self._merge(existing, message)
            if message == existing:
                return False

        if persist:
            with self.data_file.open("a", encoding="utf-8") as data:
                data.write(json.dumps(message, separators=(",", ":"), ensure_ascii=True) + "\n")
                data.flush()
                os.fsync(data.fileno())

        if existing is None:
            self.messages[message["id"]] = message
            self.log.append(message)
        else:
            existing.update(message)
        return True

    def add_message(self, message):
        self._validate_message(message)
        message = {key: message[key] for key in ("id", "from", "to", "subject", "body")}
        with self.lock:
            return self._apply_message(message)

    def set_read(self, user, message_id, read):
        with self.lock:
            message = self.messages.get(message_id)
            if message is None or message["deleted"] or message["to"] != user:
                return None
            rv = max(int(time.time() * 1000), message["rv"] + 1)
            self._apply_message(dict(message, read=read, rv=rv))
            return dict(message)

    def delete_message(self, user, message_id):
        with self.lock:
            message = self.messages.get(message_id)
            if message is not None and (message["deleted"] or message["to"] != user):
                return False
            tombstone = dict(message) if message is not None else {
                "id": message_id, "to": user, "from": "", "subject": "", "body": "",
            }
            tombstone["deleted"] = True
            self._apply_message(tombstone)
            return message is not None

    def import_messages(self, messages):
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")
        imported = 0
        for message in messages:
            self._validate_message(message)
            with self.lock:
                imported += self._apply_message(message)
        return imported

    def mailbox(self, user):
        with self.lock:
            return [
                dict(message) for message in self.log
                if message["to"] == user and not message["deleted"]
            ]

    def get_message(self, user, message_id):
        with self.lock:
            message = self.messages.get(message_id)
            if message is None or message["deleted"] or message["to"] != user:
                return None
            return dict(message)

    def all_messages(self):
        with self.lock:
            return [dict(message) for message in self.log]


def http_get(url, timeout=2.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read() or b"{}")


def http_post(url, obj, timeout=2.0):
    request = urllib.request.Request(
        url,
        data=json.dumps(obj).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read() or b"{}")


class RequestHandler(BaseHTTPRequestHandler):
    node = None
    peer_url = ""
    protocol_version = "HTTP/1.1"

    def send(self, status, obj):
        payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_error(self, code, message=None, explain=None):
        self.send(code, {"error": message or self.responses.get(code, ("error",))[0]})

    def body(self):
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError as error:
            raise ValueError("a valid Content-Length header is required") from error
        if length < 0 or length > MAX_BODY_SIZE:
            raise ValueError(f"request body must be at most {MAX_BODY_SIZE} bytes")
        raw_body = self.rfile.read(length)
        if len(raw_body) != length:
            raise ValueError("request body ended before Content-Length bytes were received")
        try:
            result = json.loads(raw_body)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("request body must be valid JSON") from error
        if not isinstance(result, dict):
            raise ValueError("request body must be a JSON object")
        return result

    @staticmethod
    def _mailbox_path(path):
        parts = path.split("/")
        if len(parts) not in (3, 4) or parts[1] != "mailbox":
            return None
        user = urllib.parse.unquote(parts[2])
        message_id = urllib.parse.unquote(parts[3]) if len(parts) == 4 else None
        if not USER_RE.fullmatch(user) or (message_id is not None and not message_id):
            return None
        return user, message_id

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/status":
            return self.send(200, {
                "messages": len(self.node.all_messages()),
                "peer": self.peer_url or None,
                "peer_reachable": self.node.peer_reachable,
            })
        if parsed.path == "/replication":
            return self.send(200, {"messages": self.node.all_messages()})

        mailbox = self._mailbox_path(parsed.path)
        if mailbox is None:
            return self.send(404, {"error": "unknown path"})
        user, message_id = mailbox
        if message_id is None:
            return self.send(200, {"user": user, "messages": self.node.mailbox(user)})
        message = self.node.get_message(user, message_id)
        if message is None:
            return self.send(404, {"error": "no such message", "id": message_id})
        return self.send(200, message)

    def do_POST(self):
        parsed = urllib.parse.urlsplit(self.path)
        try:
            body = self.body()
            if parsed.path == "/mail":
                created = self.node.add_message(body)
                return self.send(200, {"id": body["id"], "stored": True, "duplicate": not created})
            if parsed.path == "/replication":
                imported = self.node.import_messages(body.get("messages"))
                return self.send(200, {"imported": imported})
            return self.send(404, {"error": "unknown path"})
        except ValueError as error:
            return self.send(400, {"error": str(error)})

    def do_PUT(self):
        parsed = urllib.parse.urlsplit(self.path)
        try:
            body = self.body()
            mailbox = self._mailbox_path(parsed.path)
            if mailbox is None or mailbox[1] is None:
                return self.send(404, {"error": "unknown path"})
            if not isinstance(body.get("read"), bool):
                raise ValueError('body must contain "read": true or "read": false')
            user, message_id = mailbox
            message = self.node.set_read(user, message_id, body["read"])
            if message is None:
                return self.send(404, {"error": "no such message", "id": message_id})
            return self.send(200, {key: message[key] for key in ("id", "from", "subject", "read")})
        except ValueError as error:
            return self.send(400, {"error": str(error)})

    def do_DELETE(self):
        parsed = urllib.parse.urlsplit(self.path)
        mailbox = self._mailbox_path(parsed.path)
        if mailbox is None or mailbox[1] is None:
            return self.send(404, {"error": "unknown path"})
        user, message_id = mailbox
        if not self.node.delete_message(user, message_id):
            return self.send(404, {"error": "no such message", "id": message_id})
        return self.send(200, {"id": message_id, "deleted": True})


def peer_address(value, default_port):
    if not value:
        return ""
    parsed = urllib.parse.urlsplit(value if "://" in value else f"//{value}", scheme="http")
    if not parsed.hostname:
        raise ValueError(f"invalid peer address: {value!r}")
    port = parsed.port or default_port
    return f"http://{parsed.hostname}:{port}"


def sync_peer(node, peer_url, interval, stop_event):
    while not stop_event.is_set():
        try:
            result = http_get(f"{peer_url}/replication")
            node.import_messages(result.get("messages"))
            node.peer_reachable = True
        except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as error:
            node.peer_reachable = False
            print(f"peer sync with {peer_url} failed: {error}", flush=True)
        stop_event.wait(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--peer", "--sibling", dest="peer", default="",
                        help="peer backend hostname or hostname:port for log catch-up")
    parser.add_argument("--data", default="mailbox.log", help="append-only message log file")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--sync-interval", type=float, default=1.0,
                        help="seconds between peer catch-up attempts")
    args = parser.parse_args()
    if args.port < 1 or args.port > 65535:
        parser.error("--port must be between 1 and 65535")
    if args.sync_interval <= 0:
        parser.error("--sync-interval must be positive")

    node = StorageNode(args.data)
    peer_url = peer_address(args.peer, args.port)
    RequestHandler.node = node
    RequestHandler.peer_url = peer_url
    server = ThreadingHTTPServer(("0.0.0.0", args.port), RequestHandler)
    server.daemon_threads = True
    stop_event = threading.Event()
    sync_thread = None
    if peer_url:
        sync_thread = threading.Thread(
            target=sync_peer,
            args=(node, peer_url, args.sync_interval, stop_event),
            daemon=True,
        )
        sync_thread.start()

    print(
        f"backend listening on port {args.port}; "
        f"{len(node.all_messages())} messages loaded from {args.data}; "
        f"peer={peer_url or 'none'}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.server_close()
        if sync_thread is not None:
            sync_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()
