import argparse
import hashlib
import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import urllib.request, urllib.error
from routing import SITES, check_site, forward_request, get_home_site, get_replica_site, get_replica_peers

STORAGE_TIMEOUT = 1.0
MAX_BODY_SIZE = 1024 * 1024
USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")

def log(message):
    print(datetime.now().strftime("%H:%M:%S.%f")[:-3], message, flush=True)

def sites_digest():
    return hashlib.sha256(",".join(sorted(SITES)).encode("utf-8")).hexdigest()[:8]

def storage_call(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=STORAGE_TIMEOUT) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read())
        except ValueError:
            return error.code, {"error": "unexpected reply from storage node"}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None

def parse_mailbox_path(path):
    parts = path.split("/")
    if len(parts) not in (3, 4) or parts[1] != "mailbox":
        return None
    user = urllib.parse.unquote(parts[2])
    message_id = urllib.parse.unquote(parts[3]) if len(parts) == 4 else None
    if not USER_RE.fullmatch(user) or (message_id is not None and not message_id):
        return None
    return user, message_id

def storage_path(user, message_id=None):
    path = "/mailbox/" + urllib.parse.quote(user, safe="")
    if message_id is not None:
        path += "/" + urllib.parse.quote(message_id, safe="")
    return path

class FrontendHandler(BaseHTTPRequestHandler):
    def parse_request(self):
        self.started = time.time()
        self.info = {}
        self.route = []
        return super().parse_request()

    def log_message(self, *args):
        pass

    def step(self, text):
        self.route.append(text)

    def send_json(self, status, data):
        payload = json.dumps(data).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()

        self.wfile.write(payload)

        details = "".join(f" {key}={value}" for key, value in self.info.items())
        route = " > ".join(self.route)
        log(
            f"{self.server.site} {self.command} {self.path} -> {status} "
            f"{(time.time() - self.started) * 1000:.1f}ms{details} route={route}"
        )

    def read_body(self):
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            raise ValueError("a valid Content-Length header is required")
        if length < 0 or length > MAX_BODY_SIZE:
            raise ValueError(f"request body must be at most {MAX_BODY_SIZE} bytes")
        try:
            body = json.loads(self.rfile.read(length))
        except ValueError:
            raise ValueError("request body must be valid JSON")
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

    def storage_nodes(self):
        return [("storage2", self.server.storage2), ("storage3", self.server.storage3)]

    def storage_unavailable(self):
        return 503, {
            "error": "storage nodes unavailable",
            "site": self.server.site,
            "unreachable": [name for name, url in self.storage_nodes()],
        }

    def write_storage(self, method, path, body=None):
        nodes = self.storage_nodes()
        with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
            results = list(pool.map(lambda node: storage_call(node[1] + path, method, body), nodes))
        reachable = [result for result in results if result is not None]
        for (name, url), result in zip(nodes, results):
            if result is None:
                self.step(f"{name} unreachable")
        for result in reachable:
            if result[0] == 200:
                return result
        return reachable[0] if reachable else self.storage_unavailable()

    def local_read(self, user, message_id):
        result = None
        for name, url in self.storage_nodes():
            result = storage_call(url + storage_path(user, message_id))
            if result is not None:
                break
            self.step(f"{name} unreachable")
        if result is None:
            return self.storage_unavailable()
        status, data = result
        if status != 200:
            return status, data
        if message_id is not None:
            return 200, {key: data.get(key) for key in ("id", "from", "to", "subject", "body", "read")}
        summaries = [
            {
                "id": message.get("id"),
                "from": message.get("from"),
                "subject": message.get("subject"),
                "read": message.get("read")
            }
            for message in data.get("messages")
        ]
        return 200, {
            "user": user,
            "site": self.server.site,
            "messages": summaries
        }

    def do_GET(self):
        print("GET request received:", self.path)
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/status":
            if "brief" in query:
                return self.send_json(200, {
                    "site": self.server.site,
                    "status": "running"
                })
            me = self.server.site
            others = [site for site in SITES if site != me]
            nodes = self.storage_nodes()
            with ThreadPoolExecutor(max_workers=len(others) + len(nodes)) as pool:
                site_jobs = {site: pool.submit(check_site, site) for site in others}
                node_jobs = {name: pool.submit(storage_call, url + "/status") for name, url in nodes}
                sites = {me: {"reachable": True}}
                sites.update({site: {"reachable": job.result()} for site, job in site_jobs.items()})
                storage = {}
                for name, url in nodes:
                    result = node_jobs[name].result()
                    up = result is not None and result[0] == 200
                    storage[name] = {"url": url, "reachable": up, "status": result[1] if up else None}
            return self.send_json(200, {
                "site": me,
                "status": "running",
                "sites": sites,
                "sites_digest": sites_digest(),
                "storage": storage,
                "replica_of": self.server.replica_state
            })

        if parsed.path == "/internal/replication":
            records = []
            reachable = False
            for name, url in self.storage_nodes():
                result = storage_call(url + "/replication")
                if result is not None and result[0] == 200:
                    reachable = True
                    records += [
                        record for record in result[1]["messages"]
                        if {get_home_site(record["to"]), get_replica_site(record["to"])}
                        == {self.server.site, query.get("for", [""])[0]}
                    ]
            if not reachable:
                return self.send_json(*self.storage_unavailable())
            return self.send_json(200, {"messages": records})

        mailbox = parse_mailbox_path(parsed.path)

        if mailbox is None:
            return self.send_json(404, {
                "error": "unknown path"
            })
        
        user, message_id = mailbox

        #find the site that stores the users mailbox 
        home_site = get_home_site(user)
        replica_site = get_replica_site(user)
        self.info.update(user=user, home=home_site, replica=replica_site)

        if "replica" not in query:
            if home_site == self.server.site:
                self.step("local")
                status, response = self.local_read(user, message_id)
                if status != 503:
                    return self.send_json(status, response)
            else:
                #send request to the correct site if it isnt ours 
                self.step(f"forward to {home_site}")
                status, response = forward_request(
                    home_site, "GET", self.path
                )
                if status != 503:
                    return self.send_json(status, response)
                self.step(f"{home_site} unreachable")

            if replica_site != self.server.site:
                if replica_site != home_site:
                    self.step(f"forward to replica {replica_site}")
                    status, response = forward_request(
                        replica_site, "GET", parsed.path + "?replica=1"
                    )
                    if status != 503:
                        return self.send_json(status, response)
                    self.step(f"{replica_site} unreachable")
                return self.send_json(503, {
                    "error": "mailbox unavailable",
                    "unreachable": sorted({home_site, replica_site})
                })

        self.step("serve from local replica")
        status, response = self.local_read(user, message_id)
        if status == 200:
            response["site"] = home_site
            response["stale"] = True
            response["note"] = f"home site {home_site} is unreachable; this reply is from the replica and may be out of date"
        return self.send_json(status, response)

    def do_POST(self):
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path != "/mail":
            return self.send_json(404, {"error": "unknown path"})
        try:
            body = self.read_body()
            for field in ("from", "to", "subject", "body"):
                if not isinstance(body.get(field), str):
                    raise ValueError("from, to, subject and body must be strings")
            if not USER_RE.fullmatch(body["to"]):
                raise ValueError("to must be a valid user name")
        except ValueError as error:
            return self.send_json(400, {"error": str(error)})

        home_site = get_home_site(body["to"])
        replica_site = get_replica_site(body["to"])
        self.info.update(user=body["to"], home=home_site, replica=replica_site)
        if home_site != self.server.site and "replica" not in query:
            self.step(f"forward to {home_site}")
            status, response = forward_request(home_site, "POST", "/mail", body)
            if status != 503 or replica_site == home_site:
                return self.send_json(status, response)
            self.step(f"{home_site} unreachable")
            if replica_site != self.server.site:
                self.step(f"forward to replica {replica_site}")
                status, response = forward_request(replica_site, "POST", "/mail?replica=1", body)
                return self.send_json(status, response)

        self.step("local store" if home_site == self.server.site else "local store at replica")
        message = {
            "id": f"{self.server.site}-{uuid.uuid4().hex}",
            "from": body["from"],
            "to": body["to"],
            "subject": body["subject"],
            "body": body["body"],
        }
        status, response = self.write_storage("POST", "/mail", message)
        if status != 200:
            return self.send_json(status, response)
        reply = {"id": message["id"], "to": message["to"], "site": home_site}
        if home_site != self.server.site:
            reply["stored_at_replica"] = self.server.site
            reply["note"] = f"home site {home_site} is unreachable; stored at the replica and copied to the home site when it returns"
        return self.send_json(200, reply)

    def do_PUT(self):
        self.change_message("PUT")

    def do_DELETE(self):
        self.change_message("DELETE")

    def change_message(self, method):
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        mailbox = parse_mailbox_path(parsed.path)
        if mailbox is None or mailbox[1] is None:
            return self.send_json(404, {"error": "unknown path"})
        user, message_id = mailbox

        body = None
        if method == "PUT":
            try:
                body = self.read_body()
                if not isinstance(body.get("read"), bool):
                    raise ValueError('body must contain "read": true or "read": false')
            except ValueError as error:
                return self.send_json(400, {"error": str(error)})

        home_site = get_home_site(user)
        replica_site = get_replica_site(user)
        self.info.update(user=user, home=home_site, replica=replica_site)
        if home_site != self.server.site and "replica" not in query:
            self.step(f"forward to {home_site}")
            status, response = forward_request(home_site, method, parsed.path, body)
            if status != 503 or method != "PUT" or replica_site == home_site:
                return self.send_json(status, response)
            self.step(f"{home_site} unreachable")
            if replica_site != self.server.site:
                self.step(f"forward to replica {replica_site}")
                status, response = forward_request(replica_site, method, parsed.path + "?replica=1", body)
                return self.send_json(status, response)

        at_replica = home_site != self.server.site
        self.step("local store at replica" if at_replica else "local store")
        if method == "PUT":
            body = {"read": body["read"], "site": self.server.site}
        status, response = self.write_storage(method, storage_path(user, message_id), body)
        if status == 200 and at_replica:
            self.info["stored_at_replica"] = self.server.site
            if method == "PUT":
                response = dict(response)
                response["stored_at_replica"] = self.server.site
                response["note"] = (
                    f"home site {home_site} is unreachable; read change stored at the replica "
                    f"and copied to the home site when it returns"
                )
        return self.send_json(status, response)

def replicate_loop(server, interval):
    while True:
        for site in get_replica_peers(server.site):
            ok = False
            imported = 0
            started = time.time()
            status, response = forward_request(site, "GET", f"/internal/replication?for={server.site}")
            if status == 200:
                ok = True
                records = response["messages"]
                for start in range(0, len(records), 100):
                    batch = {"messages": records[start:start + 100]}
                    results = [
                        storage_call(url + "/replication", "POST", batch)
                        for url in (server.storage2, server.storage3)
                    ]
                    ok = ok and any(result is not None and result[0] == 200 for result in results)
                    imported += max(
                        (result[1].get("imported", 0) for result in results
                         if result is not None and result[0] == 200),
                        default=0
                    )
            state = server.replica_state.setdefault(site, {"last_success": None})
            if ok != state.get("ok"):
                down = f" (out of contact for {time.time() - state['changed']:.1f}s)" if "changed" in state and ok else ""
                log(f"{server.site} sync with {site}: {'in sync' if ok else f'cannot reach {site}'}{down}")
                state["changed"] = time.time()
            state["ok"] = ok
            if ok:
                state["last_success"] = time.time()
            if imported:
                log(f"{server.site} sync with {site}: imported {imported} changes in {(time.time() - started) * 1000:.0f}ms")
        time.sleep(interval)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--port", type=int, default=8000)
    # each group member must use thier own site number(site-1, site-2, site-3, site-4)
    parser.add_argument("--site", default="site-4")
    parser.add_argument("--sites", default="sites.json")
    parser.add_argument(
        "--storage2",
        default="http://127.0.0.1:8001"
    )
    parser.add_argument(
        "--storage3",
        default="http://127.0.0.1:8002"
    )
    parser.add_argument("--replica-interval", type=float, default=2.0)

    args = parser.parse_args()

    if os.path.exists(args.sites):
        with open(args.sites, encoding="utf-8") as sites_file:
            SITES.clear()
            SITES.update(json.load(sites_file))

    server = ThreadingHTTPServer(
        ("0.0.0.0", args.port),
        FrontendHandler
    )
    server.storage2 = args.storage2
    server.storage3 = args.storage3
    server.site = args.site
    server.replica_state = {}
    threading.Thread(
        target=replicate_loop, args=(server, args.replica_interval), daemon=True
    ).start()

    print(f"Front end running on port {args.port}")
    log(f"{args.site} sites (sorted)={sorted(SITES)} digest={sites_digest()} "
        f"storage2={args.storage2} storage3={args.storage3}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.server_close()


if __name__ == "__main__":
    main()
