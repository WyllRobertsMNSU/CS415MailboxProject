import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request, urllib.error
from routing import get_home_site, forward_request 
def storage_get(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.loads(response.read())

class FrontendHandler(BaseHTTPRequestHandler):
    def send_json(self, status, data):
        payload = json.dumps(data).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()

        self.wfile.write(payload)

    def do_GET(self):
        print("GET request received:", self.path)

        if self.path == "/status":
            return self.send_json(200, {
                "site": self.server.site,
                "status": "running"
            })

        parts = self.path.split("/")
        
        if len(parts) != 3 or parts[1] != "mailbox" or not parts[2]:
            return self.send_json(404, {
                "error": "unknown path"
            })
        
        user = parts[2]

        #find the site that stores the users mailbox 
        home_site = get_home_site(user)
        #send request to the correct site if it isnt ours 
        if home_site != self.server.site:
            status, response = forward_request(
                home_site, "GET", self.path
            )
            return self.send_json(status, response)

        try:
            url = self.server.storage2 + self.path
            messages = storage_get(url)
        except urllib.error.URLError:
            return self.send_json(503, {
                "error": "storage node unavailable"
            })

        mailbox_messages = messages.get("messages")

        summaries = [
            {
                "id": message.get("id"),
                "from": message.get("from"),
                "subject": message.get("subject"),
                "read": message.get("read")
            }
            for message in mailbox_messages
        ]

        return self.send_json(200, {
            "user": user,
            "site": self.server.site,
            "messages": summaries
        })

        

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

    args = parser.parse_args()

    server = ThreadingHTTPServer(
        ("0.0.0.0", args.port),
        FrontendHandler
    )
    server.storage2 = args.storage2
    server.site = args.site

    print(f"Front end running on port {args.port}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.server_close()


if __name__ == "__main__":
    main()
