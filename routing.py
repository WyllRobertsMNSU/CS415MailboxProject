
import hashlib
import json
import urllib.request
import urllib.error

SITES = {
    # TODO: Replace with actual group network addresses
    # Wyll Roberts
    "site-1": "http://10.100.1.13:8000",
    # David Swedin
    "site-2": "http://10.100.1.15:8000",
    # Blake Williams
    "site-3": "http://10.100.1.20:8000",
    # Logan Klein
    "site-4": "http://10.100.1.11:8000"
}


# Prevent requests from waiting too long
TIMEOUT = 2.0


def get_home_site(username):
    site_names = sorted(SITES)

    hash_value = hashlib.sha256(
        username.encode("utf-8")
    ).digest()

    number = int.from_bytes(hash_value, "big")

    index = number % len(site_names)

    return site_names[index]

def get_replica_site(username):
    site_names = sorted(SITES)

    home_site = get_home_site(username)

    home_index = site_names.index(home_site)

    replica_index = (home_index + 1) % len(site_names)

    return site_names[replica_index]
def forward_request(site, method, path, body=None):
    url = SITES[site] + path

    data = None
    if body is not None:
        # Convert Python dictionary to JSON bytes
        data = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method
    )

    try:
        with urllib.request.urlopen(
            request, timeout=TIMEOUT
        ) as response:
            return (
                response.status,
                json.loads(response.read())
            )

    except urllib.error.HTTPError as error:
        return (
            error.code,
            json.loads(error.read())
        )

    except (urllib.error.URLError, TimeoutError, OSError):
        return (
            503,
            {
                "error": "site unreachable",
                "site": site
            }
        )


def check_site(site):
    url = SITES[site] + "/status"

    try:
        with urllib.request.urlopen(
            url, timeout=TIMEOUT
        ) as response:
            return response.status == 200

    except (urllib.error.URLError, TimeoutError, OSError):
        return False


if __name__ == "__main__":
    users = ["alice", "bob", "charlie", "david"]

    print("Testing home and replica sites:")

    for user in users:
        print(
            user,
            "Home:", get_home_site(user),
            "Replica:", get_replica_site(user)
        )

    print("\nTesting site-3 connection:")

    status, response = forward_request(
        "site-3",
        "GET",
        "/mailbox/alice"
    )

    print("Status:", status)
    print("Response:", response)
    print("Site reachable:", check_site("site-3"))