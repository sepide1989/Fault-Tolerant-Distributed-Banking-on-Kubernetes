"""Cross-platform 2PC crash demonstration (uses requests).

Usage:
 python tests/crash_demo.py --request-id crash-demo-001
 # Gateway may crash; restart it via: docker compose up -d gateway
 python tests/crash_demo.py --check --request-id crash-demo-001
"""
import argparse
import json
import requests


def show(data):
    print(json.dumps(data, indent=2, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--request-id", default="crash-demo-001")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    url = args.url.rstrip("/")
    if args.check:
        try:
            response = requests.get(url + "/api/requests/" + args.request_id, timeout=5)
            print("HTTP status:", response.status_code)
            show(response.json())
            for account in ("A0300", "A1300"):
                show(requests.get(url + "/api/accounts/" + account, timeout=5).json())
        except requests.RequestException as exc:
            print("Cannot connect; start Gateway and let recovery run:", exc)
        return

    print("Crash Demo request_id =", args.request_id, flush=True)
    armed = requests.post(url + "/api/admin/arm-crash-after-commit", timeout=5)
    print("Fault armed:", armed.status_code, flush=True)
    show(armed.json())
    if armed.status_code != 200:
        return
    try:
        response = requests.post(url + "/api/transfer", json={
            "source": "A0300", "destination": "A1300", "amount": 100,
            "request_id": args.request_id,
        }, timeout=30)
        print("POST response:", response.status_code, flush=True)
        show(response.json())
        print("If it did not crash, check whether you reused an existing request_id.")
    except requests.RequestException as exc:
        print("Expected connection interruption after the injected Coordinator crash:", exc, flush=True)
    print("Next: docker compose up -d gateway", flush=True)
    print("Then: python tests/crash_demo.py --check --request-id " + args.request_id, flush=True)


if __name__ == "__main__":
    main()
