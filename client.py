"""Interactive banking CLI - run on your Windows/Linux host."""
import argparse
import json
import uuid
import requests


def call(base_url, method, path, body=None):
    try:
        response = requests.request(method, base_url + path, json=body, timeout=35)
        print(f"HTTP {response.status_code}")
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    except requests.RequestException as exc:
        print("The Gateway did not return a response:", exc)
        if body and body.get("request_id"):
            print("Do NOT blindly create another request. Query /api/requests/" + body["request_id"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    base = args.url.rstrip("/")
    while True:
        print("\n1) Balance  2) Deposit  3) Withdraw  4) Transfer  5) Lookup transfer  0) Exit")
        choice = input("Choose: ").strip()
        try:
            if choice == "0":
                break
            elif choice == "1":
                call(base, "GET", "/api/accounts/" + input("Account (A0500): ").strip())
            elif choice in ("2", "3"):
                account = input("Account: ").strip()
                amount = int(input("Amount (integer): "))
                req_id = uuid.uuid4().hex
                print("Request ID:", req_id)
                call(base, "POST", "/api/" + ("deposit" if choice == "2" else "withdraw"),
                     {"account": account, "amount": amount, "request_id": req_id})
            elif choice == "4":
                source = input("From: ").strip()
                destination = input("To: ").strip()
                amount = int(input("Amount (integer): "))
                req_id = uuid.uuid4().hex
                print("Request ID:", req_id)
                call(base, "POST", "/api/transfer",
                     {"source": source, "destination": destination,
                      "amount": amount, "request_id": req_id})
            elif choice == "5":
                call(base, "GET", "/api/requests/" + input("Request ID: ").strip())
        except ValueError:
            print("Please enter an integer for the amount.")


if __name__ == "__main__":
    main()
