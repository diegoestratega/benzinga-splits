from curl_cffi import requests as curl_requests

r = curl_requests.get(
    "https://www.benzinga.com/calendars/stock-splits",
    impersonate="chrome124",
)

print(f"Status: {r.status_code}")
print(f"Bytes:  {len(r.text):,}")

if "Something went wrong" in r.text or r.status_code != 200:
    print("BLOCKED")
    with open("test_response.html", "w", encoding="utf-8") as f:
        f.write(r.text)
    print("Saved → test_response.html")
else:
    print("SUCCESS — page loaded cleanly")
    print(r.text[:500])