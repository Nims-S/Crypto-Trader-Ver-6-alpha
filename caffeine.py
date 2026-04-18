import requests
import os

CAFFEINE_URL = os.getenv("CAFFEINE_URL")

if not CAFFEINE_URL:
    raise RuntimeError("CAFFEINE_URL must be set")
CAFFEINE_TOKEN = os.getenv("CAFFEINE_TOKEN")


def push_to_caffeine(data):
    if not CAFFEINE_TOKEN:
        return  # silently skip if not configured

    try:
        requests.post(
            CAFFEINE_URL,
            json=data,
            headers={"Authorization": f"Bearer {CAFFEINE_TOKEN}"},
            timeout=3
        )
    except Exception as e:
        print(f"[CAFFEINE PUSH ERROR] {e}", flush=True)