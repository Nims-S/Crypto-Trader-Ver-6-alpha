import requests
import os

CAFFEINE_URL = os.getenv("CAFFEINE_URL")
CAFFEINE_TOKEN = os.getenv("CAFFEINE_TOKEN")


def push_to_caffeine(data):
    if not CAFFEINE_URL or not CAFFEINE_TOKEN:
        return  # silently skip if not configured

    try:
        response = requests.post(
            caffeine_url,
            json=data,
            headers=headers,
            timeout=3
        )
        if response.status_code >= 400:
            print(
                f"[CAFFEINE PUSH ERROR] HTTP {response.status_code} from {caffeine_url}: {response.text[:200]}",
                flush=True
            )
            return False
        return True
    except Exception as e:
        print(f"[CAFFEINE PUSH ERROR] {e}", flush=True)
