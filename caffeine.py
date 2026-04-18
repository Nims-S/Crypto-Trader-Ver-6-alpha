import requests
import os

_MISSING_URL_WARNED = False


def push_to_caffeine(data):
    global _MISSING_URL_WARNED

    caffeine_url = (os.getenv("CAFFEINE_URL") or "").strip()
    caffeine_token = (os.getenv("CAFFEINE_TOKEN") or "").strip()

    if not caffeine_url:
        if not _MISSING_URL_WARNED:
            print("[CAFFEINE PUSH] Skipped: CAFFEINE_URL is not configured.", flush=True)
            _MISSING_URL_WARNED = True
        return False

    headers = {}
    if caffeine_token:
        headers["Authorization"] = f"Bearer {caffeine_token}"

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
        return False
