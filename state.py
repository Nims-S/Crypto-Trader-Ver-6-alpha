# state.py (NEW FILE)

from datetime import datetime

STATE = {
    "last_update": None,
    "assets": {}
}

def update_asset(symbol, regime, strategy, signal=None, position=None):
    STATE["assets"][symbol] = {
        "regime": regime,
        "strategy": strategy,
        "signal": signal,
        "position": position,
        "timestamp": datetime.utcnow().isoformat()
    }
    STATE["last_update"] = datetime.utcnow().isoformat()

def get_state():
    return STATE