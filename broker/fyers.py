"""
broker/fyers.py  —  Fyers API Connection Layer
═══════════════════════════════════════════════════════════════════════════════
  Handles:
    • OAuth2 login flow  (browser → redirect → auth code → access token)
    • Token persistence  (saved to .fyers_token, reloaded on next run)
    • Single shared fyers client instance used everywhere
    • Order placement helper (wraps fyers.place_order)

  Usage
  ─────
    from broker.fyers import get_client, place_order

    # First time setup (run once per day)
    client = get_client()

    # Place order
    place_order(client, symbol="NSE:NIFTY50-INDEX", qty=1,
                side="BUY", order_type="MARKET")
═══════════════════════════════════════════════════════════════════════════════
"""

import json
import time
import webbrowser
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import config
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Lazy imports (fyers_apiv3 only needed if actually connecting) ────────────
_fyers_client = None


# ═══════════════════════════════════════════════════════════════════════════
#  TOKEN MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def _save_token(access_token: str):
    """Persist access token to disk so we don't re-login every run."""
    config.TOKEN_FILE.write_text(
        json.dumps({"access_token": access_token, "saved_at": time.time()})
    )
    logger.debug("Access token saved → %s", config.TOKEN_FILE)


def _load_token() -> str | None:
    """
    Load token from disk.
    Returns None if file missing or token is older than 23 hours
    (Fyers tokens expire daily).
    """
    if not config.TOKEN_FILE.exists():
        return None
    try:
        data = json.loads(config.TOKEN_FILE.read_text())
        age_hours = (time.time() - data.get("saved_at", 0)) / 3600
        if age_hours > 23:
            logger.info("Saved token is %.1f hours old — will refresh", age_hours)
            return None
        return data.get("access_token")
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  LOGIN FLOW
# ═══════════════════════════════════════════════════════════════════════════

def login() -> str:
    """
    Full Fyers OAuth2 login flow.

    Steps
    ─────
    1.  Build auth URL and open it in the browser
    2.  User logs in on Fyers website
    3.  Browser redirects to REDIRECT_URI with ?auth_code=XXXX
    4.  User pastes the full redirect URL here
    5.  Exchange auth_code for access_token
    6.  Save token to disk

    Returns the access_token string.
    """
    try:
        from fyers_apiv3 import fyersModel
    except ImportError:
        raise ImportError(
            "fyers-apiv3 not installed.\n"
            "  Fix: pip install fyers-apiv3"
        )

    if not config.FYERS_CLIENT_ID or not config.FYERS_SECRET_KEY:
        raise EnvironmentError(
            "\n  ✗  Fyers credentials missing in .env\n"
            "  Add FYERS_CLIENT_ID and FYERS_SECRET_KEY\n"
            "  Get them at: https://myapi.fyers.in/dashboard\n"
        )

    # Step 1 — Generate auth URL
    session = fyersModel.SessionModel(
        client_id     = config.FYERS_CLIENT_ID,
        secret_key    = config.FYERS_SECRET_KEY,
        redirect_uri  = config.FYERS_REDIRECT_URI,
        response_type = "code",
        grant_type    = "authorization_code",
    )
    auth_url = session.generate_authcode()

    # Step 2 — Open browser
    print("\n" + "═" * 65)
    print("  FYERS LOGIN")
    print("═" * 65)
    print(f"\n  Opening browser → {auth_url}\n")
    print("  If browser doesn't open, paste the URL manually.")
    print("═" * 65)
    webbrowser.open(auth_url)

    # Step 3 — User pastes the redirect URL
    print("\n  After login, Fyers will redirect you to a URL like:")
    print("  https://trade.fyers.in/...?auth_code=XXXXXXXXXX&state=...")
    print("\n  Paste that FULL URL here:")
    redirect_url = input("  → ").strip()

    # Step 4 — Extract auth_code from URL
    try:
        parsed    = urlparse(redirect_url)
        auth_code = parse_qs(parsed.query)["auth_code"][0]
    except (KeyError, IndexError):
        raise ValueError(
            "Could not extract auth_code from URL.\n"
            "Make sure you pasted the full redirect URL."
        )

    # Step 5 — Exchange for access token
    session.set_token(auth_code)
    response = session.generate_token()

    if response.get("s") != "ok":
        raise ConnectionError(
            f"Token generation failed: {response.get('message', response)}"
        )

    access_token = response["access_token"]

    # Step 6 — Save
    _save_token(access_token)
    logger.info("✅  Login successful — token saved")
    return access_token


# ═══════════════════════════════════════════════════════════════════════════
#  CLIENT FACTORY
# ═══════════════════════════════════════════════════════════════════════════

def get_client(force_login: bool = False):
    """
    Returns a ready-to-use fyers client.

    Token priority
    ──────────────
    1.  FYERS_ACCESS_TOKEN in .env   (manual override)
    2.  .fyers_token file            (auto-saved from last login)
    3.  Interactive login flow       (opens browser)

    Parameters
    ----------
    force_login : bool  —  skip saved token, always do fresh login
    """
    global _fyers_client

    if _fyers_client is not None and not force_login:
        return _fyers_client

    try:
        from fyers_apiv3 import fyersModel
    except ImportError:
        raise ImportError("pip install fyers-apiv3")

    # Determine access token
    if force_login:
        access_token = login()
    elif config.FYERS_ACCESS_TOKEN:
        access_token = config.FYERS_ACCESS_TOKEN
        logger.info("Using FYERS_ACCESS_TOKEN from .env")
    else:
        access_token = _load_token()
        if access_token is None:
            logger.info("No valid saved token — starting login flow …")
            access_token = login()
        else:
            logger.info("Loaded saved access token (< 23 h old)")

    # Build client
    _fyers_client = fyersModel.FyersModel(
        client_id    = config.FYERS_CLIENT_ID,
        is_async     = False,
        token        = access_token,
        log_path     = str(config.LOG_DIR),
    )

    # Quick validation ping
    try:
        profile = _fyers_client.get_profile()
        if profile.get("s") == "ok":
            name = profile["data"]["name"]
            logger.info("Connected as: %s", name)
        else:
            logger.warning("Profile fetch returned: %s", profile)
    except Exception as e:
        logger.warning("Profile check failed (token may still be valid): %s", e)

    return _fyers_client


# ═══════════════════════════════════════════════════════════════════════════
#  ORDER HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def place_order(
    client,
    symbol      : str,
    qty         : int,
    side        : str,         # "BUY" | "SELL"
    order_type  : str = "MARKET",
    limit_price : float = 0,
    stop_price  : float = 0,
    product_type: str = "INTRADAY",
    tag         : str = "fvg_bot",
    dry_run     : bool = True,
) -> dict:
    """
    Place a single order via Fyers API.

    Parameters
    ----------
    side        : "BUY" or "SELL"
    order_type  : "MARKET" | "LIMIT" | "STOP" | "STOP_LIMIT"
    dry_run     : If True, log the order but don't actually send it

    Returns Fyers API response dict (or mock dict in dry_run mode).
    """

    # Map to Fyers constants
    _side_map  = {"BUY": 1, "SELL": -1}
    _type_map  = {"MARKET": 2, "LIMIT": 1, "STOP": 3, "STOP_LIMIT": 4}
    _prod_map  = {"INTRADAY": "INTRADAY", "CNC": "CNC", "MARGIN": "MARGIN"}

    order_data = {
        "symbol"      : symbol,
        "qty"         : qty,
        "type"        : _type_map.get(order_type.upper(), 2),
        "side"        : _side_map.get(side.upper(), 1),
        "productType" : _prod_map.get(product_type.upper(), "INTRADAY"),
        "limitPrice"  : limit_price,
        "stopPrice"   : stop_price,
        "validity"    : "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "stopLoss"    : 0,
        "takeProfit"  : 0,
        "orderTag"    : tag,
    }

    if dry_run:
        logger.info(
            "🧪  DRY RUN — ORDER NOT SENT\n"
            "    Symbol: %s | Side: %s | Qty: %d | Type: %s",
            symbol, side, qty, order_type,
        )
        return {"s": "ok", "id": "DRY_RUN", "order_data": order_data}

    try:
        response = client.place_order(data=order_data)
        if response.get("s") == "ok":
            logger.info(
                "✅  ORDER PLACED | %s %s %d @ %s | id=%s",
                side, symbol, qty, order_type, response.get("id")
            )
        else:
            logger.error("❌  ORDER FAILED | %s", response)
        return response

    except Exception as e:
        logger.error("❌  ORDER EXCEPTION | %s | %s", symbol, e)
        return {"s": "error", "message": str(e)}


def cancel_all_orders(client, dry_run: bool = True) -> list:
    """Cancel all open orders (used for EOD cleanup)."""
    if dry_run:
        logger.info("🧪  DRY RUN — would cancel all open orders")
        return []
    try:
        orders = client.orderbook()
        cancelled = []
        for order in orders.get("orderBook", []):
            if order["status"] == 6:   # 6 = open
                res = client.cancel_order({"id": order["id"]})
                cancelled.append(res)
                logger.info("Cancelled order %s", order["id"])
        return cancelled
    except Exception as e:
        logger.error("Error cancelling orders: %s", e)
        return []


def get_ltp(client, symbol: str) -> float | None:
    """Get Last Traded Price for a symbol."""
    try:
        resp = client.quotes(data={"symbols": symbol})
        if resp.get("s") == "ok":
            return resp["d"][0]["v"]["lp"]
    except Exception as e:
        logger.warning("LTP fetch failed for %s: %s", symbol, e)
    return None
