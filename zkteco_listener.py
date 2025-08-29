"""Continuously monitor ZKTeco devices and print scanned user IDs.
This script polls each device (port 4370) using the `zk` Python package.
It keeps a small in-memory set of already-seen attendance records and prints
only new user IDs as they appear.
Requirements:
  pip install pyzk
Run:
  python zkteco_listener.py
"""

import threading
import time
import logging
import socket
import os
import json
import urllib.request
from queue import Queue
from typing import Set, Tuple
from datetime import datetime, date

from device import fetch_initial_devices

logger = logging.getLogger("zkteco_listener")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


try:
    import zk as zk_module
    from zk import ZK, const
except Exception as e:
    logger.error("Missing dependency 'zk'. Install with: pip install zk - or ensure correct package provides ZK class: %s", e)
    raise

# Optional: prefer requests if available for simpler HTTP calls
try:
    import requests
except Exception:
    requests = None

# Webhook queue + worker to reuse connections and avoid ephemeral port exhaustion
_WEBHOOK_URL = os.environ.get(
    "N8N_WEBHOOK_URL",
    "https://n8n.pfpintranet.com/webhook/c70ded1f-e6e4-4cb2-8038-4407e733a546"
)
_webhook_q: Queue = Queue()
_WEBHOOK_WORKERS = int(os.environ.get("N8N_WEBHOOK_WORKERS", "3"))
_WEBHOOK_TIMEOUT = float(os.environ.get("N8N_WEBHOOK_TIMEOUT", "5"))

if requests:
    _session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=1)
    _session.mount("https://", adapter)
else:
    _session = None


def _webhook_worker():
    while True:
        ip, name, userid, ts = _webhook_q.get()
        # Get local machine IP
        local_ip = socket.gethostbyname(socket.gethostname())
        payload = {"device_ip": ip, "local_ip": local_ip, "device": name, "userid": userid, "timestamp": ts}
        headers = {"Content-Type": "application/json"}
        # simple retry with backoff
        attempt = 0
        max_attempts = 3
        backoff = 0.5
        while attempt < max_attempts:
            try:
                if _session:
                    resp = _session.post(_WEBHOOK_URL, json=payload, headers=headers, timeout=_WEBHOOK_TIMEOUT)
                    # logger.info("Webhook %s -> status=%s", _WEBHOOK_URL, getattr(resp, "status_code", None))
                else:
                    data = json.dumps(payload).encode("utf-8")
                    req = urllib.request.Request(_WEBHOOK_URL, data=data, headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=_WEBHOOK_TIMEOUT) as r:
                        status = getattr(r, "status", None)
                        # logger.info("Webhook %s -> status=%s", _WEBHOOK_URL, status)
                break
            except Exception as e:
                attempt += 1
                logger.warning("Failed to send webhook to %s for %s (attempt %d/%d): %s", _WEBHOOK_URL, ip, attempt, max_attempts, e)
                time.sleep(backoff)
                backoff *= 2
        _webhook_q.task_done()


# start workers
for _ in range(_WEBHOOK_WORKERS):
    t = threading.Thread(target=_webhook_worker, daemon=True)
    t.start()


def enqueue_webhook(ip: str, name: str, userid: str, ts: str) -> None:
    try:
        _webhook_q.put_nowait((ip, name, userid, ts))
    except Exception as e:
        logger.warning("Failed to enqueue webhook for %s: %s", ip, e)


def is_timestamp_today(ts: str) -> bool:
    """Return True if the provided timestamp string represents today's date.

    This function is defensive: it tries several common formats and epoch
    representations. If it cannot confidently parse the timestamp, it
    returns False so we avoid sending outdated/unknown-date events.
    """
    if not ts:
        return False
    s = str(ts).strip()
    # epoch seconds or milliseconds
    try:
        if s.isdigit():
            val = int(s)
            # treat >10 digits as milliseconds
            if len(s) > 10:
                val = val / 1000
            dt = datetime.fromtimestamp(val)
            return dt.date() == date.today()
    except Exception:
        pass

    # common datetime formats
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.date() == date.today()
        except Exception:
            continue

    # try a relaxed ISO-like parse (strip fractional seconds/timezone)
    try:
        cleaned = s.split(".")[0].replace("T", " ")
        cleaned = cleaned.split("+")[0].split("Z")[0].strip()
        dt = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
        return dt.date() == date.today()
    except Exception:
        pass

    # substring checks for common date patterns
    if date.today().isoformat() in s:
        return True
    if date.today().strftime("%d/%m/%Y") in s:
        return True

    logger.debug("Could not parse timestamp '%s' to determine if it's today", ts)
    return False


def monitor_device(ip: str, name: str, poll_interval: float = 5.0):
    """Connect to a device and poll attendance logs, printing new userids."""
    seen: Set[Tuple] = set()
    while True:
        zk = None
        conn = None
        try:
            zk = ZK(ip, port=4370, timeout=5)
            conn = zk.connect()
            # logger.info("Connected to device %s (%s)", ip, name)
            # Optional: Sync time or disable device while reading
            while True:
                try:
                    records = conn.get_attendance()
                except Exception as e:
                    # Log specific network/timeouts from the ZK library or socket
                    try:
                        if hasattr(zk_module, 'exception') and isinstance(e, zk_module.exception.ZKNetworkError):
                            logger.warning("Device %s (%s) network error during get_attendance: %s", ip, name, e)
                        elif isinstance(e, (socket.timeout, TimeoutError)):
                            logger.warning("Timeout reading attendance from %s (%s): %s", ip, name, e)
                        else:
                            logger.debug("get_attendance failed for %s (%s): %s", ip, name, e)
                    except Exception:
                        logger.debug("get_attendance exception for %s (%s): %s", ip, name, e)
                    break

                # Some drivers return a list of tuples/rows, others a single
                # Attendance object. Normalize into an iterable of record-like
                # values and extract userid/timestamp defensively.
                if records is None:
                    time.sleep(poll_interval)
                    continue

                # If a single object (non-iterable), wrap it
                try:
                    iter(records)
                    rec_iter = records
                except TypeError:
                    rec_iter = [records]

                for rec in rec_iter:
                    # Normalize record into a tuple-like sequence (user, ts, ...)
                    def record_to_tuple(r):
                        # If already sequence-like, convert directly
                        if isinstance(r, (list, tuple)):
                            return tuple(r)

                        # Try common attribute names on Attendance-like objects
                        attrs_user = ("user_id", "userid", "user", "uid", "id", "pin", "enroll_number", "badge", "card", "cardid")
                        attrs_time = ("timestamp", "time", "check_time", "datetime", "date_time")
                        u = None
                        t = None

                        for a in attrs_user:
                            u = getattr(r, a, None)
                            if u is not None:
                                break
                        for a in attrs_time:
                            t = getattr(r, a, None)
                            if t is not None:
                                break

                        # If object has a dict-like representation, try that for missing values
                        d = {}
                        if hasattr(r, "__dict__"):
                            d = getattr(r, "__dict__", {}) or {}
                            if u is None:
                                for a in attrs_user:
                                    if a in d and d[a] is not None:
                                        u = d[a]
                                        break
                            if t is None:
                                for a in attrs_time:
                                    if a in d and d[a] is not None:
                                        t = d[a]
                                        break

                        # If we found a numeric userid, try to locate a string representation
                        # that preserves leading zeros by inspecting dict values and repr
                        try:
                            if u is not None:
                                # normalize bytes to str
                                if isinstance(u, (bytes, bytearray)):
                                    u = u.decode(errors="ignore")

                                # If it's an int (or numeric string), look for a matching
                                # string value elsewhere that contains leading zeros.
                                numeric_val = None
                                if isinstance(u, int):
                                    numeric_val = u
                                elif isinstance(u, str) and u.isdigit():
                                    # keep string but also consider it numeric
                                    numeric_val = int(u)

                                if numeric_val is not None:
                                    # search d values for a zero-padded string equal to numeric_val
                                    for v in d.values():
                                        if isinstance(v, (bytes, bytearray)):
                                            try:
                                                v = v.decode()
                                            except Exception:
                                                continue
                                        if isinstance(v, str) and v.isdigit():
                                            try:
                                                if int(v) == numeric_val and (v.lstrip('0') != v or v == '0'):
                                                    u = v
                                                    break
                                            except Exception:
                                                continue

                                    # if not found in dict, try to scan the string representation
                                    if isinstance(u, (int,)) or (isinstance(u, str) and u.isdigit() and u == str(numeric_val)):
                                        s = str(r)
                                        # find digit substrings with leading zeros
                                        import re
                                        for m in re.finditer(r"0+\d+", s):
                                            candidate = m.group(0)
                                            try:
                                                if int(candidate) == numeric_val:
                                                    u = candidate
                                                    break
                                            except Exception:
                                                continue
                        except Exception:
                            # be defensive: if anything goes wrong, fall back to original u
                            pass

                        # Fallback: stringify the object if nothing useful found
                        if u is None and t is None:
                            return (str(r),)

                        return (u, t) if t is not None else (u,)

                    try:
                        tup = record_to_tuple(rec)
                    except Exception:
                        # logger.debug("Failed to normalize record %r", rec)
                        tup = (str(rec),)

                    key = tuple(map(str, tup))
                    if key not in seen:
                        seen.add(key)

                        # Preserve leading zeros for user ids when possible.
                        # If the normalized value is an int, try to find a
                        # zero-padded string in the original record's dict
                        # or string representation; otherwise fall back to str().
                        def _format_userid(u, original_rec):
                            try:
                                # normalize bytes -> str
                                if isinstance(u, (bytes, bytearray)):
                                    u = u.decode(errors="ignore")

                                # Helper to search for a zero-padded string matching numeric_val
                                def _search_zero_padded(numeric_val):
                                    d = {}
                                    if hasattr(original_rec, "__dict__"):
                                        d = getattr(original_rec, "__dict__", {}) or {}
                                    for v in d.values():
                                        if isinstance(v, (bytes, bytearray)):
                                            try:
                                                v = v.decode()
                                            except Exception:
                                                continue
                                        if isinstance(v, str) and v.isdigit():
                                            try:
                                                if int(v) == numeric_val and (v.lstrip("0") != v or v == "0"):
                                                    return v
                                            except Exception:
                                                continue
                                    # Fallback: scan string repr for zero-padded digit substrings
                                    import re
                                    s = str(original_rec)
                                    for m in re.finditer(r"0+\d+", s):
                                        candidate = m.group(0)
                                        try:
                                            if int(candidate) == numeric_val:
                                                return candidate
                                        except Exception:
                                            continue
                                    return None

                                # if string
                                if isinstance(u, str):
                                    # numeric string -> try to find zero-padded variant
                                    if u.isdigit():
                                        try:
                                            numeric_val = int(u)
                                        except Exception:
                                            return u
                                        found = _search_zero_padded(numeric_val)
                                        return found if found is not None else u
                                    # non-numeric string -> return as-is
                                    return u

                                # if int -> search for zero-padded variant
                                if isinstance(u, int):
                                    found = _search_zero_padded(u)
                                    return found if found is not None else str(u)

                                # fallback for other types
                                return str(u) if u is not None else ""
                            except Exception:
                                return str(u) if u is not None else ""

                        userid = _format_userid(tup[0], rec) if len(tup) > 0 else ""
                        # If userid is shorter than 5 characters, pad with leading zeros
                        try:
                            if userid is not None:
                                s = str(userid)
                                if len(s) < 5:
                                    userid = s.zfill(5)
                                else:
                                    userid = s
                        except Exception:
                            userid = str(userid) if userid is not None else ""

                        ts = str(tup[1]) if len(tup) > 1 else ""
                        msg = f"{ip} [{name}] scanned user: {userid} at {ts}"
                        print(msg)
                        # Only enqueue webhook if timestamp is today
                        try:
                            if is_timestamp_today(ts):
                                enqueue_webhook(ip, name, userid, ts)
                            else:
                                logger.debug("Skipping webhook for %s: timestamp not today (%s)", ip, ts)
                        except Exception as e:
                            logger.debug("Failed to enqueue webhook for %s: %s", ip, e)
                time.sleep(poll_interval)
        except Exception as e:
            # Handle ZK network/timeouts more explicitly so logs are informative
            try:
                if isinstance(e, zk_module.exception.ZKNetworkError):
                    logger.warning("Network error connecting to %s (%s): %s", ip, name, e)
                elif isinstance(e, (socket.timeout, TimeoutError)):
                    logger.warning("Timeout connecting to %s (%s): %s", ip, name, e)
                else:
                    logger.exception("Connection/monitor error for %s (%s): %s", ip, name, e)
            except Exception:
                # If zk_module or its exception class isn't available, fall back
                if isinstance(e, (socket.timeout, TimeoutError)):
                    logger.warning("Timeout connecting to %s (%s): %s", ip, name, e)
                else:
                    logger.exception("Connection/monitor error for %s (%s)", ip, name)
        finally:
            try:
                if conn:
                    conn.disconnect()
            except Exception:
                pass
        # Wait before reconnecting
        time.sleep(3)


def main():
    devices = fetch_initial_devices()
    if not devices:
        logger.error("No devices found from device.fetch_initial_devices()")
        return
    threads = []
    for d in devices:
        t = threading.Thread(target=monitor_device, args=(d.ip, d.name), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.2)

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down listener")

if __name__ == "__main__":
    main()
