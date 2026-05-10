#!/usr/bin/env python3
"""
Energy Monitor — Unified Backend
  --mode terminal   : standalone terminal agent  (default)
  --mode server     : Flask REST API + WebSocket + serves dashboard

pip install eventlet flask flask-socketio pyserial requests

Usage (server mode):
  python3 energy_monitor.py --mode server
  Then open:  http://<raspberry-pi-ip>:5000

OVERCURRENT LOGIC:
  - Current > 12 A  → relay cuts immediately
  - Waits 30 seconds (countdown emitted to dashboard every second)
  - After 30 s:
      • Current < 12 A  → relay restores ✅
      • Current ≥ 12 A  → 30 s timer resets, relay stays OFF, tries again
"""

# ── eventlet MUST be monkey-patched before every other import ──────────────────
import eventlet
eventlet.monkey_patch()
# ──────────────────────────────────────────────────────────────────────────────

import argparse
import json
import os
import sys
import time
import threading
import serial
import requests
from datetime import datetime
from flask import Flask, jsonify, request, send_file
from flask_socketio import SocketIO

# ── Config ─────────────────────────────────────────────────────────────────────

SERIAL_PORT         = "/dev/ttyUSB0"
BAUD_RATE           = 115200
SERIAL_TIMEOUT      = 5

OLLAMA_URL          = "http://localhost:11434/api/generate"
OLLAMA_MODEL        = "llama3"

READINGS_PER_PROMPT = 5
ANALYSIS_INTERVAL   = 3

OVERCURRENT_THRESHOLD = 12.0          # Hard relay-cut limit: current > 12 A
OVERCURRENT_COOLDOWN  = 30            # Seconds relay stays OFF before retry

NORMAL_RANGES = {
    "voltage":   (210, 240),
    "current":   (0,   OVERCURRENT_THRESHOLD),
    "power":     (0,   3300),
    "frequency": (49,  51),
    "pf":        (0.7, 1.0),
}

CRITICAL_LIMITS = {
    "voltage":   (195, 250),
    "current":   (0,   OVERCURRENT_THRESHOLD),
    "power":     (0,   3300),
    "frequency": (47,  52),
}

# ── Shared state ───────────────────────────────────────────────────────────────

state_lock   = threading.Lock()
shared_state = {
    "latest":           None,
    "history":          [],
    "relay":            True,
    "relay_mode":       "auto",
    "alerts":           [],
    "last_analysis":    "",
    "connected":        False,
    "llm_busy":         False,
    "last_analysis_ts": 0,
    "relay_cut_reasons": [],
}

# ── Overcurrent state ──────────────────────────────────────────────────────────
#
#  State machine:
#
#    IDLE ──(current > 12)──► ACTIVE
#      ACTIVE: relay OFF, start 30 s cooldown timer
#      ACTIVE ──(30 s elapsed, current < 12)──► IDLE  (relay ON)
#      ACTIVE ──(30 s elapsed, current ≥ 12)──► ACTIVE (reset timer, relay stays OFF)
#
_oc_lock           = threading.Lock()
_oc_active         = False    # True while we are in an overcurrent event
_oc_cooldown_start = 0.0      # epoch when the current 30-s window started
_oc_attempt        = 0        # how many 30-s windows we've waited so far
_oc_last_llm       = 0.0      # epoch of last LLM call for this event

OVERCURRENT_LLM_INTERVAL = 5

app = Flask(__name__)
app.config["SECRET_KEY"] = "energymonitor2024"
io  = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

_port: serial.Serial | None = None

# ── Serial helpers ─────────────────────────────────────────────────────────────

def open_serial_blocking() -> serial.Serial:
    while True:
        try:
            p = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=SERIAL_TIMEOUT)
            print(f"[serial] Connected to {SERIAL_PORT} @ {BAUD_RATE} baud")
            return p
        except serial.SerialException as e:
            print(f"[serial] Cannot open {SERIAL_PORT}: {e}  — retrying in 3 s")
            time.sleep(3)


def open_serial_nonblocking() -> serial.Serial | None:
    try:
        p = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=SERIAL_TIMEOUT)
        with state_lock:
            shared_state["connected"] = True
        print(f"[serial] Connected to {SERIAL_PORT}")
        return p
    except serial.SerialException as e:
        print(f"[serial] {e}")
        with state_lock:
            shared_state["connected"] = False
        return None


def read_line(port: serial.Serial) -> dict | None:
    try:
        raw = port.readline().decode("utf-8", errors="replace").strip()
    except serial.SerialException as e:
        print(f"[serial] Read error: {e}")
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"[serial] Non-JSON line: {raw}")
        return None


def send_command(port_or_none, cmd: str):
    port = port_or_none if port_or_none else _port
    if port and port.is_open:
        try:
            port.write((cmd + "\n").encode("utf-8"))
            print(f"[serial] >> {cmd}")
        except serial.SerialException as e:
            print(f"[serial] Write error: {e}")

# ── Anomaly / protection ───────────────────────────────────────────────────────

def flag_anomalies(reading: dict) -> list:
    flags = []
    for field, (lo, hi) in NORMAL_RANGES.items():
        val = reading.get(field)
        if val is None:
            continue
        if val < lo:
            flags.append(f"{field} LOW ({val:.2f} < {lo})")
        elif val > hi:
            flags.append(f"{field} HIGH ({val:.2f} > {hi})")
    return flags


def is_critical(reading: dict) -> tuple:
    reasons = []
    for field, (lo, hi) in CRITICAL_LIMITS.items():
        val = reading.get(field)
        if val is None:
            continue
        if val < lo:
            reasons.append(f"CRITICAL: {field} too LOW ({val:.2f} < {lo})")
        elif val > hi:
            reasons.append(f"CRITICAL: {field} too HIGH ({val:.2f} > {hi})")
    return bool(reasons), reasons


def push_alert(msg: str):
    ts    = datetime.now().strftime("%H:%M:%S")
    entry = {"time": ts, "msg": msg}
    with state_lock:
        shared_state["alerts"].insert(0, entry)
        shared_state["alerts"] = shared_state["alerts"][:20]
    io.emit("alert", entry)

# ── Instant per-second rule-based feedback ────────────────────────────────────

def instant_feedback(reading: dict, history: list) -> dict:
    ts = datetime.now().strftime("%H:%M:%S")
    critical, crit_reasons = is_critical(reading)
    anomalies              = flag_anomalies(reading)
    v  = reading.get("voltage",   0) or 0
    i  = reading.get("current",   0) or 0
    p  = reading.get("power",     0) or 0
    f  = reading.get("frequency", 0) or 0
    pf = reading.get("pf",        0) or 0

    rising, falling = [], []
    if len(history) >= 4:
        window = history[-4:]
        for field, label, threshold in [
            ("voltage",   "Voltage",      1.5),
            ("current",   "Current",      0.3),
            ("power",     "Power",        50),
            ("frequency", "Frequency",    0.3),
            ("pf",        "Power Factor", 0.05),
        ]:
            vals = [r.get(field) for r in window if r.get(field) is not None]
            if len(vals) >= 2:
                delta = vals[-1] - vals[0]
                if delta >  threshold:
                    rising.append(f"{label} ↑ {vals[-1]:.2f} (+{delta:.2f})")
                elif delta < -threshold:
                    falling.append(f"{label} ↓ {vals[-1]:.2f} ({delta:.2f})")

    if critical:
        level = "critical"
        msg = (
            f"🔴 CRITICAL — {' | '.join(crit_reasons[:3])}. "
            f"Relay protection has been activated. "
            f"V={v:.1f}V  I={i:.3f}A  P={p:.1f}W  f={f:.2f}Hz  PF={pf:.2f}. "
            f"Immediate action required!"
        )
    elif anomalies:
        level = "warning"
        trend_note = ""
        if rising:
            trend_note = f" Still rising: {', '.join(rising[:2])}."
        elif falling:
            trend_note = f" Still falling: {', '.join(falling[:2])}."
        msg = (
            f"⚠ WARNING — {' | '.join(anomalies[:3])}.{trend_note} "
            f"V={v:.1f}V  I={i:.3f}A  P={p:.1f}W  f={f:.2f}Hz  PF={pf:.2f}. "
            f"Monitor closely."
        )
    elif rising:
        level = "warning"
        msg = (
            f"⚠ WARNING — Values increasing: {', '.join(rising[:3])}. "
            f"V={v:.1f}V  I={i:.3f}A  P={p:.1f}W  f={f:.2f}Hz  PF={pf:.2f}. "
            f"Watch for potential overload."
        )
    elif falling:
        level = "warning"
        msg = (
            f"⚠ WARNING — Values dropping: {', '.join(falling[:3])}. "
            f"V={v:.1f}V  I={i:.3f}A  P={p:.1f}W  f={f:.2f}Hz  PF={pf:.2f}. "
            f"Check supply stability."
        )
    else:
        level = "safe"
        msg = (
            f"✅ SAFE — All readings within normal range. "
            f"Voltage: {v:.1f}V  Current: {i:.3f}A  Power: {p:.1f}W  "
            f"Frequency: {f:.2f}Hz  Power Factor: {pf:.2f}. "
            f"Circuit operating normally."
        )
    return {"text": msg, "level": level, "ts": ts}


# ── LLM instant feedback ───────────────────────────────────────────────────────

_instant_llm_busy = False
_instant_llm_lock = threading.Lock()


def build_instant_prompt(reading: dict, relay_on: bool, relay_reasons: list) -> str:
    v  = reading.get("voltage",   0) or 0
    i  = reading.get("current",   0) or 0
    p  = reading.get("power",     0) or 0
    f  = reading.get("frequency", 0) or 0
    pf = reading.get("pf",        0) or 0
    ts = reading.get("_timestamp", datetime.now().strftime("%H:%M:%S"))
    relay_str = (
        "ON (load is powered)" if relay_on
        else f"OFF — cut automatically. Reasons: {'; '.join(relay_reasons)}" if relay_reasons
        else "OFF (relay open, load disconnected)"
    )
    critical, _ = is_critical(reading)
    anomalies   = flag_anomalies(reading)
    status_hint = "CRITICAL" if critical else ("WARNING" if anomalies else "NORMAL")
    return (
        f"You are a live home energy monitor AI. "
        f"Write EXACTLY ONE sentence (max 25 words) describing what is happening right now. "
        f"Include specific numbers. No markdown, no preamble, no bullet points.\n\n"
        f"Time: {ts}\n"
        f"Power: {p:.1f} W | Voltage: {v:.1f} V | Current: {i:.3f} A | "
        f"Frequency: {f:.2f} Hz | PF: {pf:.2f}\n"
        f"Relay: {relay_str}\n"
        f"Status: {status_hint}\n"
        f"Normal ranges — Voltage 210–240V | Current 0–14A | Power 0–3400W | Freq 49–51Hz | PF 0.70–1.00\n\n"
        f"One-sentence live status:"
    )


def llm_instant_worker(reading: dict, relay_on: bool, relay_reasons: list):
    global _instant_llm_busy
    ts          = reading.get("_timestamp", datetime.now().strftime("%H:%M:%S"))
    critical, _ = is_critical(reading)
    anomalies   = flag_anomalies(reading)
    level       = "critical" if critical else ("warning" if anomalies else "safe")
    try:
        payload = {
            "model":   OLLAMA_MODEL,
            "prompt":  build_instant_prompt(reading, relay_on, relay_reasons),
            "stream":  False,
            "options": {"num_predict": 60, "temperature": 0.3, "top_p": 0.8},
        }
        resp = requests.post(OLLAMA_URL, json=payload, timeout=12)
        resp.raise_for_status()
        text = resp.json().get("response", "").strip()
        if not text:
            raise ValueError("Empty LLM response")
        for sep in (".", "!", "?"):
            idx = text.find(sep)
            if 0 < idx < 220:
                text = text[: idx + 1]
                break
        io.emit("instant_feedback", {"text": text, "level": level, "ts": ts, "source": "llm"})
        print(f"[instant_llm] {ts} [{level.upper()}] {text}")
    except Exception as e:
        print(f"[instant_llm] Error — falling back to rule-based: {e}")
        with state_lock:
            hist_snap = list(shared_state["history"])
        fb = instant_feedback(reading, hist_snap)
        fb["source"] = "rule"
        io.emit("instant_feedback", fb)
    finally:
        with _instant_llm_lock:
            _instant_llm_busy = False


# ── Protection (generic critical limits) ──────────────────────────────────────

def check_and_protect_terminal(port: serial.Serial, reading: dict, relay_is_on: bool) -> bool:
    critical, reasons = is_critical(reading)
    if critical and relay_is_on:
        print(f"\n{'!'*60}")
        print("[PROTECTION] CRITICAL VALUES — CUTTING RELAY")
        for r in reasons:
            print(f"  {r}")
        print(f"{'!'*60}\n")
        send_command(port, "RELAY_OFF")
        return False
    if not critical and not relay_is_on:
        print("[PROTECTION] Values back in safe range — restoring relay")
        send_command(port, "RELAY_ON")
        return True
    return relay_is_on


def check_and_protect_server(reading: dict) -> bool:
    with state_lock:
        relay_on   = shared_state["relay"]
        relay_mode = shared_state["relay_mode"]
    critical, reasons = is_critical(reading)
    relay_cut = False
    if critical and relay_on and relay_mode == "auto":
        send_command(None, "RELAY_OFF")
        with state_lock:
            shared_state["relay"] = False
            shared_state["relay_cut_reasons"] = reasons
        relay_cut = True
        for r in reasons:
            push_alert(r)
        io.emit("relay_update", {"relay": "OFF", "relay_mode": "auto", "reason": reasons})
    elif not critical and not relay_on and relay_mode == "auto":
        send_command(None, "RELAY_ON")
        with state_lock:
            shared_state["relay"] = True
            shared_state["relay_cut_reasons"] = []
        push_alert("Values normalised — relay restored automatically")
        io.emit("relay_update", {"relay": "ON", "relay_mode": "auto", "reason": []})
    anomalies = flag_anomalies(reading)
    if anomalies and not critical:
        for a in anomalies:
            push_alert(f"ANOMALY: {a}")
    return relay_cut


# ══════════════════════════════════════════════════════════════════════════════
# OVERCURRENT PROTECTION  (current > OVERCURRENT_THRESHOLD = 12 A)
#
# State machine:
#
#   IDLE
#     │  current > 12 A
#     ▼
#   ACTIVE  ── relay OFF immediately, 30 s cooldown timer starts
#     │
#     │  (every reading) emit overcurrent_tick with countdown
#     │  (every OVERCURRENT_LLM_INTERVAL s) spawn LLM worker
#     │
#     │  30 s elapsed?
#     ├── NO  → stay in ACTIVE, keep waiting
#     └── YES → sample current NOW
#                 ├── current < 12 A → restore relay, go to IDLE ✅
#                 └── current ≥ 12 A → reset 30 s timer, stay in ACTIVE,
#                                       log retry attempt, try again ↺
#
# Emits:
#   "overcurrent_alert"   — {text, current, threshold, duration, ts, level}
#   "overcurrent_tick"    — {current, threshold, elapsed, remaining, attempt, ts}
#   "overcurrent_cleared" — {current, ts}
#   "overcurrent_retry"   — {current, attempt, ts}
# ══════════════════════════════════════════════════════════════════════════════

def build_overcurrent_prompt(reading: dict, duration_sec: float, attempt: int) -> str:
    v       = reading.get("voltage",   0) or 0
    i       = reading.get("current",   0) or 0
    p       = reading.get("power",     0) or 0
    ts      = reading.get("_timestamp", datetime.now().strftime("%H:%M:%S"))
    excess  = max(0.0, i - OVERCURRENT_THRESHOLD)
    retry   = f"  Retry attempt : #{attempt}" if attempt > 1 else ""
    return (
        f"You are a home electrical-safety AI monitoring a live circuit.\n"
        f"Write EXACTLY 2 sentences (max 45 words total) about this OVERCURRENT event.\n"
        f"Be specific: name the numbers, explain the risk, and confirm the relay is open.\n"
        f"No markdown, no bullet points, no preamble.\n\n"
        f"Time         : {ts}\n"
        f"Current      : {i:.3f} A  — EXCEEDS LIMIT ({OVERCURRENT_THRESHOLD:.0f} A) by {excess:.3f} A\n"
        f"Voltage      : {v:.1f} V\n"
        f"Power        : {p:.1f} W\n"
        f"Duration     : {duration_sec:.0f} s above limit\n"
        f"Cooldown     : {OVERCURRENT_COOLDOWN} s between relay-restore attempts\n"
        f"{retry}\n"
        f"Relay        : OPEN — load disconnected for safety\n\n"
        f"Two-sentence overcurrent safety report:"
    )


def overcurrent_llm_worker(reading: dict, duration_sec: float, attempt: int):
    global _oc_last_llm
    ts = reading.get("_timestamp", datetime.now().strftime("%H:%M:%S"))
    i  = reading.get("current",    0) or 0
    try:
        payload = {
            "model":   OLLAMA_MODEL,
            "prompt":  build_overcurrent_prompt(reading, duration_sec, attempt),
            "stream":  False,
            "options": {"num_predict": 90, "temperature": 0.3, "top_p": 0.85},
        }
        resp = requests.post(OLLAMA_URL, json=payload, timeout=15)
        resp.raise_for_status()
        text = resp.json().get("response", "").strip()
        if not text:
            raise ValueError("Empty LLM response")
        end, count = -1, 0
        for idx, ch in enumerate(text):
            if ch in ".!?":
                count += 1
                if count == 2:
                    end = idx + 1
                    break
        if end > 0:
            text = text[:end].strip()
    except Exception as e:
        text = (
            f"Overcurrent detected: {i:.3f} A exceeds the {OVERCURRENT_THRESHOLD:.0f} A safety limit "
            f"by {max(0, i - OVERCURRENT_THRESHOLD):.3f} A. "
            f"Relay has been opened to disconnect the load and prevent damage."
        )
        print(f"[overcurrent_llm] Error: {e}")

    _oc_last_llm = time.time()
    io.emit("overcurrent_alert", {
        "text":      text,
        "current":   round(i, 3),
        "threshold": OVERCURRENT_THRESHOLD,
        "duration":  round(duration_sec, 1),
        "attempt":   attempt,
        "ts":        ts,
        "level":     "critical",
    })
    push_alert(f"⚡ OVERCURRENT {i:.2f} A > {OVERCURRENT_THRESHOLD:.0f} A — {text[:100]}")
    print(f"[overcurrent] {ts}  I={i:.3f} A  dur={duration_sec:.0f}s  attempt=#{attempt} → {text[:80]}")


def check_overcurrent(reading: dict):
    """
    Called on EVERY reading from serial_reader().

    Flow:
      • current > 12 A and NOT active  → cut relay, start 30 s cooldown, mark active
      • active, 30 s NOT elapsed        → emit live tick (countdown to dashboard)
                                          spawn LLM worker every OVERCURRENT_LLM_INTERVAL s
      • active, 30 s elapsed:
          – current < 12 A              → restore relay, clear state → IDLE
          – current ≥ 12 A              → reset timer (+1 attempt), emit retry event,
                                          stay active
    """
    global _oc_active, _oc_cooldown_start, _oc_attempt, _oc_last_llm

    i  = reading.get("current", 0) or 0
    ts = reading.get("_timestamp", datetime.now().strftime("%H:%M:%S"))
    now = time.time()

    with _oc_lock:

        # ── Not in an overcurrent event ────────────────────────────────────
        if not _oc_active:
            if i > OVERCURRENT_THRESHOLD:
                # ── First detection ──────────────────────────────────────────
                _oc_active         = True
                _oc_cooldown_start = now
                _oc_attempt        = 1
                _oc_last_llm       = 0.0   # force immediate LLM call below

                send_command(None, "RELAY_OFF")
                with state_lock:
                    shared_state["relay"] = False
                    shared_state["relay_cut_reasons"] = [
                        f"OVERCURRENT: {i:.3f} A exceeds {OVERCURRENT_THRESHOLD:.0f} A limit"
                    ]
                io.emit("relay_update", {
                    "relay":      "OFF",
                    "relay_mode": "auto",
                    "reason":     [f"⚡ OVERCURRENT: {i:.3f} A > {OVERCURRENT_THRESHOLD:.0f} A"],
                })
                io.emit("critical_alert", {
                    "text": (
                        f"⚡ OVERCURRENT — {i:.3f} A exceeds {OVERCURRENT_THRESHOLD:.0f} A limit. "
                        f"Relay cut. Will retry in {OVERCURRENT_COOLDOWN} s."
                    ),
                    "ts": ts,
                })
                print(
                    f"[overcurrent] RELAY CUT — {i:.3f} A > {OVERCURRENT_THRESHOLD:.0f} A @ {ts}  "
                    f"(cooldown {OVERCURRENT_COOLDOWN} s, attempt #1)"
                )
            # else: normal reading, nothing to do
            return

        # ── We ARE in an active overcurrent event ──────────────────────────
        elapsed   = now - _oc_cooldown_start
        remaining = max(0.0, OVERCURRENT_COOLDOWN - elapsed)

        # ── Emit live countdown tick every reading ─────────────────────────
        io.emit("overcurrent_tick", {
            "current":   round(i, 3),
            "threshold": OVERCURRENT_THRESHOLD,
            "elapsed":   round(elapsed, 1),
            "remaining": round(remaining, 1),
            "cooldown":  OVERCURRENT_COOLDOWN,
            "attempt":   _oc_attempt,
            "ts":        ts,
        })

        # ── Periodic LLM commentary while waiting ─────────────────────────
        if (now - _oc_last_llm) >= OVERCURRENT_LLM_INTERVAL:
            _oc_last_llm = now   # mark early to prevent stacking
            threading.Thread(
                target=overcurrent_llm_worker,
                args=(reading, elapsed, _oc_attempt),
                daemon=True,
            ).start()

        # ── 30 s not up yet — keep waiting ────────────────────────────────
        if elapsed < OVERCURRENT_COOLDOWN:
            return

        # ══════════════════════════════════════════════════════════════════
        # 30 s elapsed — now check current
        # ══════════════════════════════════════════════════════════════════

        if i < OVERCURRENT_THRESHOLD:
            # ── Safe to restore ──────────────────────────────────────────
            _oc_active         = False
            _oc_cooldown_start = 0.0
            _oc_attempt        = 0
            _oc_last_llm       = 0.0

            send_command(None, "RELAY_ON")
            with state_lock:
                shared_state["relay"] = True
                shared_state["relay_cut_reasons"] = []

            io.emit("relay_update", {"relay": "ON", "relay_mode": "auto", "reason": []})
            io.emit("overcurrent_cleared", {"current": round(i, 3), "ts": ts})
            push_alert(
                f"✅ Overcurrent cleared after {round(elapsed)}s — "
                f"current {i:.3f} A below {OVERCURRENT_THRESHOLD:.0f} A. "
                f"Relay restored."
            )
            print(
                f"[overcurrent] CLEARED — I={i:.3f} A after {round(elapsed)}s, "
                f"relay restored @ {ts}"
            )

        else:
            # ── Still overcurrent — reset 30 s timer, try again ──────────
            _oc_cooldown_start  = now   # reset window
            _oc_attempt        += 1
            _oc_last_llm        = 0.0   # force LLM call for the new attempt

            io.emit("overcurrent_retry", {
                "current": round(i, 3),
                "attempt": _oc_attempt,
                "ts":      ts,
            })
            push_alert(
                f"⚡ Overcurrent still active after {round(elapsed)}s — "
                f"{i:.3f} A ≥ {OVERCURRENT_THRESHOLD:.0f} A. "
                f"Relay stays OFF. Retry #{_oc_attempt} in {OVERCURRENT_COOLDOWN} s."
            )
            print(
                f"[overcurrent] RETRY #{_oc_attempt} — I={i:.3f} A still high, "
                f"resetting {OVERCURRENT_COOLDOWN}s timer @ {ts}"
            )


# ── LLM prompt builder ─────────────────────────────────────────────────────────

def build_prompt(readings: list, relay_cut: bool, connected: bool = True) -> str:
    if not readings:
        return ""
    latest = readings[-1]

    def trend(key):
        vals = [r.get(key) for r in readings if r.get(key) is not None]
        if len(vals) < 2:
            return "stable"
        delta = vals[-1] - vals[0]
        if   abs(delta) < 0.01 * (vals[0] + 1e-9): return "stable"
        elif delta > 0:  return f"rising  (+{delta:+.2f})"
        else:            return f"falling ({delta:+.2f})"

    all_anomalies = []
    for r in readings:
        ts = r.get("_timestamp", "?")
        for a in flag_anomalies(r):
            all_anomalies.append(f"[{ts}] {a}")
        _, crits = is_critical(r)
        for c in crits:
            all_anomalies.append(f"[{ts}] *** {c} ***")

    anomaly_block = (
        "DETECTED ISSUES:\n" + "\n".join(f"  {a}" for a in all_anomalies)
        if all_anomalies
        else "No anomalies detected across this window."
    )

    reading_lines = []
    for r in readings:
        _, crits = is_critical(r)
        flags = flag_anomalies(r)
        tag = " [CRITICAL]" if crits else (" [ANOMALY]" if flags else "")
        reading_lines.append(
            f"  [{r.get('_timestamp','?')}]  "
            f"V={r.get('voltage','?'):>7}V  "
            f"I={r.get('current','?'):>7}A  "
            f"P={r.get('power','?'):>8}W  "
            f"E={r.get('energy','?'):>8}kWh  "
            f"f={r.get('frequency','?'):>6}Hz  "
            f"PF={r.get('pf','?'):>5}{tag}"
        )

    conn_status  = "ESP32 is ONLINE and actively streaming sensor data." if connected \
                   else "⚠ ESP32 IS OFFLINE — no live data from the sensor."
    relay_status = (
        "⚡ RELAY IS CURRENTLY OFF — was automatically cut due to overcurrent. "
        f"Will retry every {OVERCURRENT_COOLDOWN} s until current < {OVERCURRENT_THRESHOLD} A."
        if relay_cut else
        "Relay is ON — the load is powered."
    )

    return f"""You are a live electrical-system narrator monitoring a home energy circuit via a PZEM-004T sensor on an ESP32 microcontroller. Every 5 seconds you deliver a spoken-style, real-time status report covering EVERY metric.

SYSTEM SNAPSHOT  ({datetime.now().strftime('%H:%M:%S')})
{conn_status}
{relay_status}

Normal operating ranges:
  Voltage 210–240 V  |  Current 0–{OVERCURRENT_THRESHOLD:.0f} A  |  Power 0–3500 W  |  Frequency 49–51 Hz  |  PF 0.70–1.00

─── Raw readings (oldest → newest) ───────────────────────────────────────
{chr(10).join(reading_lines)}

─── Trends across this {len(readings)}-reading window ─────────────────────────────────
  Voltage   : {trend('voltage')}
  Current   : {trend('current')}
  Power     : {trend('power')}
  Frequency : {trend('frequency')}
  Power Fac.: {trend('pf')}

─── Anomaly summary ────────────────────────────────────────────────────────
{anomaly_block}

YOUR TASK — Write a live, spoken-style status report in PLAIN PROSE (no bullet points, no markdown). Cover voltage, current, power, energy, frequency, power factor, relay state, trends, and end with exactly one of: ✅ SAFE  ⚠ WARNING  🔴 CRITICAL
"""


def ask_ollama_terminal(prompt: str) -> str:
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": True}
    try:
        resp = requests.post(OLLAMA_URL, json=payload, stream=True, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        return "[Ollama] Connection refused — is Ollama running? Try: ollama serve"
    except requests.exceptions.RequestException as e:
        return f"[Ollama] Request error: {e}"
    full = []
    sys.stdout.write("\n[LLM] ")
    sys.stdout.flush()
    for raw in resp.iter_lines():
        if not raw:
            continue
        try:
            chunk = json.loads(raw)
        except json.JSONDecodeError:
            continue
        token = chunk.get("response", "")
        if token:
            sys.stdout.write(token)
            sys.stdout.flush()
            full.append(token)
        if chunk.get("done"):
            break
    sys.stdout.write("\n")
    sys.stdout.flush()
    return "".join(full)


def ask_ollama_server(prompt: str) -> str:
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": True}
    try:
        resp = requests.post(OLLAMA_URL, json=payload, stream=True, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        err = "[Ollama] Connection refused — is Ollama running? Try: ollama serve"
        io.emit("analysis", {"text": err})
        return err
    except Exception as e:
        err = f"[Ollama error] {e}"
        io.emit("analysis", {"text": err})
        return err
    tokens = []
    for raw in resp.iter_lines():
        if not raw:
            continue
        try:
            chunk = json.loads(raw)
        except json.JSONDecodeError:
            continue
        token = chunk.get("response", "")
        if token:
            tokens.append(token)
            io.emit("llm_token", {"token": token})
        if chunk.get("done"):
            break
    return "".join(tokens)


# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL MODE
# ══════════════════════════════════════════════════════════════════════════════

def run_terminal():
    print("=" * 60)
    print("  Energy Monitor — Terminal Agent")
    print(f"  Serial : {SERIAL_PORT} @ {BAUD_RATE}")
    print(f"  Model  : {OLLAMA_MODEL}")
    print(f"  Batch  : every {ANALYSIS_INTERVAL} seconds")
    print(f"  OC cut : {OVERCURRENT_THRESHOLD} A  |  Cooldown: {OVERCURRENT_COOLDOWN} s")
    print("=" * 60)

    port               = open_serial_blocking()
    relay_on           = True
    relay_cut_in_batch = False
    last_analysis_time = 0
    buffer             = []

    while True:
        data = read_line(port)
        if data is None:
            continue
        if data.get("status") != "ok":
            print(f"[esp32] {data.get('msg', data)}")
            continue

        data["_timestamp"] = datetime.now().strftime("%H:%M:%S")
        print(
            f"[{data['_timestamp']}] "
            f"V={data.get('voltage',0):6.1f}V  "
            f"I={data.get('current',0):5.3f}A  "
            f"P={data.get('power',0):7.2f}W  "
            f"E={data.get('energy',0):7.3f}kWh  "
            f"f={data.get('frequency',0):5.2f}Hz  "
            f"PF={data.get('pf',0):.2f}  "
            f"Relay={'ON' if relay_on else 'OFF'}"
        )

        new_state = check_and_protect_terminal(port, data, relay_on)
        if new_state != relay_on:
            relay_cut_in_batch = True
        relay_on = new_state

        buffer.append(data)
        if len(buffer) > READINGS_PER_PROMPT:
            buffer = buffer[-READINGS_PER_PROMPT:]

        now = time.time()
        if now - last_analysis_time >= ANALYSIS_INTERVAL:
            last_analysis_time = now
            print(f"\n{'─'*60}")
            print(f"[agent] Auto-analysing {len(buffer)} readings with {OLLAMA_MODEL}...")
            ask_ollama_terminal(build_prompt(buffer, relay_cut_in_batch))
            print(f"{'─'*60}\n")
            relay_cut_in_batch = False


# ══════════════════════════════════════════════════════════════════════════════
# SERVER MODE — serial reader thread
# ══════════════════════════════════════════════════════════════════════════════

def serial_reader():
    global _port
    while True:
        if _port is None or not _port.is_open:
            _port = open_serial_nonblocking()
            if _port is None:
                time.sleep(3)
                continue
        try:
            raw = _port.readline().decode("utf-8", errors="replace").strip()
        except serial.SerialException as e:
            print(f"[serial] Read error: {e}")
            with state_lock:
                shared_state["connected"] = False
            _port = None
            continue
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if data.get("status") != "ok":
            continue

        data["_timestamp"] = datetime.now().strftime("%H:%M:%S")
        data["_ts_epoch"]  = time.time()

        # ── Overcurrent check (30-s cooldown state machine) ───────────────
        check_overcurrent(data)

        # ── Generic critical-limits check ─────────────────────────────────
        check_and_protect_server(data)

        data["anomalies"] = flag_anomalies(data)

        with state_lock:
            shared_state["latest"] = data
            shared_state["history"].append(data)
            shared_state["history"] = shared_state["history"][-100:]
            shared_state["connected"] = True

        io.emit("reading", data)

        # ── LLM instant feedback ──────────────────────────────────────────
        global _instant_llm_busy
        with state_lock:
            relay_on_snap = shared_state["relay"]
            cut_reasons   = list(shared_state["relay_cut_reasons"])

        with _instant_llm_lock:
            if not _instant_llm_busy:
                _instant_llm_busy = True
                threading.Thread(
                    target=llm_instant_worker,
                    args=(data, relay_on_snap, cut_reasons),
                    daemon=True,
                ).start()
            else:
                with state_lock:
                    hist_snap = list(shared_state["history"])
                fb = instant_feedback(data, hist_snap)
                fb["source"] = "rule"
                io.emit("instant_feedback", fb)

        critical, crit_reasons = is_critical(data)
        if critical:
            io.emit("critical_alert", {
                "text":    f"⚡ CRITICAL: {' | '.join(crit_reasons[:3])}",
                "ts":      data["_timestamp"],
                "reasons": crit_reasons,
            })


# ══════════════════════════════════════════════════════════════════════════════
# SERVER MODE — LLM analysis loop
# ══════════════════════════════════════════════════════════════════════════════

def llm_analysis_loop():
    last_run = time.time()
    while True:
        time.sleep(1)
        now       = time.time()
        elapsed   = now - last_run
        remaining = max(0, int(ANALYSIS_INTERVAL - elapsed))
        with state_lock:
            busy = shared_state["llm_busy"]
        io.emit("analysis_tick", {
            "remaining": remaining,
            "interval":  ANALYSIS_INTERVAL,
            "busy":      busy,
        })
        if elapsed < ANALYSIS_INTERVAL:
            continue
        with state_lock:
            if shared_state["llm_busy"]:
                print("[agent] LLM still busy — skipping this cycle")
                last_run = now
                continue
            readings  = list(shared_state["history"][-READINGS_PER_PROMPT:])
            relay_cut = (not shared_state["relay"]) and shared_state["relay_mode"] == "auto"
            connected = shared_state["connected"]
            shared_state["llm_busy"] = True
        last_run = now
        if not readings:
            with state_lock:
                shared_state["llm_busy"] = False
            continue
        print(f"[agent] Auto-analysis — {len(readings)} readings → {OLLAMA_MODEL}...")
        io.emit("analysis_start", {"ts": datetime.now().strftime("%H:%M:%S")})
        try:
            analysis = ask_ollama_server(build_prompt(readings, relay_cut, connected))
            with state_lock:
                shared_state["last_analysis"]    = analysis
                shared_state["last_analysis_ts"] = time.time()
            io.emit("analysis", {"text": analysis, "ts": datetime.now().strftime("%H:%M:%S")})
        except Exception as e:
            print(f"[agent] LLM error: {e}")
        finally:
            with state_lock:
                shared_state["llm_busy"] = False


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    dashboard = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    if os.path.exists(dashboard):
        return send_file(dashboard)
    return "<h2>dashboard.html not found</h2>", 404

@app.route("/api/status")
def api_status():
    with _oc_lock:
        oc_active    = _oc_active
        oc_elapsed   = round(time.time() - _oc_cooldown_start, 1) if _oc_active else 0
        oc_remaining = round(max(0, OVERCURRENT_COOLDOWN - oc_elapsed), 1) if _oc_active else 0
        oc_attempt   = _oc_attempt
    with state_lock:
        return jsonify({
            "connected":          shared_state["connected"],
            "relay":              "ON" if shared_state["relay"] else "OFF",
            "relay_mode":         shared_state["relay_mode"],
            "latest":             shared_state["latest"],
            "last_analysis":      shared_state["last_analysis"],
            "last_analysis_ts":   shared_state["last_analysis_ts"],
            "llm_busy":           shared_state["llm_busy"],
            "analysis_interval":  ANALYSIS_INTERVAL,
            "serial_port":        SERIAL_PORT,
            "model":              OLLAMA_MODEL,
            "overcurrent": {
                "active":    oc_active,
                "elapsed":   oc_elapsed,
                "remaining": oc_remaining,
                "cooldown":  OVERCURRENT_COOLDOWN,
                "threshold": OVERCURRENT_THRESHOLD,
                "attempt":   oc_attempt,
            },
        })

@app.route("/api/history")
def api_history():
    limit = int(request.args.get("limit", 100))
    with state_lock:
        return jsonify(shared_state["history"][-limit:])

@app.route("/api/alerts")
def api_alerts():
    with state_lock:
        return jsonify(shared_state["alerts"])

@app.route("/api/relay", methods=["POST"])
def api_relay():
    body   = request.get_json(force=True) or {}
    action = body.get("action", "").upper()
    if action not in ("ON", "OFF", "AUTO"):
        return jsonify({"error": "action must be ON, OFF, or AUTO"}), 400
    if action == "AUTO":
        with state_lock:
            shared_state["relay_mode"] = "auto"
        push_alert("Relay mode set to AUTO via dashboard")
        with state_lock:
            r = "ON" if shared_state["relay"] else "OFF"
        io.emit("relay_update", {"relay": r, "relay_mode": "auto"})
        return jsonify({"relay_mode": "auto"})
    send_command(None, f"RELAY_{action}")
    with state_lock:
        shared_state["relay"]      = (action == "ON")
        shared_state["relay_mode"] = "manual"
    push_alert(f"Relay manually set to {action} via dashboard")
    io.emit("relay_update", {"relay": action, "relay_mode": "manual"})
    return jsonify({"relay": action, "relay_mode": "manual"})

@app.route("/api/analysis")
def api_analysis():
    with state_lock:
        return jsonify({"analysis": shared_state["last_analysis"]})

@app.route("/api/analyse_now", methods=["POST"])
def api_analyse_now():
    with state_lock:
        readings = shared_state["history"][-READINGS_PER_PROMPT:]
        if shared_state["llm_busy"]:
            return jsonify({"error": "LLM is currently busy, please wait"}), 503
    if not readings:
        return jsonify({"error": "No readings yet"}), 503

    def _run():
        with state_lock:
            shared_state["llm_busy"] = True
            _connected = shared_state["connected"]
        io.emit("analysis_start", {"ts": datetime.now().strftime("%H:%M:%S")})
        try:
            analysis = ask_ollama_server(build_prompt(readings, False, _connected))
            with state_lock:
                shared_state["last_analysis"] = analysis
            io.emit("analysis", {"text": analysis, "ts": datetime.now().strftime("%H:%M:%S")})
        finally:
            with state_lock:
                shared_state["llm_busy"] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "analysis started"})

@app.route("/api/ranges")
def api_ranges():
    return jsonify({"normal": NORMAL_RANGES, "critical": CRITICAL_LIMITS})


# ── WebSocket events ───────────────────────────────────────────────────────────

@io.on("connect")
def on_connect():
    print("[ws] Client connected")
    with _oc_lock:
        oc_snap = {
            "active":    _oc_active,
            "remaining": round(max(0, OVERCURRENT_COOLDOWN - (time.time() - _oc_cooldown_start)), 1)
                         if _oc_active else 0,
            "cooldown":  OVERCURRENT_COOLDOWN,
            "threshold": OVERCURRENT_THRESHOLD,
            "attempt":   _oc_attempt,
        }
    with state_lock:
        snapshot = {
            "relay":             "ON" if shared_state["relay"] else "OFF",
            "relay_mode":        shared_state["relay_mode"],
            "history":           shared_state["history"][-20:],
            "alerts":            shared_state["alerts"],
            "analysis":          shared_state["last_analysis"],
            "llm_busy":          shared_state["llm_busy"],
            "analysis_interval": ANALYSIS_INTERVAL,
            "serial_port":       SERIAL_PORT,
            "model":             OLLAMA_MODEL,
            "overcurrent":       oc_snap,
        }
    io.emit("status_snapshot", snapshot)

@io.on("disconnect")
def on_disconnect():
    print("[ws] Client disconnected")

@io.on("relay_command")
def on_relay_command(data):
    action = str(data.get("action", "")).upper()
    if action in ("ON", "OFF"):
        send_command(None, f"RELAY_{action}")
        with state_lock:
            shared_state["relay"]      = (action == "ON")
            shared_state["relay_mode"] = "manual"
        push_alert(f"Relay set to {action} via dashboard")
        io.emit("relay_update", {"relay": action, "relay_mode": "manual"})
    elif action == "AUTO":
        with state_lock:
            shared_state["relay_mode"] = "auto"
            r = "ON" if shared_state["relay"] else "OFF"
        push_alert("Relay mode set to AUTO via dashboard")
        io.emit("relay_update", {"relay": r, "relay_mode": "auto"})


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    global SERIAL_PORT, OLLAMA_MODEL, ANALYSIS_INTERVAL, OVERCURRENT_COOLDOWN
    parser = argparse.ArgumentParser(description="Energy Monitor")
    parser.add_argument("--mode",     choices=["terminal", "server"], default="terminal")
    parser.add_argument("--port",     default=SERIAL_PORT,       help="Serial port")
    parser.add_argument("--model",    default=OLLAMA_MODEL,      help="Ollama model name")
    parser.add_argument("--webport",  type=int, default=5000,    help="Web server port")
    parser.add_argument("--interval", type=int, default=ANALYSIS_INTERVAL,
                        help=f"LLM analysis interval in seconds (default: {ANALYSIS_INTERVAL})")
    parser.add_argument("--cooldown", type=int, default=OVERCURRENT_COOLDOWN,
                        help=f"Overcurrent relay-off cooldown in seconds (default: {OVERCURRENT_COOLDOWN})")
    args = parser.parse_args()

    
    SERIAL_PORT          = args.port
    OLLAMA_MODEL         = args.model
    ANALYSIS_INTERVAL    = args.interval
    OVERCURRENT_COOLDOWN = args.cooldown

    if args.mode == "terminal":
        try:
            run_terminal()
        except KeyboardInterrupt:
            print("\n[agent] Stopped.")
    else:
        import socket as _socket
        try:
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            local_ip = "localhost"

        print("=" * 60)
        print("  Energy Monitor — Server Mode")
        print(f"  Serial      : {SERIAL_PORT} @ {BAUD_RATE}")
        print(f"  Model       : {OLLAMA_MODEL}")
        print(f"  LLM every   : {ANALYSIS_INTERVAL}s")
        print(f"  OC cut      : >{OVERCURRENT_THRESHOLD:.0f} A → relay OFF, retry every {OVERCURRENT_COOLDOWN}s")
        print(f"  ────────────────────────────────────")
        print(f"  Open this in your browser:")
        print(f"  >>> http://{local_ip}:{args.webport} <<<")
        print(f"  ────────────────────────────────────")
        print("=" * 60)

        threading.Thread(target=serial_reader,     daemon=True).start()
        threading.Thread(target=llm_analysis_loop, daemon=True).start()

        io.run(app, host="0.0.0.0", port=args.webport)


if __name__ == "__main__":
    main()
