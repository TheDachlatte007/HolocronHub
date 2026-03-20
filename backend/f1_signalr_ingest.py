from __future__ import annotations

import argparse
import json
import os
import ssl
import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

try:
    import websocket
except Exception as exc:  # pragma: no cover - import guard for local setup
    raise SystemExit(
        "websocket-client is required for backend/f1_signalr_ingest.py. "
        "Install backend requirements first."
    ) from exc


ENV_FILE = Path(__file__).resolve().parent / ".env"
DEFAULT_BASE_URL = "livetiming.formula1.com/signalr"
DEFAULT_HUB = "Streaming"
DEFAULT_TOPICS = [
    "Heartbeat",
    "SessionInfo",
    "TrackStatus",
    "LapCount",
    "DriverList",
    "TimingData",
    "TimingAppData",
    "WeatherData",
    "RaceControlMessages",
]


def _load_local_env() -> None:
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _post_json(url: str, payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    res = requests.post(url, json=payload, timeout=timeout)
    res.raise_for_status()
    if "application/json" in str(res.headers.get("Content-Type", "")):
        return res.json()
    return {"ok": True, "status_code": res.status_code}


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _deep_merge(current: Any, incoming: Any) -> Any:
    if isinstance(current, dict) and isinstance(incoming, dict):
        merged = deepcopy(current)
        for key, value in incoming.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    return deepcopy(incoming)


def _driver_name(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return "Driver"
    full = str(item.get("FullName") or "").strip()
    if full:
        return full
    first = str(item.get("FirstName") or "").strip()
    last = str(item.get("LastName") or "").strip()
    joined = f"{first} {last}".strip()
    if joined:
        return joined
    return str(item.get("BroadcastName") or item.get("Tla") or item.get("RacingNumber") or "Driver")


def _team_color(item: dict[str, Any]) -> str | None:
    raw = str(item.get("TeamColour") or item.get("TeamColor") or "").strip().lstrip("#")
    return raw or None


def _latest_compound(stints_line: dict[str, Any]) -> tuple[str | None, int | None]:
    if not isinstance(stints_line, dict):
        return None, None
    stints = stints_line.get("Stints") if isinstance(stints_line.get("Stints"), list) else []
    if not stints:
        return None, None
    current = stints[-1] if isinstance(stints[-1], dict) else {}
    compound = current.get("Compound") or current.get("Tyre") or current.get("CompoundShortName")
    stint_laps = _safe_int(current.get("TotalLaps") or current.get("Laps") or current.get("LapCount"))
    return (str(compound).strip() if compound else None), stint_laps


def _extract_interval(line: dict[str, Any]) -> tuple[str | None, str | None]:
    if not isinstance(line, dict):
        return None, None
    gap = line.get("GapToLeader")
    if isinstance(gap, dict):
        gap = gap.get("Value")
    interval = line.get("IntervalToPositionAhead")
    if isinstance(interval, dict):
        interval = interval.get("Value")
    return (
        str(gap).strip() if gap not in (None, "") else None,
        str(interval).strip() if interval not in (None, "") else None,
    )


def _extract_time_value(line: dict[str, Any], key: str) -> str | None:
    value = line.get(key)
    if isinstance(value, dict):
        value = value.get("Value")
    if value in (None, ""):
        return None
    return str(value).strip()


def _build_leaderboard(state: dict[str, Any]) -> list[dict[str, Any]]:
    drivers = state.get("DriverList") if isinstance(state.get("DriverList"), dict) else {}
    timing = state.get("TimingData") if isinstance(state.get("TimingData"), dict) else {}
    timing_lines = timing.get("Lines") if isinstance(timing.get("Lines"), dict) else {}
    app_data = state.get("TimingAppData") if isinstance(state.get("TimingAppData"), dict) else {}
    app_lines = app_data.get("Lines") if isinstance(app_data.get("Lines"), dict) else {}

    rows: list[dict[str, Any]] = []
    for number, driver in drivers.items():
        line = timing_lines.get(number) if isinstance(timing_lines, dict) else {}
        app_line = app_lines.get(number) if isinstance(app_lines, dict) else {}
        gap_to_leader, interval = _extract_interval(line if isinstance(line, dict) else {})
        compound, stint_laps = _latest_compound(app_line if isinstance(app_line, dict) else {})
        pos = _safe_int((line or {}).get("Position")) if isinstance(line, dict) else None
        rows.append(
            {
                "position": pos,
                "driver_number": str(number),
                "driver": _driver_name(driver if isinstance(driver, dict) else {}),
                "code": str((driver if isinstance(driver, dict) else {}).get("Tla") or "").strip() or None,
                "team": str((driver if isinstance(driver, dict) else {}).get("TeamName") or "").strip() or None,
                "team_color": _team_color(driver if isinstance(driver, dict) else {}),
                "grid_position": _safe_int((line or {}).get("GridLine")) if isinstance(line, dict) else None,
                "gap_to_leader": gap_to_leader,
                "interval": interval,
                "best_lap_time": _extract_time_value(line if isinstance(line, dict) else {}, "BestLapTime"),
                "time": _extract_time_value(line if isinstance(line, dict) else {}, "LastLapTime"),
                "current_tyre": compound,
                "stint": stint_laps,
                "status": "PIT" if bool((line or {}).get("InPit")) else ("OUT" if bool((line or {}).get("Stopped")) else None),
            }
        )

    rows.sort(key=lambda row: (_safe_int(row.get("position")) or 999, _safe_int(row.get("driver_number")) or 999))
    return rows[:20]


def _build_weather(state: dict[str, Any]) -> dict[str, Any]:
    weather = state.get("WeatherData") if isinstance(state.get("WeatherData"), dict) else {}
    return {
        "air_temperature": weather.get("AirTemp"),
        "track_temperature": weather.get("TrackTemp"),
        "humidity": weather.get("Humidity"),
        "rainfall": weather.get("Rainfall"),
        "wind_direction": weather.get("WindDirection"),
        "wind_speed": weather.get("WindSpeed"),
        "pressure": weather.get("Pressure"),
        "date": weather.get("Timestamp") or _utc_now(),
        "source": "f1_signalr",
    }


def _build_race_control(state: dict[str, Any]) -> list[dict[str, Any]]:
    messages = state.get("RaceControlMessages") if isinstance(state.get("RaceControlMessages"), dict) else {}
    rows = messages.get("Messages") if isinstance(messages.get("Messages"), dict) else {}
    out: list[dict[str, Any]] = []
    for _, item in sorted(rows.items(), key=lambda pair: str(pair[0]), reverse=True):
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "date": item.get("Utc"),
                "category": item.get("Category"),
                "flag": item.get("Flag"),
                "message": item.get("Message"),
                "scope": item.get("Scope"),
                "driver_number": item.get("RacingNumber"),
                "lap_number": item.get("Lap"),
            }
        )
        if len(out) >= 12:
            break
    return out


def _session_name_from_state(state: dict[str, Any], fallback: str) -> str:
    session_info = state.get("SessionInfo") if isinstance(state.get("SessionInfo"), dict) else {}
    return str(session_info.get("Name") or session_info.get("MeetingName") or fallback or "Live Session")


def _data_as_of_from_state(state: dict[str, Any]) -> str:
    heartbeat = state.get("Heartbeat") if isinstance(state.get("Heartbeat"), dict) else {}
    heartbeat_utc = str(heartbeat.get("Utc") or "").strip()
    if heartbeat_utc:
        return heartbeat_utc
    weather = state.get("WeatherData") if isinstance(state.get("WeatherData"), dict) else {}
    weather_ts = str(weather.get("Timestamp") or "").strip()
    if weather_ts:
        return weather_ts
    return _utc_now()


class F1SignalRIngest:
    def __init__(self, *, base_url: str, hub: str, session_key: str, session_name: str, ingest_url: str, topics: list[str], probe_only: bool, push_interval: float, once: bool) -> None:
        self.base_url = base_url.strip().rstrip("/")
        self.hub = hub
        self.session_key = session_key
        self.session_name = session_name
        self.ingest_url = ingest_url
        self.topics = topics
        self.probe_only = probe_only
        self.push_interval = max(1.0, push_interval)
        self.once = once
        self.state: dict[str, Any] = {}
        self.last_push = 0.0

    def negotiate(self) -> tuple[str, str]:
        connection_data = json.dumps([{"name": self.hub}])
        url = f"https://{self.base_url}/negotiate?{urlencode({'clientProtocol': '1.5', 'connectionData': connection_data})}"
        response = requests.get(url, timeout=20, headers={"User-Agent": "BestHTTP", "Accept": "application/json, text/plain, */*"})
        response.raise_for_status()
        data = response.json()
        token = str(data.get("ConnectionToken") or data.get("connectionToken") or "").strip()
        if not token:
            raise RuntimeError("SignalR negotiation returned no connection token")
        cookie = response.headers.get("Set-Cookie", "")
        return token, cookie

    def connect(self):
        token, cookie = self.negotiate()
        params = urlencode(
            {
                "transport": "webSockets",
                "clientProtocol": "1.5",
                "connectionToken": token,
                "connectionData": json.dumps([{"name": self.hub}]),
                "tid": "10",
            }
        )
        url = f"wss://{self.base_url}/connect?{params}"
        headers = [
            "User-Agent: BestHTTP",
            "Accept-Encoding: gzip,identity",
        ]
        if cookie:
            headers.append(f"Cookie: {cookie}")
        return websocket.create_connection(
            url,
            header=headers,
            timeout=20,
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
        )

    def subscribe(self, ws) -> dict[str, Any]:
        req_id = str(uuid.uuid4())
        message = {"h": self.hub, "m": "Subscribe", "a": [self.topics], "i": req_id}
        ws.send(json.dumps(message))
        deadline = time.time() + 15
        while time.time() < deadline:
            raw = ws.recv()
            if not raw:
                continue
            data = json.loads(raw)
            if not isinstance(data, dict):
                continue
            if "R" in data:
                response_id = str(data.get("I") or "").strip()
                if response_id and response_id != req_id:
                    continue
                result = data.get("R")
                return result if isinstance(result, dict) else {}
        raise TimeoutError("SignalR subscribe did not return an initial state in time")

    def merge_topic(self, topic: str, payload: Any) -> None:
        self.state[topic] = _deep_merge(self.state.get(topic), payload)

    def ingest_payload(self) -> dict[str, Any]:
        leaderboard = _build_leaderboard(self.state)
        race_control = _build_race_control(self.state)
        weather = _build_weather(self.state)
        live = True
        session_info = self.state.get("SessionInfo") if isinstance(self.state.get("SessionInfo"), dict) else {}
        track_status = self.state.get("TrackStatus") if isinstance(self.state.get("TrackStatus"), dict) else {}
        lap_count = self.state.get("LapCount") if isinstance(self.state.get("LapCount"), dict) else {}
        return {
            "session_key": self.session_key,
            "source": "signalr_secondary",
            "data_as_of": _data_as_of_from_state(self.state),
            "live": live,
            "drivers_count": len(leaderboard),
            "session": {
                "session_key": self.session_key,
                "session_name": _session_name_from_state(self.state, self.session_name),
                "status": "live" if live else "upcoming",
                "meeting_name": session_info.get("MeetingName"),
                "meeting_key": session_info.get("MeetingKey"),
                "track_status": track_status.get("Status"),
                "track_status_message": track_status.get("Message"),
                "lap_current": lap_count.get("CurrentLap"),
                "lap_total": lap_count.get("TotalLaps"),
            },
            "leaderboard": leaderboard,
            "race_control": race_control,
            "weather": weather,
        }

    def push(self) -> None:
        payload = self.ingest_payload()
        if self.probe_only:
            print(json.dumps(payload, ensure_ascii=False, indent=2)[:6000], flush=True)
            return
        result = _post_json(self.ingest_url, payload, timeout=10)
        print(f"[{_utc_now()}] pushed {payload['session_key']} rows={len(payload['leaderboard'])} rc={len(payload['race_control'])} -> {result}", flush=True)

    def maybe_push(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self.last_push) < self.push_interval:
            return
        self.push()
        self.last_push = now
        if self.once:
            raise SystemExit(0)

    def run(self) -> None:
        print(f"[{_utc_now()}] connecting to SignalR at {self.base_url} for session {self.session_key}", flush=True)
        ws = self.connect()
        try:
            ws.settimeout(10)
            initial = self.subscribe(ws)
            if isinstance(initial, dict):
                for topic, payload in initial.items():
                    self.merge_topic(topic, payload)
            print(f"[{_utc_now()}] subscribed to {', '.join(self.topics)}", flush=True)
            self.maybe_push(force=True)
            while True:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    continue
                data = json.loads(raw)
                updates = data.get("M") if isinstance(data, dict) else None
                if not isinstance(updates, list):
                    continue
                touched = False
                for update in updates:
                    if not isinstance(update, dict):
                        continue
                    args = update.get("A")
                    if not isinstance(args, list) or len(args) < 2:
                        continue
                    topic = args[0]
                    payload = args[1]
                    self.merge_topic(str(topic), payload)
                    touched = True
                if touched:
                    self.maybe_push()
        finally:
            try:
                ws.close()
            except Exception:
                pass


def main() -> int:
    _load_local_env()
    parser = argparse.ArgumentParser(description="Experimental F1 SignalR secondary ingest worker.")
    parser.add_argument("--session-key", default=_env("F1_SIGNALR_SESSION_KEY"), help="Session key for HolocronHub ingest, e.g. fallback:2026:2:race")
    parser.add_argument("--session-name", default=_env("F1_SIGNALR_SESSION_NAME", "Live Session"), help="Fallback session name")
    parser.add_argument("--ingest-url", default=_env("F1_SIGNALR_INGEST_URL", "http://127.0.0.1:8787/api/f1/ingest/session"))
    parser.add_argument("--base-url", default=_env("F1_SIGNALR_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--hub", default=_env("F1_SIGNALR_HUB", DEFAULT_HUB))
    parser.add_argument("--topics", default=_env("F1_SIGNALR_TOPICS", ",".join(DEFAULT_TOPICS)))
    parser.add_argument("--push-interval", type=float, default=float(_env("F1_SIGNALR_PUSH_INTERVAL", "2")))
    parser.add_argument("--probe-only", action="store_true", help="Connect and print payloads instead of posting them")
    parser.add_argument("--once", action="store_true", help="Exit after the first successful payload push")
    args = parser.parse_args()

    if not args.session_key:
        raise SystemExit("Missing --session-key or F1_SIGNALR_SESSION_KEY")

    topics = [part.strip() for part in str(args.topics or "").split(",") if part.strip()]
    worker = F1SignalRIngest(
        base_url=args.base_url,
        hub=args.hub,
        session_key=args.session_key,
        session_name=args.session_name,
        ingest_url=args.ingest_url,
        topics=topics or list(DEFAULT_TOPICS),
        probe_only=bool(args.probe_only),
        push_interval=float(args.push_interval),
        once=bool(args.once),
    )
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
