from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


class VerificationError(RuntimeError):
    pass


@dataclass
class PollResult:
    status: str
    matched_event: Optional[Dict[str, Any]]
    sampled_events: List[Dict[str, Any]]
    reason: str


class EventClient:
    def __init__(self, base_url: str, timeout_sec: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    def fetch_events(
        self,
        *,
        env: str,
        device_id: str,
        limit: int = 20,
        event_name: Optional[str] = None,
        since_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "env": env,
            "device_id": device_id,
            "limit": limit,
        }
        if event_name:
            params["event_name"] = event_name
        if since_id is not None:
            params["since_id"] = since_id

        resp = requests.get(f"{self.base_url}/events", params=params, timeout=self.timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise VerificationError("events response is not a list")
        return data

    def get_baseline_id(self, *, env: str, device_id: str) -> int:
        data = self.fetch_events(env=env, device_id=device_id, limit=1)
        if not data:
            return 0
        top = data[0]
        return int(top.get("id", 0))

    def wait_for_event(
        self,
        *,
        env: str,
        device_id: str,
        event_name: str,
        required_fields: Optional[List[str]],
        timeout_sec: int,
        poll_interval_sec: float = 1.5,
        since_id: Optional[int] = None,
    ) -> PollResult:
        end_ts = time.time() + timeout_sec
        sampled_events: List[Dict[str, Any]] = []
        last_error = ""

        while time.time() < end_ts:
            try:
                rows = self.fetch_events(
                    env=env,
                    device_id=device_id,
                    event_name=event_name,
                    limit=20,
                    since_id=since_id,
                )
            except Exception as e:  # noqa: BLE001
                last_error = str(e)
                time.sleep(poll_interval_sec)
                continue

            if rows:
                sampled_events = rows[:3]
                for row in rows:
                    ok, reason = _check_required_fields(row, required_fields)
                    if ok:
                        return PollResult(
                            status="pass",
                            matched_event=row,
                            sampled_events=sampled_events,
                            reason="matched",
                        )
                    last_error = reason

            time.sleep(poll_interval_sec)

        return PollResult(
            status="fail",
            matched_event=None,
            sampled_events=sampled_events,
            reason=last_error or "timeout waiting for expected event",
        )


def _check_required_fields(event: Dict[str, Any], required_fields: Optional[List[str]]) -> tuple[bool, str]:
    if not required_fields:
        return True, "no required fields"

    for field_path in required_fields:
        parts = field_path.split(".")
        cursor: Any = event
        for part in parts:
            if not isinstance(cursor, dict) or part not in cursor:
                return False, f"missing required field: {field_path}"
            cursor = cursor[part]
        if cursor is None:
            return False, f"required field is None: {field_path}"

    return True, "all required fields matched"

