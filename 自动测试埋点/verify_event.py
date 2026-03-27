from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


class VerifyError(RuntimeError):
    pass


def _get_by_path(obj: Any, path: str) -> Tuple[bool, Any]:
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


@dataclass
class VerifyResult:
    status: str
    reason: str
    baseline_since_id: int
    matched_event: Optional[Dict[str, Any]]
    sampled_events: List[Dict[str, Any]]


def fetch_events(
    *,
    base_url: str,
    env: str,
    device_id: str,
    limit: int,
    event_name: Optional[str],
    since_id: Optional[int],
    timeout_sec: int,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"env": env, "device_id": device_id, "limit": limit}
    if event_name:
        params["event_name"] = event_name
    if since_id is not None:
        params["since_id"] = since_id

    r = requests.get(f"{base_url.rstrip('/')}/events", params=params, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise VerifyError("events response is not a list")
    return data


def get_baseline_since_id(*, base_url: str, env: str, device_id: str) -> int:
    rows = fetch_events(
        base_url=base_url,
        env=env,
        device_id=device_id,
        limit=1,
        event_name=None,
        since_id=None,
        timeout_sec=10,
    )
    if not rows:
        return 0
    return int(rows[0].get("id", 0) or 0)


def verify_once(
    *,
    base_url: str,
    env: str,
    device_id: str,
    event_name: str,
    required_fields: List[str],
    timeout_sec: int,
    poll_interval_sec: float,
    request_timeout_sec: int,
    baseline_since_id: int,
) -> VerifyResult:
    deadline = time.time() + timeout_sec
    last_reason = ""
    sampled: List[Dict[str, Any]] = []

    while time.time() < deadline:
        try:
            rows = fetch_events(
                base_url=base_url,
                env=env,
                device_id=device_id,
                limit=50,
                event_name=event_name,
                since_id=baseline_since_id,
                timeout_sec=request_timeout_sec,
            )
        except Exception as e:  # noqa: BLE001
            last_reason = f"query_failed: {type(e).__name__}: {e}"
            time.sleep(poll_interval_sec)
            continue

        if rows:
            sampled = rows[:3]
            for row in rows:
                for field in required_fields:
                    ok, val = _get_by_path(row, field)
                    if not ok:
                        last_reason = f"missing_field: {field}"
                        break
                    if val is None:
                        last_reason = f"field_is_none: {field}"
                        break
                else:
                    return VerifyResult(
                        status="pass",
                        reason="matched",
                        baseline_since_id=baseline_since_id,
                        matched_event=row,
                        sampled_events=sampled,
                    )

        time.sleep(poll_interval_sec)

    return VerifyResult(
        status="fail",
        reason=last_reason or "timeout_waiting_event",
        baseline_since_id=baseline_since_id,
        matched_event=None,
        sampled_events=sampled,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="手机手动操作 + 自动校验埋点 (/events)")
    p.add_argument("--base-url", default="http://127.0.0.1:8888")
    p.add_argument("--env", default="uat", help="test/uat/production")
    p.add_argument("--device-id", required=True)
    p.add_argument("--event", required=True, help="期望 event_name")
    p.add_argument("--require", default="", help="必填字段路径，逗号分隔，如 content.start_type,content.button_id")
    p.add_argument("--timeout", type=int, default=25, help="总等待秒数")
    p.add_argument("--poll", type=float, default=1.5, help="轮询间隔秒")
    p.add_argument("--request-timeout", type=int, default=10, help="单次请求超时秒")
    p.add_argument("--baseline", type=int, default=-1, help="since_id 基线；-1 表示自动取当前最大 id")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    required_fields = [x.strip() for x in args.require.split(",") if x.strip()]

    baseline = args.baseline
    if baseline < 0:
        baseline = get_baseline_since_id(base_url=args.base_url, env=args.env, device_id=args.device_id)

    print(json.dumps({"phase": "baseline", "since_id": baseline}, ensure_ascii=False))
    print("现在去手机上完成一次对应操作…")

    res = verify_once(
        base_url=args.base_url,
        env=args.env,
        device_id=args.device_id,
        event_name=args.event,
        required_fields=required_fields,
        timeout_sec=args.timeout,
        poll_interval_sec=args.poll,
        request_timeout_sec=args.request_timeout,
        baseline_since_id=baseline,
    )

    out = {
        "status": res.status,
        "reason": res.reason,
        "baseline_since_id": res.baseline_since_id,
        "matched_event_id": (res.matched_event or {}).get("id"),
        "matched_event_name": (res.matched_event or {}).get("event_name"),
        "sampled_events": res.sampled_events,
    }
    print(json.dumps(out, ensure_ascii=False))
    return 0 if res.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

