from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml
from appium import webdriver
try:
    # Appium-Python-Client >= 5
    from appium.options.xcuitest import XCUITestOptions  # type: ignore
except ModuleNotFoundError:
    # Appium-Python-Client 4.x
    from appium.options.ios import XCUITestOptions  # type: ignore

from ios_automation.actions import ActionError, run_steps
from ios_automation.assertions import EventClient


REQUIRED_CAP_KEYS = [
    "platformName",
    "appium:automationName",
    "appium:udid",
    "appium:bundleId",
]


@dataclass
class CaseResult:
    case_id: str
    status: str
    reason: str
    details: Dict[str, Any]


def load_cases(path: Path) -> List[Dict[str, Any]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("cases.yaml must contain a non-empty `cases` list")
    return cases


def build_driver(server_url: str, capabilities: Dict[str, Any]) -> webdriver.Remote:
    options = XCUITestOptions()
    for key, value in capabilities.items():
        options.set_capability(key, value)
    return webdriver.Remote(server_url, options=options)


def run_case(driver: webdriver.Remote, event_client: EventClient, case: Dict[str, Any], env: str, device_id: str) -> CaseResult:
    case_id = str(case.get("id", "unknown"))
    event_name = str(case.get("expect_event", "")).strip()
    required_fields = case.get("required_fields") or []
    timeout_sec = int(case.get("timeout_sec", 20))
    steps = case.get("steps") or []

    if not event_name:
        return CaseResult(case_id=case_id, status="fail", reason="expect_event is required", details={})

    baseline = event_client.get_baseline_id(env=env, device_id=device_id)

    try:
        run_steps(driver, steps)
    except ActionError as e:
        return CaseResult(
            case_id=case_id,
            status="fail",
            reason=f"ui_action_failed: {e}",
            details={"classification": "UI_ACTION_FAILED"},
        )

    result = event_client.wait_for_event(
        env=env,
        device_id=device_id,
        event_name=event_name,
        required_fields=required_fields,
        timeout_sec=timeout_sec,
        since_id=baseline,
    )

    if result.status == "pass":
        return CaseResult(
            case_id=case_id,
            status="pass",
            reason="matched",
            details={
                "classification": "PASS",
                "baseline_since_id": baseline,
                "matched_event_id": result.matched_event.get("id") if result.matched_event else None,
            },
        )

    reason = result.reason
    classification = "EVENT_TIMEOUT"
    if "missing required field" in reason or "required field is None" in reason:
        classification = "FIELD_ASSERTION_FAILED"
    elif "HTTP" in reason or "Connection" in reason:
        classification = "EVENT_QUERY_FAILED"

    return CaseResult(
        case_id=case_id,
        status="fail",
        reason=reason,
        details={
            "classification": classification,
            "baseline_since_id": baseline,
            "sampled_events": result.sampled_events,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="iOS 自动化埋点冒烟 Runner")
    parser.add_argument("--cases", default="ios_automation/cases.yaml", help="用例配置文件路径")
    parser.add_argument("--event-base-url", default=os.getenv("EVENT_BASE_URL", "http://127.0.0.1:8888"))
    parser.add_argument("--env", default=os.getenv("TRACKING_ENV", "test"))
    parser.add_argument("--device-id", default=os.getenv("TRACKING_DEVICE_ID", ""), help="埋点查询 device_id")
    parser.add_argument("--appium-server", default=os.getenv("APPIUM_SERVER_URL", "http://127.0.0.1:4723"))
    parser.add_argument("--capabilities", default=os.getenv("IOS_CAPABILITIES_JSON", "{}"))
    parser.add_argument("--case-id", default="", help="仅执行指定 case id")
    return parser.parse_args()


def validate_capabilities(caps: Dict[str, Any]) -> tuple[bool, str]:
    missing = [k for k in REQUIRED_CAP_KEYS if not str(caps.get(k, "")).strip()]
    if missing:
        return False, f"Missing required capability keys: {', '.join(missing)}"

    placeholder_keys = []
    for key, value in caps.items():
        s = str(value).strip()
        if s.startswith("<") and s.endswith(">"):
            placeholder_keys.append(key)
    if placeholder_keys:
        return False, f"Capability keys still using template placeholders: {', '.join(placeholder_keys)}"

    pv = str(caps.get("appium:platformVersion", "")).strip()
    if pv:
        parts = pv.split(".")
        if not all(p.isdigit() for p in parts):
            return False, "appium:platformVersion must be numeric like '17.4' (or remove this key)"

    return True, ""


def main() -> int:
    args = parse_args()
    if not args.device_id:
        print("TRACKING_DEVICE_ID/--device-id is required", file=sys.stderr)
        return 2

    try:
        capabilities = json.loads(args.capabilities)
    except json.JSONDecodeError as e:
        print(f"Invalid IOS_CAPABILITIES_JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(capabilities, dict):
        print("IOS_CAPABILITIES_JSON must be a JSON object", file=sys.stderr)
        return 2

    ok, err = validate_capabilities(capabilities)
    if not ok:
        print(f"Invalid IOS_CAPABILITIES_JSON: {err}", file=sys.stderr)
        return 2

    cases = load_cases(Path(args.cases))
    if args.case_id:
        cases = [c for c in cases if c.get("id") == args.case_id]
        if not cases:
            print(f"case-id not found: {args.case_id}", file=sys.stderr)
            return 2

    event_client = EventClient(args.event_base_url)
    driver = build_driver(args.appium_server, capabilities)
    try:
        results = [run_case(driver, event_client, case, args.env, args.device_id) for case in cases]
    finally:
        driver.quit()

    fail_count = 0
    for r in results:
        line = {
            "case_id": r.case_id,
            "status": r.status,
            "reason": r.reason,
            "details": r.details,
        }
        print(json.dumps(line, ensure_ascii=False))
        if r.status != "pass":
            fail_count += 1

    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())

