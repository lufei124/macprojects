from __future__ import annotations

import time
from typing import Any, Dict, Iterable

from appium.webdriver.common.appiumby import AppiumBy
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait


LOCATOR_BY_MAP = {
    "accessibility_id": AppiumBy.ACCESSIBILITY_ID,
    "id": AppiumBy.ID,
    "xpath": AppiumBy.XPATH,
    "ios_predicate": AppiumBy.IOS_PREDICATE,
    "ios_class_chain": AppiumBy.IOS_CLASS_CHAIN,
}


class ActionError(RuntimeError):
    pass


def run_steps(driver: WebDriver, steps: Iterable[Dict[str, Any]]) -> None:
    for index, step in enumerate(steps, start=1):
        action = step.get("action")
        if action == "wait_for":
            _wait_for(driver, step)
        elif action == "tap":
            _tap(driver, step)
        elif action == "input":
            _input(driver, step)
        elif action == "sleep":
            _sleep(driver, step)
        elif action == "terminate_app":
            _terminate_app(driver, step)
        elif action == "activate_app":
            _activate_app(driver, step)
        else:
            raise ActionError(f"Unsupported action at step {index}: {action}")


def _get_locator(step: Dict[str, Any]) -> tuple[str, str]:
    by_raw = str(step.get("by", "")).strip()
    value = str(step.get("value", "")).strip()
    by = LOCATOR_BY_MAP.get(by_raw)
    if not by or not value:
        raise ActionError(f"Invalid locator config: by={by_raw}, value={value}")
    return by, value


def _wait_for(driver: WebDriver, step: Dict[str, Any]) -> None:
    timeout = int(step.get("timeout_sec", 15))
    by, value = _get_locator(step)
    WebDriverWait(driver, timeout).until(ec.presence_of_element_located((by, value)))


def _tap(driver: WebDriver, step: Dict[str, Any]) -> None:
    timeout = int(step.get("timeout_sec", 15))
    by, value = _get_locator(step)
    element = WebDriverWait(driver, timeout).until(ec.element_to_be_clickable((by, value)))
    element.click()


def _input(driver: WebDriver, step: Dict[str, Any]) -> None:
    timeout = int(step.get("timeout_sec", 15))
    text = str(step.get("text", ""))
    by, value = _get_locator(step)
    element = WebDriverWait(driver, timeout).until(ec.presence_of_element_located((by, value)))
    clear_first = bool(step.get("clear_first", True))
    if clear_first:
        element.clear()
    element.send_keys(text)


def _sleep(driver: WebDriver, step: Dict[str, Any]) -> None:
    ms = int(step.get("ms", 1000))
    time.sleep(max(ms, 0) / 1000.0)


def _terminate_app(driver: WebDriver, step: Dict[str, Any]) -> None:
    bundle_id = str(step.get("bundle_id", "")).strip()
    if not bundle_id:
        raise ActionError("terminate_app requires bundle_id")
    driver.terminate_app(bundle_id)


def _activate_app(driver: WebDriver, step: Dict[str, Any]) -> None:
    bundle_id = str(step.get("bundle_id", "")).strip()
    if not bundle_id:
        raise ActionError("activate_app requires bundle_id")
    driver.activate_app(bundle_id)

