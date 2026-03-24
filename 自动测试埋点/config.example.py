import os
from dotenv import load_dotenv

load_dotenv()


def _load_env_config(prefix: str, display_name: str) -> dict:
    """从环境变量加载指定前缀的数据库配置"""
    return {
        "name": display_name,
        "host": os.getenv(f"{prefix}_DB_HOST", ""),
        "port": int(os.getenv(f"{prefix}_DB_PORT", "3306")),
        "user": os.getenv(f"{prefix}_DB_USER", ""),
        "password": os.getenv(f"{prefix}_DB_PASSWORD", ""),
        "database": os.getenv(f"{prefix}_DB_DATABASE", ""),
        "table": os.getenv(f"{prefix}_DB_TABLE", "event_logs"),
    }


_test_cfg = _load_env_config("TEST", "测试环境")
_uat_cfg = _load_env_config("UAT", "UAT环境")

DB_CONFIGS = {
    "test": _test_cfg,
    "uat": {
        "name": "UAT环境",
        "host": _uat_cfg["host"] or _test_cfg["host"],
        "port": int(os.getenv("UAT_DB_PORT") or _test_cfg["port"]),
        "user": _uat_cfg["user"] or _test_cfg["user"],
        "password": _uat_cfg["password"] or _test_cfg["password"],
        "database": _uat_cfg["database"] or "restart_life_uat",
        "table": _uat_cfg["table"] or _test_cfg["table"],
    },
    "production": _load_env_config("PROD", "线上环境"),
}
DEFAULT_ENV = "test"
