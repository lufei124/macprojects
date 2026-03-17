"""公共配置模块"""
import os

from dotenv import load_dotenv


load_dotenv()


def _get_env(name, default=None, required=False):
    value = os.getenv(name, default)
    if value is not None:
        value = value.strip()
    if required and (value is None or value == ""):
        raise ValueError(f"缺少必需环境变量: {name}（不能为空，请在 .env 中填写）")
    return value or default


# ================= 数据库配置 =================
DB_HOST = _get_env("DB_HOST", required=True)
DB_PORT = int(_get_env("DB_PORT", "3306"))
DB_USER = _get_env("DB_USER", required=True)
DB_PASS = _get_env("DB_PASS", required=True)
DB_NAME = _get_env("DB_NAME", required=True)


# ================= 渠道列表 =================
TARGET_CHANNELS = [ch.strip() for ch in _get_env(
    "TARGET_CHANNELS",
    "200001,200002,200008,200009,200011",
).split(",") if ch.strip()]


# ================= 飞书配置 =================
# 这些配置均来源于飞书开放平台和具体的电子表格
FEISHU_APP_ID = _get_env("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = _get_env("FEISHU_APP_SECRET", "")
FEISHU_SHEET_TOKEN = _get_env("FEISHU_SHEET_TOKEN", "")
FEISHU_SHEET_ID_DAILY = _get_env("FEISHU_SHEET_ID_DAILY", "")
FEISHU_SHEET_ID_CHANNEL = _get_env("FEISHU_SHEET_ID_CHANNEL", "")

# 每日表行号映射配置：假定日期连续递增
# 例如：基准日期 2024-10-01 位于 A2，则：
#  - FEISHU_DAILY_BASE_DATE=2024-10-01
#  - FEISHU_DAILY_BASE_ROW=2
FEISHU_DAILY_BASE_DATE = _get_env("FEISHU_DAILY_BASE_DATE", "2024-10-01")
FEISHU_DAILY_BASE_ROW = int(_get_env("FEISHU_DAILY_BASE_ROW", "2"))

# 渠道表行号映射配置：假定每个自然日按渠道分块、行号连续
# 例如：基准日期 2025-02-01 的第一个渠道位于 A2，则：
#  - FEISHU_CHANNEL_BASE_DATE=2025-02-01
#  - FEISHU_CHANNEL_BASE_ROW=2
FEISHU_CHANNEL_BASE_DATE = _get_env("FEISHU_CHANNEL_BASE_DATE", "2025-02-01")
FEISHU_CHANNEL_BASE_ROW = int(_get_env("FEISHU_CHANNEL_BASE_ROW", "2"))
