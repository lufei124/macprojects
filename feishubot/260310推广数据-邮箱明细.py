#!/usr/bin/env python3
"""从 MySQL 查询 generate_lead 数据，增量写入飞书电子表格（按 邮箱+创建时间 去重）"""

import os
import sys
import pymysql
import requests
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

# ── 飞书配置 ──
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET")
WIKI_TOKEN = os.getenv("WIKI_TOKEN", "M9ypwLfK5i4wA2k6muec5OLHnUj")
SHEET_ID = os.getenv("SHEET_ID", "H8jrA5")
FEISHU_BASE = "https://open.feishu.cn/open-apis"

# ── 数据库配置 ──
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
}


# ═══════════════════════════════════════════════════════
#  飞书 API
# ═══════════════════════════════════════════════════════

EXCEL_EPOCH = datetime(1899, 12, 30)


def _headers(token):
    return {"Authorization": f"Bearer {token}"}


def _extract_email(cell):
    """从飞书单元格提取邮箱（可能是纯文本或富文本链接对象）"""
    if isinstance(cell, list):
        for item in cell:
            if isinstance(item, dict) and item.get("text"):
                return str(item["text"]).strip().lower()
        return ""
    return str(cell).strip().lower() if cell else ""


def _normalize_time(val):
    """统一时间格式：Excel 序列号或各种日期字符串 → YYYY/MM/DD HH:MM:SS"""
    val = str(val).strip()
    try:
        serial = float(val)
        if 30000 < serial < 60000:
            dt = EXCEL_EPOCH + timedelta(days=serial)
            return dt.replace(microsecond=0).strftime("%Y/%m/%d %H:%M:%S")
    except ValueError:
        pass
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y/%m/%d %H:%M:%S")
        except ValueError:
            continue
    return val


def get_tenant_access_token():
    """通过 app_id / app_secret 获取 tenant_access_token"""
    resp = requests.post(
        f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
    return data["tenant_access_token"]


def get_spreadsheet_token(token):
    """通过 wiki token 获取内嵌电子表格的真实 spreadsheet_token"""
    resp = requests.get(
        f"{FEISHU_BASE}/wiki/v2/spaces/get_node",
        params={"token": WIKI_TOKEN},
        headers=_headers(token),
    )
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 wiki 节点失败: {data}")
    node = data["data"]["node"]
    print(f"      节点类型: {node.get('obj_type')}, obj_token: {node.get('obj_token')}")
    return node["obj_token"]


def read_existing_keys(token, spreadsheet_token):
    """读取表格中已有的 (邮箱, 创建时间) 组合，用于增量去重"""
    range_str = f"{SHEET_ID}!A:D"
    resp = requests.get(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}",
        headers=_headers(token),
        params={"valueRenderOption": "ToString"},
    )
    data = resp.json()
    if data.get("code") != 0:
        print(f"      ⚠ 读取表格返回: {data}")
        return set()

    keys = set()
    rows = data.get("data", {}).get("valueRange", {}).get("values") or []
    for row in rows[1:]:  # 跳过表头
        if len(row) >= 4 and row[0] and row[3]:
            email = _extract_email(row[0])
            create_time = str(row[3]).strip()
            if email and create_time:
                keys.add((email, create_time))
    return keys


def append_rows(token, spreadsheet_token, rows):
    """将新行追加到电子表格末尾"""
    BATCH_SIZE = 4000
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        resp = requests.post(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values_append",
            headers={**_headers(token), "Content-Type": "application/json"},
            params={"insertDataOption": "INSERT_ROWS"},
            json={"valueRange": {"range": f"{SHEET_ID}!A:H", "values": batch}},
        )
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"写入飞书表格失败: {data}")
        print(f"      批次 {i // BATCH_SIZE + 1}: 写入 {len(batch)} 行")


def set_date_column_text_format(token, spreadsheet_token):
    """将 D 列（创建时间）设为文本格式，防止飞书把日期字符串转成序列号"""
    resp = requests.put(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/style",
        headers={**_headers(token), "Content-Type": "application/json"},
        json={
            "appendStyle": {
                "range": f"{SHEET_ID}!D:D",
                "style": {"formatter": "@"},
            }
        },
    )
    data = resp.json()
    if data.get("code") != 0:
        print(f"      ⚠ 设置列格式返回: {data}")
    else:
        print("      D 列已设为文本格式")


# ═══════════════════════════════════════════════════════
#  MySQL 查询
# ═══════════════════════════════════════════════════════

QUERY_SQL = """
SELECT
    DATE(create_time),
    JSON_UNQUOTE(JSON_EXTRACT(properties, '$.email'))    AS email,
    JSON_UNQUOTE(JSON_EXTRACT(properties, '$.country'))  AS country,
    JSON_UNQUOTE(JSON_EXTRACT(properties, '$.Platform')) AS platform,
    utm_source, ip, ua, url
FROM rsl_feedback
WHERE event_name = 'generate_lead'
  AND DATE(create_time) BETWEEN %s AND %s
ORDER BY create_time DESC
"""


def query_leads(target_date: date):
    """查询指定日期的 generate_lead 数据"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY_SQL, (target_date, target_date))
            result = []
            for row in cur.fetchall():
                # SQL 返回顺序: create_date, email, country, platform, utm_source, ip, ua, url
                # 写入 sheet 顺序: email, country, platform, create_date, utm_source, ip, ua, url
                converted = []
                for i, v in enumerate(row):
                    if v is None:
                        converted.append("")
                    elif i == 0 and hasattr(v, 'strftime'):
                        converted.append(v.strftime("%-Y/%-m/%-d"))
                    else:
                        converted.append(str(v))
                # 重排为 sheet 列顺序
                result.append([
                    converted[1], converted[2], converted[3], converted[0],
                    converted[4], converted[5], converted[6], converted[7],
                ])
            return result
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════

def main():
    # 解析目标日期（默认当天）
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        target = date.today()

    print(f"[1/6] 目标日期: {target}")

    print("[2/6] 获取飞书访问令牌...")
    token = get_tenant_access_token()
    print("      OK")

    print("[3/6] 解析 Wiki 中的电子表格 token...")
    spreadsheet_token = get_spreadsheet_token(token)

    print("[4/6] 读取飞书表格已有记录...")
    existing = read_existing_keys(token, spreadsheet_token)
    print(f"      已有 {len(existing)} 条 (邮箱+创建时间) 记录")

    print("[5/6] 查询 MySQL 数据...")
    rows = query_leads(target)
    print(f"      查询到 {len(rows)} 条记录")

    # 增量过滤：邮箱+创建时间 组合不存在于表格中的才写入
    new_rows = []
    for r in rows:
        email = r[0].strip().lower() if r[0] else ""
        create_time = r[3].strip() if r[3] else ""
        if (email, create_time) not in existing:
            new_rows.append(r)

    print(f"      其中新增 {len(new_rows)} 条")

    if new_rows:
        print("[6/6] 写入飞书表格...")
        set_date_column_text_format(token, spreadsheet_token)
        append_rows(token, spreadsheet_token, new_rows)
        print(f"      完成! 共写入 {len(new_rows)} 行")
    else:
        print("[6/6] 无新增数据，跳过写入")
        print("      完成!")


if __name__ == "__main__":
    main()
