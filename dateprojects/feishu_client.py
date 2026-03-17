"""飞书机器人与电子表格客户端封装"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Tuple

import requests

from config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    FEISHU_SHEET_TOKEN,
    FEISHU_DAILY_BASE_DATE,
    FEISHU_DAILY_BASE_ROW,
)
from logger import get_logger

logger = get_logger(__name__)

_TOKEN_CACHE: Dict[str, Tuple[str, float]] = {}


class FeishuConfigError(RuntimeError):
    """飞书配置缺失或错误"""


def _ensure_basic_config():
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise FeishuConfigError("FEISHU_APP_ID / FEISHU_APP_SECRET 未正确配置，请检查 .env")
    if not FEISHU_SHEET_TOKEN:
        raise FeishuConfigError("FEISHU_SHEET_TOKEN 未配置，无法定位目标电子表格")


def get_tenant_access_token() -> str:
    """获取并缓存 tenant_access_token"""
    _ensure_basic_config()

    cache_key = "tenant_access_token"
    cached = _TOKEN_CACHE.get(cache_key)
    now = time.time()
    if cached and cached[1] > now:
        return cached[0]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")

    token = data["tenant_access_token"]
    expire = now + max(int(data.get("expire", 7000)) - 60, 60)
    _TOKEN_CACHE[cache_key] = (token, expire)
    return token


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_tenant_access_token()}",
        "Content-Type": "application/json; charset=utf-8",
    }


def read_sheet_values(
    sheet_token: str,
    range_a1: str,
    value_render_option: str = "ToString",
    date_time_render_option: str = "FormattedString",
) -> List[List[str]]:
    """读取指定 A1 区域的数据。

    使用 ToString + FormattedString，使日期列返回格式化字符串（如 2025-02-01），
    便于与业务侧日期匹配；否则日期会返回 Excel 序列数，导致 (日期, 渠道) 索引无法匹配。
    参考：https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/reading-a-single-range
    """
    from urllib.parse import quote

    encoded_range = quote(range_a1, safe="")
    url = (
        f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values/{encoded_range}"
        f"?valueRenderOption={value_render_option}&dateTimeRenderOption={date_time_render_option}"
    )
    resp = requests.get(url, headers=_headers(), timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"读取表格失败: {data}")
    value_range = data.get("data", {}).get("valueRange", {})
    return value_range.get("values", []) or []


def write_sheet_values(
    sheet_token: str,
    range_a1: str,
    values: List[List[object]],
) -> None:
    """向单个范围写入数据（覆盖模式）"""
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values"
    payload = {
        "valueRange": {
            "range": range_a1,
            "values": values,
        }
    }
    resp = requests.put(url, headers=_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"写入表格失败: {data}")


def append_rows(
    sheet_token: str,
    sheet_id: str,
    start_row_index: int,
    rows: List[List[object]],
) -> None:
    """在指定 sheet 尾部追加行

    注意：Feishu Sheets 没有纯“append”接口，这里策略是：
    - 调用 write_sheet_values，目标 range 为从 start_row_index 开始的区域
    - 调用方需要自己计算 start_row_index（例如：现有行数 + 1）
    """
    end_row = start_row_index + len(rows) - 1
    # 假设列从 A 开始，调用方应保证 rows 中列数与表头一致
    last_col_index = len(rows[0]) - 1
    end_col = _index_to_col_letter(last_col_index)
    range_a1 = f"{sheet_id}!A{start_row_index}:{end_col}{end_row}"
    write_sheet_values(sheet_token, range_a1, rows)


def build_date_index(
    existing_rows: List[List[str]],
    date_col_index: int,
) -> Dict[str, int]:
    """根据日期列构建索引: date_str -> row_number(1-based)"""
    index: Dict[str, int] = {}
    for i, row in enumerate(existing_rows, start=1):
        if len(row) <= date_col_index:
            continue
        date_val = row[date_col_index]
        norm = _normalize_date(date_val)
        if norm:
            index[norm] = i
    return index


def build_date_channel_index(
    existing_rows: List[List[str]],
    date_col_index: int,
    channel_col_index: int,
) -> Dict[Tuple[str, str], int]:
    """根据 (日期, 渠道) 构建索引"""
    index: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(existing_rows, start=1):
        if len(row) <= max(date_col_index, channel_col_index):
            continue
        date_val = row[date_col_index]
        ch_val = row[channel_col_index]
        if date_val and ch_val:
            index[(str(date_val), str(ch_val))] = i
    return index


def _index_to_col_letter(idx: int) -> str:
    """0-based 列索引转换为 Excel 风格列名（支持到 ZZ）"""
    idx = int(idx)
    result = []
    while idx >= 0:
        idx, rem = divmod(idx, 26)
        result.append(chr(ord("A") + rem))
        idx -= 1
    return "".join(reversed(result))


def upsert_rows_by_date(
    sheet_token: str,
    sheet_id: str,
    data_rows: List[List[object]],
    date_col_index: int = 0,
    max_rows: int = 10000,
) -> None:
    """按“日期列”做 upsert：存在则覆盖该行，不存在则在表尾追加

    约定：
    - 第 1 行是表头，从第 2 行开始是数据行
    - date_col_index 指的是 data_rows 中日期的列索引
    """
    if not data_rows:
        return

    # 读取表头（仅第 1 行），用于推断列数
    header_values = read_sheet_values(sheet_token, f"{sheet_id}!A1:ZZ1")
    header = header_values[0] if header_values else []
    num_cols = max(len(header), len(data_rows[0]))
    end_col_letter = _index_to_col_letter(num_cols - 1)

    # 只读取“日期列”数据用于构建索引（避免整表过大）
    date_col_letter = _index_to_col_letter(date_col_index)
    existing_dates_col = read_sheet_values(
        sheet_token,
        f"{sheet_id}!{date_col_letter}2:{date_col_letter}{max_rows}",
    )
    # existing_dates_col 是形如 [[日期1],[日期2],...] 的二维数组
    date_index = build_date_index(existing_dates_col, 0)

    # 记录需要 append 的行
    append_bucket: List[List[object]] = []

    for row in data_rows:
        if len(row) <= date_col_index:
            continue
        date_val = row[date_col_index]
        date_str = _normalize_date(date_val)
        if not date_str:
            continue
        if date_str in date_index:
            # 已存在：覆盖对应行（existing_dates 中第 i=1 的行对应 sheet 的第 2 行）
            rel_index = date_index[date_str]  # 1-based in existing_dates
            target_row = 1 + rel_index  # 加上表头行偏移
            range_a1 = f"{sheet_id}!A{target_row}:{end_col_letter}{target_row}"
            write_sheet_values(sheet_token, range_a1, [row])
        else:
            # 不存在：稍后 append
            append_bucket.append(row)

    if append_bucket:
        # 追加行，从当前已有“日期行数”之后开始
        start_row = 2 + len(existing_dates_col)
        append_rows(sheet_token, sheet_id, start_row, append_bucket)


def upsert_metric_by_date(
    sheet_token: str,
    sheet_id: str,
    data_rows: List[List[object]],
    date_col_index_in_rows: int,
    metric_col_index_in_rows: int,
    metric_header: str,
    max_rows: int = 10000,
) -> None:
    """按日期仅更新单个指标列（其它列不动）

    - data_rows: 由报表代码生成的二维数组（第一列为日期，第二列为指标等）
    - metric_header: 飞书表头中对应这一列的中文名称，例如 "全部新增启动人数（仅app）"
    """
    if not data_rows:
        return

    # 读取首行表头，基于中文表头名定位“指标列”
    header_values = read_sheet_values(sheet_token, f"{sheet_id}!A1:ZZ1")
    header = header_values[0] if header_values else []

    try:
        metric_col_index_in_sheet = header.index(metric_header)
    except ValueError:
        logger.warning("在飞书表头中找不到指标列: %s", metric_header)
        return

    metric_col_letter = _index_to_col_letter(metric_col_index_in_sheet)

    # 根据配置的基准日期 / 起始行，按天数偏移直接算出目标行号
    try:
        base_date = datetime.strptime(FEISHU_DAILY_BASE_DATE, "%Y-%m-%d")
    except ValueError:
        logger.error("FEISHU_DAILY_BASE_DATE 配置格式错误，应为 YYYY-MM-DD，当前为: %s", FEISHU_DAILY_BASE_DATE)
        return
    base_row = FEISHU_DAILY_BASE_ROW

    for row in data_rows:
        if len(row) <= max(date_col_index_in_rows, metric_col_index_in_rows):
            continue
        date_val = row[date_col_index_in_rows]
        metric_val = row[metric_col_index_in_rows]
        date_str = _normalize_date(date_val)
        if not date_str:
            continue

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue

        offset_days = (dt - base_date).days
        if offset_days < 0:
            # 早于基准日期，跳过
            continue

        target_row = base_row + offset_days
        range_a1 = f"{sheet_id}!{metric_col_letter}{target_row}:{metric_col_letter}{target_row}"
        write_sheet_values(sheet_token, range_a1, [[metric_val]])

def upsert_rows_by_date_channel(
    sheet_token: str,
    sheet_id: str,
    data_rows: List[List[object]],
    date_col_index: int = 0,
    channel_col_index: int = 1,
    max_rows: int = 10000,
) -> None:
    """按“日期+渠道”做 upsert：存在则覆盖该行，不存在则在表尾追加

    约定：
    - 第 1 行是表头，从第 2 行开始是数据行
    - date_col_index / channel_col_index 指的是 data_rows 中对应列索引
    """
    if not data_rows:
        return

    # 读取表头（仅第 1 行），用于推断列数
    header_values = read_sheet_values(sheet_token, f"{sheet_id}!A1:ZZ1")
    header = header_values[0] if header_values else []
    num_cols = max(len(header), len(data_rows[0]))
    end_col_letter = _index_to_col_letter(num_cols - 1)

    # 只读取“日期列 + 渠道列”两列数据，用于构建索引
    date_col_letter = _index_to_col_letter(date_col_index)
    channel_col_letter = _index_to_col_letter(channel_col_index)
    existing_date_col = read_sheet_values(
        sheet_token,
        f"{sheet_id}!{date_col_letter}2:{date_col_letter}{max_rows}",
    )
    existing_channel_col = read_sheet_values(
        sheet_token,
        f"{sheet_id}!{channel_col_letter}2:{channel_col_letter}{max_rows}",
    )

    # 将两列组合成“行”结构（每行只有 日期 和 渠道 两个字段）
    max_len = max(len(existing_date_col), len(existing_channel_col))
    combined_rows: List[List[str]] = []
    for i in range(max_len):
        date_val = existing_date_col[i][0] if i < len(existing_date_col) and existing_date_col[i] else ""
        ch_val = (
            existing_channel_col[i][0]
            if i < len(existing_channel_col) and existing_channel_col[i]
            else ""
        )
        combined_rows.append([date_val, ch_val])

    key_index = build_date_channel_index(combined_rows, 0, 1)

    append_bucket: List[List[object]] = []

    for row in data_rows:
        if len(row) <= max(date_col_index, channel_col_index):
            continue
        date_val = row[date_col_index]
        ch_val = row[channel_col_index]
        if not date_val or not ch_val:
            continue

        key = (_normalize_date(date_val), str(ch_val))
        if key in key_index:
            rel_index = key_index[key]  # 1-based in existing
            target_row = 1 + rel_index  # 加上表头
            range_a1 = f"{sheet_id}!A{target_row}:{end_col_letter}{target_row}"
            write_sheet_values(sheet_token, range_a1, [row])
        else:
            append_bucket.append(row)

    if append_bucket:
        # 现有数据行数 = 已组合的 (日期, 渠道) 行数
        start_row = 2 + len(combined_rows)
        append_rows(sheet_token, sheet_id, start_row, append_bucket)


def _build_channel_sheet_index(
    sheet_token: str,
    sheet_id: str,
    date_col_letter: str = "A",
    channel_col_letter: str = "C",
    max_rows: int = 10000,
) -> Tuple[Dict[Tuple[str, str], int], int]:
    """从渠道 Sheet 读取日期列 + 渠道列，构建 (日期, 渠道号) -> 表内行号（2-based）的索引。

    约定：第 1 行为表头，数据从第 2 行开始。返回的索引值为表内实际行号（2-based），
    可直接用于 range_a1 如 \"Sheet!D{row}\"。
    """
    existing_date_col = read_sheet_values(
        sheet_token,
        f"{sheet_id}!{date_col_letter}2:{date_col_letter}{max_rows}",
    )
    existing_channel_col = read_sheet_values(
        sheet_token,
        f"{sheet_id}!{channel_col_letter}2:{channel_col_letter}{max_rows}",
    )
    max_len = max(len(existing_date_col), len(existing_channel_col))
    combined_rows: List[List[str]] = []
    for i in range(max_len):
        date_val = (
            existing_date_col[i][0]
            if i < len(existing_date_col) and existing_date_col[i]
            else ""
        )
        ch_val = (
            existing_channel_col[i][0]
            if i < len(existing_channel_col) and existing_channel_col[i]
            else ""
        )
        combined_rows.append([date_val, ch_val])

    # 用归一化日期构建索引，便于与 data_rows 中的日期匹配
    index_1based: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(combined_rows):
        if len(row) < 2:
            continue
        norm = _normalize_date(row[0])
        ch = str(row[1]).strip() if row[1] else ""
        if norm and ch:
            index_1based[(norm, ch)] = i + 1  # 1-based 数据行序号

    # 转为表内行号（2-based）：第 1 条数据行 = 2
    sheet_index: Dict[Tuple[str, str], int] = {
        k: 1 + v for k, v in index_1based.items()
    }
    return sheet_index, len(combined_rows)


def upsert_metric_by_date_channel(
    sheet_token: str,
    sheet_id: str,
    data_rows: List[List[object]],
    date_col_index_in_rows: int,
    channel_col_index_in_rows: int,
    metric_col_index_in_rows: int,
    metric_header: str,
) -> None:
    """按 (日期, 渠道) 仅更新单个指标列（其它列不动）。

    先根据表内 A 列日期 + C 列渠道号构建 (日期, 渠道号) -> 行号 索引，再对命中的行写入
    对应 metric_header 列；未在表内出现的 (日期, 渠道) 跳过并打日志。
    """
    if not data_rows:
        return

    # 读取首行表头，基于中文表头名定位“指标列”
    header_values = read_sheet_values(sheet_token, f"{sheet_id}!A1:ZZ1")
    header = header_values[0] if header_values else []

    try:
        metric_col_index_in_sheet = header.index(metric_header)
    except ValueError:
        logger.warning("在飞书渠道表头中找不到指标列: %s", metric_header)
        return

    metric_col_letter = _index_to_col_letter(metric_col_index_in_sheet)

    # 根据表内 A 列 + C 列构建 (日期, 渠道号) -> 表内行号
    sheet_index, _ = _build_channel_sheet_index(
        sheet_token, sheet_id, date_col_letter="A", channel_col_letter="C"
    )

    written = 0
    skipped = 0
    for row in data_rows:
        if len(row) <= max(
            date_col_index_in_rows, channel_col_index_in_rows, metric_col_index_in_rows
        ):
            continue

        date_val = row[date_col_index_in_rows]
        channel_val = str(row[channel_col_index_in_rows]).strip()
        metric_val = row[metric_col_index_in_rows]

        if not date_val or not channel_val:
            continue

        date_str = _normalize_date(date_val)
        if not date_str:
            continue

        key = (date_str, channel_val)
        if key not in sheet_index:
            skipped += 1
            continue

        target_row = sheet_index[key]
        range_a1 = f"{sheet_id}!{metric_col_letter}{target_row}:{metric_col_letter}{target_row}"
        write_sheet_values(sheet_token, range_a1, [[metric_val]])
        written += 1

    if skipped:
        logger.debug(
            "渠道指标 %s: 表内未匹配 (日期,渠道) 共 %s 条已跳过",
            metric_header,
            skipped,
        )


def _normalize_date(value) -> str:
    """将各种日期表示统一为 YYYY-MM-DD 字符串，用于匹配"""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # 简单兜底：如果前 10 位形如 YYYY-MM-DD 或 YYYY/MM/DD，则只取前 10 位再替换 '/'
    if len(s) >= 10 and s[4] in "-/" and s[7] in "-/":
        core = s[:10].replace("/", "-")
        return core

    return s

