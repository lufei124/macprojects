from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template_string, request
import pymysql
import json

try:
    from config import DB_CONFIG
except ImportError:
    from config.example import DB_CONFIG  # type: ignore


app = Flask(__name__)


def get_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG.get("port", 3306),
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.DictCursor,
    )


@app.route("/health")
def health() -> Any:
    return jsonify({"status": "ok"})


@app.route("/events")
def events() -> Any:
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id is required"}), 400

    since_id_param = request.args.get("since_id")
    since_id: Optional[int] = None
    if since_id_param:
        try:
            since_id = int(since_id_param)
        except ValueError:
            since_id = None

    before_id_param = request.args.get("before_id")
    before_id: Optional[int] = None
    if before_id_param:
        try:
            before_id = int(before_id_param)
        except ValueError:
            before_id = None

    limit_param = request.args.get("limit", "100")
    try:
        limit = min(max(int(limit_param), 1), 500)
    except ValueError:
        limit = 100

    table = DB_CONFIG["table"]

    # 事件筛选：可选参数 event_name，支持逗号分隔多个事件名
    raw_event_names = request.args.get("event_name", "").strip()
    event_names: List[str] = []
    if raw_event_names:
        for part in raw_event_names.split(","):
            name = part.strip()
            if name:
                event_names.append(name)

    query = f"""
        SELECT
            *
        FROM {table}
        WHERE device_id = %s
    """
    params: List[Any] = [device_id]

    if event_names:
        placeholders = ",".join(["%s"] * len(event_names))
        query += f" AND event_name IN ({placeholders})"
        params.extend(event_names)

    if since_id is not None:
        query += " AND id > %s"
        params.append(since_id)

    if before_id is not None:
        query += " AND id < %s"
        params.append(before_id)

    query += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows: List[Dict[str, Any]] = cursor.fetchall()

    # 处理行数据：content 尝试解析 JSON；bytes 字段（如 bit(1)）转成整数，时间转成字符串
    processed: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        # 将所有 bytes 转成数字或字符串，避免 jsonify 报错
        for key, value in list(item.items()):
            if isinstance(value, bytes):
                # 对于 bit(1) 这类字段，转换为 0/1 的 int，更直观
                if len(value) == 1:
                    item[key] = int.from_bytes(value, byteorder="big")
                else:
                    item[key] = value.decode(errors="ignore")
            elif isinstance(value, datetime):
                # 统一时间字段序列化格式，避免 Flask 默认 RFC1123 格式导致前端/DB 对账不一致
                item[key] = value.isoformat()

        # JS Number 会对超大整数丢精度（> 2^53-1），这里统一转字符串，避免前端展示不准
        for big_int_key in ("role_id", "user_id"):
            v = item.get(big_int_key)
            if isinstance(v, int) and abs(v) > 9007199254740991:
                item[big_int_key] = str(v)

        raw_content = item.get("content")
        parsed_content: Any = raw_content
        if isinstance(raw_content, str):
            try:
                import json

                parsed_content = json.loads(raw_content)
            except Exception:
                parsed_content = raw_content
        item["content"] = parsed_content
        processed.append(item)

    return jsonify(processed)


INDEX_HTML = """
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <title>埋点实时查看工具</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 0;
        padding: 0;
        background: #f5f5f7;
        color: #111827;
      }
      .container {
        max-width: 1100px;
        margin: 0 auto;
        padding: 24px 16px 40px;
      }
      h1 {
        font-size: 24px;
        margin-bottom: 4px;
      }
      .subtitle {
        font-size: 13px;
        color: #6b7280;
        margin-bottom: 20px;
      }
      .card {
        background: #ffffff;
        border-radius: 12px;
        padding: 16px 16px 10px;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
        margin-bottom: 16px;
      }
      .form-row {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: flex-end;
      }
      .field {
        display: flex;
        flex-direction: column;
        min-width: 0;
      }
      .field label {
        font-size: 12px;
        color: #4b5563;
        margin-bottom: 4px;
      }
      .field input {
        border-radius: 8px;
        border: 1px solid #d1d5db;
        padding: 6px 10px;
        font-size: 13px;
        min-width: 0;
      }
      .field input:focus {
        outline: none;
        border-color: #2563eb;
        box-shadow: 0 0 0 1px #2563eb33;
      }
      .btn {
        border-radius: 999px;
        border: none;
        padding: 8px 18px;
        font-size: 13px;
        font-weight: 500;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 6px;
        transition: background 0.15s ease, box-shadow 0.15s ease, transform 0.08s ease;
      }
      .btn-primary {
        background: #2563eb;
        color: white;
        box-shadow: 0 8px 18px rgba(37, 99, 235, 0.35);
      }
      .btn-primary:hover {
        background: #1d4ed8;
        transform: translateY(-1px);
      }
      .btn-secondary {
        background: #e5e7eb;
        color: #111827;
      }
      .status-line {
        margin-top: 10px;
        font-size: 12px;
        color: #6b7280;
        display: flex;
        justify-content: space-between;
        gap: 12px;
      }
      .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 2px 8px;
        border-radius: 999px;
        background: #ecfdf3;
        color: #166534;
        font-size: 11px;
      }
      .status-dot {
        width: 7px;
        height: 7px;
        border-radius: 999px;
        background: #22c55e;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        table-layout: fixed;
      }
      thead {
        background: #f9fafb;
      }
      th,
      td {
        padding: 6px 8px;
        border-bottom: 1px solid #e5e7eb;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      th {
        text-align: left;
        font-weight: 500;
        color: #4b5563;
      }
      tbody tr:nth-child(even) {
        background: #f9fafb;
      }
      tbody tr.highlight {
        animation: highlight 1.5s ease-out;
      }
      @keyframes highlight {
        0% {
          background-color: #fef3c7;
        }
        100% {
          background-color: transparent;
        }
      }
      .pill {
        display: inline-flex;
        align-items: center;
        padding: 1px 6px;
        border-radius: 999px;
        background: #eff6ff;
        color: #1d4ed8;
        font-size: 11px;
      }
      .muted {
        color: #9ca3af;
        font-size: 11px;
      }
      .scroll-container {
        margin-top: 4px;
        max-height: 480px;
        overflow: auto;
        border-radius: 10px;
        border: 1px solid #e5e7eb;
        background: #ffffff;
      }
      pre {
        margin: 0;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 11px;
        white-space: pre-wrap;
        word-break: break-all;
      }
      @media (max-width: 720px) {
        .form-row {
          flex-direction: column;
          align-items: stretch;
        }
        .field,
        .btn {
          width: 100%;
        }
        .btn {
          justify-content: center;
        }
        details.picker {
          position: relative;
        }
        .picker-summary {
          cursor: pointer;
          user-select: none;
          padding: 10px 12px;
          border: 1px solid rgba(15, 23, 42, 0.12);
          border-radius: 12px;
          background: #fff;
          font-size: 14px;
          line-height: 1.2;
        }
        .picker-panel {
          margin-top: 10px;
          padding: 12px;
          border: 1px solid rgba(15, 23, 42, 0.12);
          border-radius: 12px;
          background: #fff;
        }
        .picker-actions {
          display: flex;
          gap: 10px;
          align-items: center;
          margin-bottom: 8px;
        }
        .picker-actions input {
          flex: 1;
          min-width: 180px;
        }
        .picker-list {
          max-height: none;
          overflow: visible;
          border: 1px solid rgba(15, 23, 42, 0.08);
          border-radius: 10px;
          padding: 4px;
          background: rgba(15, 23, 42, 0.02);
        }
        .picker-item {
          display: flex;
          gap: 8px;
          align-items: center;
          padding: 6px 8px;
          border-radius: 8px;
          cursor: pointer;
        }
        .picker-item:hover {
          background: rgba(59, 130, 246, 0.08);
        }
        .picker-item input {
          width: 16px;
          height: 16px;
        }
        .event-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 10px;
          padding: 10px;
        }
        @media (max-width: 720px) {
          .event-grid {
            grid-template-columns: 1fr;
          }
        }
        .event-card {
          border: 1px solid rgba(15, 23, 42, 0.10);
          border-radius: 14px;
          padding: 10px 10px 8px;
          background: #fff;
          display: flex;
          flex-direction: column;
          gap: 6px;
        }
        .event-card:hover {
          border-color: rgba(59, 130, 246, 0.35);
          box-shadow: 0 10px 18px rgba(15, 23, 42, 0.10);
        }
        .event-card-top {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 10px;
        }
        .event-check {
          display: inline-flex;
          gap: 8px;
          align-items: flex-start;
          cursor: pointer;
          user-select: none;
        }
        .event-check input {
          width: 18px;
          height: 18px;
          margin-top: 2px;
        }
        .event-name {
          font-weight: 650;
          color: #0f172a;
          line-height: 1.15;
        }
        .event-key {
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New",
            monospace;
          font-size: 12px;
          color: #0f172a;
          background: rgba(59, 130, 246, 0.10);
          border: 1px solid rgba(59, 130, 246, 0.16);
          border-radius: 10px;
          padding: 3px 8px;
          width: fit-content;
        }
        .event-meta {
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
          align-items: center;
        }
        .event-meta .tag {
          font-size: 12px;
          padding: 2px 8px;
          border-radius: 999px;
          border: 1px solid rgba(15, 23, 42, 0.12);
          background: rgba(15, 23, 42, 0.03);
          color: #0f172a;
        }
        .event-trigger {
          font-size: 12px;
          line-height: 1.25;
        }
        .props {
          display: flex;
          flex-direction: column;
          gap: 6px;
          margin-top: 8px;
          padding: 8px;
          border: 1px solid rgba(15, 23, 42, 0.10);
          border-radius: 12px;
          background: rgba(15, 23, 42, 0.02);
        }
        .prop {
          display: grid;
          grid-template-columns: 160px 120px 1fr;
          gap: 8px;
          align-items: start;
        }
        .prop-key {
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New",
            monospace;
          font-size: 12px;
          background: rgba(59, 130, 246, 0.10);
          color: #0f172a;
          border: 1px solid rgba(59, 130, 246, 0.18);
          border-radius: 10px;
          padding: 4px 8px;
          width: fit-content;
        }
        .prop-name {
          font-weight: 600;
          color: #0f172a;
        }
        .prop-desc {
          line-height: 1.3;
        }
        .picker-divider {
          height: 1px;
          margin: 6px 4px;
          background: rgba(15, 23, 42, 0.10);
        }
        .dropdown {
          position: relative;
        }
        .dropdown-input-row {
          display: flex;
          gap: 8px;
          align-items: center;
        }
        .dropdown-toggle-btn {
          white-space: nowrap;
          padding: 10px 12px;
        }
        .modal {
          position: fixed;
          inset: 0;
          z-index: 9999;
          display: none;
          align-items: flex-start;
          justify-content: center;
          padding: 10vh 16px 16px;
        }
        .modal-backdrop {
          position: absolute;
          inset: 0;
          background: rgba(15, 23, 42, 0.45);
          backdrop-filter: blur(2px);
        }
        .modal-card {
          position: relative;
          width: min(720px, 92vw);
          max-height: 80vh;
          overflow: hidden;
          border-radius: 16px;
          background: #fff;
          border: 1px solid rgba(15, 23, 42, 0.12);
          box-shadow: 0 24px 60px rgba(15, 23, 42, 0.30);
        }
        .modal-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 12px 14px;
          border-bottom: 1px solid rgba(15, 23, 42, 0.08);
          background: rgba(15, 23, 42, 0.02);
        }
        .modal-title {
          font-weight: 650;
          color: #0f172a;
        }
        .modal-body {
          padding: 12px;
          max-height: calc(80vh - 56px);
          overflow: auto;
        }
        .icon-btn {
          border: 1px solid rgba(15, 23, 42, 0.12);
          background: #fff;
          border-radius: 10px;
          padding: 6px 10px;
          cursor: pointer;
        }
        .icon-btn:hover {
          background: rgba(59, 130, 246, 0.08);
        }
        details.cat {
          border: 1px solid rgba(15, 23, 42, 0.08);
          border-radius: 12px;
          background: #fff;
          margin-bottom: 8px;
          overflow: hidden;
        }
        details.cat[open] {
          background: rgba(15, 23, 42, 0.01);
        }
        .cat-title {
          cursor: pointer;
          user-select: none;
          padding: 8px 10px;
          font-weight: 600;
          color: #0f172a;
          background: rgba(15, 23, 42, 0.03);
        }
        details.cat .picker-item {
          padding: 6px 10px;
        }
        th:nth-child(2),
        td:nth-child(2) {
          display: none;
        }
      }
    </style>
  </head>
  <body>
    <div class="container">
      <h1>埋点实时查看工具</h1>
      <div class="subtitle">输入测试用的 device_id，保持浏览器开着，在手机上走流程时即可实时看到该设备的埋点是否落库。</div>

      <div class="card">
        <div class="form-row">
          <div class="field" style="width: 180px;">
            <label for="devicePresetSelect">常用设备</label>
            <select id="devicePresetSelect">
              <option value="">自定义（手动输入）</option>
              <option value="36D2F2D1-524E-4DDB-9CD1-DF06C32E1404">纪伟的手机</option>
            </select>
          </div>
          <div class="field" style="flex: 1 1 220px;">
            <label for="deviceIdInput">device_id</label>
            <input id="deviceIdInput" placeholder="请输入要监听的 device_id" />
          </div>
          <div class="field" style="flex: 1 1 420px;">
            <label for="eventNameInput">事件名筛选（可选）</label>
            <div class="dropdown-input-row">
              <input id="eventNameInput" placeholder="支持逗号分隔多个；也可点右侧选择" />
              <button id="eventNameToggleBtn" type="button" class="btn btn-secondary dropdown-toggle-btn">选择</button>
            </div>
          </div>
          <div class="field" style="width: 120px;">
            <label for="intervalInput">轮询间隔（秒）</label>
            <input id="intervalInput" type="number" min="1" max="60" value="3" />
          </div>
          <div class="field" style="width: 120px;">
            <label for="limitInput">最大条数</label>
            <input id="limitInput" type="number" min="10" max="500" value="100" />
          </div>
          <button id="toggleBtn" class="btn btn-primary" style="margin-left: auto;">开始监听</button>
          <button id="clearBtn" class="btn btn-secondary">清空列表</button>
        </div>
        <div class="status-line">
          <div>
            <span id="statusPill" class="status-pill" style="display: none;">
              <span class="status-dot"></span>
              <span id="statusText">监听中</span>
            </span>
            <span id="errorText" class="muted"></span>
            <span id="clockText" class="muted" style="margin-left: 8px;"></span>
          </div>
          <div class="muted" id="metaText"></div>
        </div>
      </div>

      <div id="eventNameModal" class="modal" style="display:none;">
        <div id="eventNameModalBackdrop" class="modal-backdrop"></div>
        <div class="modal-card" role="dialog" aria-modal="true" aria-label="选择事件名">
          <div class="modal-header">
            <div class="modal-title">选择事件名（可复选）</div>
            <button id="eventNameModalCloseBtn" type="button" class="icon-btn">关闭</button>
          </div>
          <div class="modal-body">
            <div class="picker-actions" style="margin-bottom: 8px;">
              <input id="eventNameSearch" placeholder="搜索事件名/标识符，如：button / story" />
              <button id="eventNameClearBtn" type="button" class="btn btn-secondary" style="padding:8px 10px;">清空</button>
            </div>

            <div id="eventNameCheckboxList" class="picker-list">
              <div class="muted" id="eventNameStaticHint">按模块分类展示，可复选；勾选会自动写入输入框。</div>
              <select id="eventNameSelect" multiple size="14" style="width:100%;">
                <optgroup label="用户启动">
                  <option value="game_start">游戏启动 (game_start)</option>
                  <option value="memorySize">启动资源更新结果 (memorySize)</option>
                  <option value="account_create_result">账户创建成功 (account_create_result)</option>
                  <option value="role_create_complete">角色创建成功 (role_create_complete)</option>
                  <option value="screen_view">主要页面曝光 (screen_view)</option>
                  <option value="button_click">关键按钮点击 (button_click)</option>
                </optgroup>
                <optgroup label="事件&剧情">
                  <option value="event_trigger">事件触发 (event_trigger)</option>
                  <option value="event_complete">事件完成 (event_complete)</option>
                  <option value="story_enter">剧情开始 (story_enter)</option>
                  <option value="story_interrupt">剧情中断 (story_interrupt)</option>
                  <option value="story_complete">剧情完成 (story_complete)</option>
                </optgroup>
                <optgroup label="游戏内操作">
                  <option value="new_round">人生年份推进 (new_round)</option>
                  <option value="role_death">死亡（未测试）(role_death)</option>
                </optgroup>
                <optgroup label="三方绑定/登录">
                  <option value="bind_attempt">点击绑定按钮 (bind_attempt)</option>
                  <option value="bind_result">绑定成功（未测试）(bind_result)</option>
                  <option value="unbind_result">解除绑定成功（未测试）(unbind_result)</option>
                  <option value="switch_click">点击切换账号 (switch_click)</option>
                </optgroup>
                <optgroup label="新手引导">
                  <option value="guide_show">引导展示 (guide_show)</option>
                  <option value="guide_close">引导关闭 (guide_close)</option>
                </optgroup>
                <optgroup label="系统解锁">
                  <option value="feature_status_change">节点成功解锁 (feature_status_change)</option>
                  <option value="feature_locked_click">点击未解锁节点 (feature_locked_click)</option>
                </optgroup>
                <optgroup label="公告和邮箱">
                  <option value="announcement_show">点击查看公告 (announcement_show)</option>
                  <option value="mail_receive">收到邮件 (mail_receive)</option>
                  <option value="mail_reward">领取邮件 (mail_reward)</option>
                  <option value="mail_claim_fail">领取失败 (mail_claim_fail)</option>
                </optgroup>
              </select>
            </div>
          </div>
        </div>
      </div>

      <div class="scroll-container">
        <table>
          <thead>
            <tr>
              <th style="width: 110px;">时间</th>
              <th style="width: 150px;">标识</th>
              <th style="width: 130px;">事件名</th>
              <th>全部字段（JSON）</th>
            </tr>
          </thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </div>

    <script>
      const devicePresetSelect = document.getElementById("devicePresetSelect");
      const deviceIdInput = document.getElementById("deviceIdInput");
      const intervalInput = document.getElementById("intervalInput");
      const limitInput = document.getElementById("limitInput");
      const toggleBtn = document.getElementById("toggleBtn");
      const clearBtn = document.getElementById("clearBtn");
      const tbody = document.getElementById("tbody");
      const statusPill = document.getElementById("statusPill");
      const statusText = document.getElementById("statusText");
      const errorText = document.getElementById("errorText");
      const metaText = document.getElementById("metaText");
      const clockText = document.getElementById("clockText");
      const eventNameInput = document.getElementById("eventNameInput");
      const eventNameToggleBtn = document.getElementById("eventNameToggleBtn");
      const eventNameModal = document.getElementById("eventNameModal");
      const eventNameModalBackdrop = document.getElementById("eventNameModalBackdrop");
      const eventNameModalCloseBtn = document.getElementById("eventNameModalCloseBtn");
      const eventNameSearch = document.getElementById("eventNameSearch");
      const eventNameCheckboxList = document.getElementById("eventNameCheckboxList");
      const eventNameStaticHint = document.getElementById("eventNameStaticHint");
      const eventNameSelect = document.getElementById("eventNameSelect");
      const eventNameClearBtn = document.getElementById("eventNameClearBtn");

      // 将前端运行时错误直接展示在页面上，便于快速定位
      window.addEventListener("error", (e) => {
        try {
          const msg = e?.message || "前端脚本错误";
          errorText.textContent = msg;
        } catch (_) {}
      });
      window.addEventListener("unhandledrejection", (e) => {
        try {
          const msg = e?.reason?.message || String(e?.reason || "Promise 错误");
          errorText.textContent = msg;
        } catch (_) {}
      });

      devicePresetSelect.addEventListener("change", () => {
        const v = devicePresetSelect.value;
        if (v) {
          deviceIdInput.value = v;
        }
      });

      function normalizeEventNameList(text) {
        return String(text || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
      }

      function setEventNameInput(values) {
        const unique = Array.from(new Set(values.map((v) => String(v).trim()).filter(Boolean)));
        eventNameInput.value = unique.join(",");
      }

      function syncCheckboxesFromInput() {
        const selected = new Set(normalizeEventNameList(eventNameInput.value));
        const options = eventNameSelect.options;
        for (let i = 0; i < options.length; i++) {
          const opt = options[i];
          if (!opt.value) continue;
          opt.selected = selected.has(opt.value);
        }
      }

      function syncInputFromCheckboxes() {
        const values = Array.from(eventNameSelect.selectedOptions).map((o) => o.value);
        setEventNameInput(values);
      }

      function filterEventCheckboxes() {
        const q = (eventNameSearch.value || "").trim().toLowerCase();
        const options = eventNameSelect.options;
        for (let i = 0; i < options.length; i++) {
          const opt = options[i];
          const text = (opt.textContent || "").toLowerCase();
          // 无法真正隐藏 option（各浏览器差异大），用 disabled 模拟过滤
          opt.disabled = !!q && !text.includes(q);
        }
      }

      function renderCatalog() {
        // select/optgroup 已在 HTML 里固定，这里只做同步
        eventNameStaticHint.style.display = "";
        syncCheckboxesFromInput();
        filterEventCheckboxes();
      }

      function setEventModalOpen(open) {
        eventNameModal.style.display = open ? "flex" : "none";
        document.body.style.overflow = open ? "hidden" : "";
        if (open) {
          renderCatalog();
          eventNameSearch.focus();
        } else {
          eventNameSearch.value = "";
          filterEventCheckboxes();
        }
      }

      function isEventModalOpen() {
        return eventNameModal.style.display !== "none";
      }

      eventNameToggleBtn.addEventListener("click", (e) => {
        e.preventDefault();
        setEventModalOpen(!isEventModalOpen());
      });

      eventNameInput.addEventListener("focus", () => {
        // 聚焦输入框时不强制展开，避免打字被打断；需要展开可点“选择”
      });

      eventNameModalBackdrop.addEventListener("click", () => {
        if (isEventModalOpen()) setEventModalOpen(false);
      });

      eventNameModalCloseBtn.addEventListener("click", () => {
        if (isEventModalOpen()) setEventModalOpen(false);
      });

      document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && isEventModalOpen()) {
          setEventModalOpen(false);
        }
      });

      eventNameSelect.addEventListener("change", () => {
        syncInputFromCheckboxes();
      });

      eventNameInput.addEventListener("input", () => {
        syncCheckboxesFromInput();
      });

      eventNameSearch.addEventListener("input", () => {
        filterEventCheckboxes();
      });

      eventNameClearBtn.addEventListener("click", () => {
        setEventNameInput([]);
        syncCheckboxesFromInput();
        eventNameSearch.value = "";
        filterEventCheckboxes();
      });

      // 初始化一次
      syncCheckboxesFromInput();
      filterEventCheckboxes();

      deviceIdInput.addEventListener("input", () => {
        const cur = deviceIdInput.value.trim();
        let matched = false;
        for (const opt of devicePresetSelect.options) {
          if (opt.value && opt.value === cur) {
            matched = true;
            devicePresetSelect.value = opt.value;
            break;
          }
        }
        if (!matched && devicePresetSelect.value) {
          devicePresetSelect.value = "";
        }
      });

      let timerId = null;
      // lastMaxId：本次会话中用于控制“首屏覆盖 / 增量插入”的游标
      let lastMaxId = null;
      // maxSeenId：当前 user_id 已经看过的最大 id，用于传给后端 since_id，避免重复拉取
      let maxSeenId = null;
      let lastIdentifierForCursor = null;
      let clockTimerId = null;
      let clearBaselineId = null;

      function setRunning(running) {
        if (running) {
          toggleBtn.textContent = "停止监听";
          toggleBtn.classList.remove("btn-secondary");
          toggleBtn.classList.add("btn-primary");
          statusPill.style.display = "inline-flex";
          statusText.textContent = "监听中";
        } else {
          toggleBtn.textContent = "开始监听";
          toggleBtn.classList.remove("btn-primary");
          toggleBtn.classList.add("btn-secondary");
          statusPill.style.display = "none";
        }
      }

      function startClock() {
        if (clockTimerId) return;
        const update = () => {
          const nowIso = new Date().toISOString();
          clockText.textContent = "当前时间：" + formatTime(nowIso);
        };
        update();
        clockTimerId = setInterval(update, 1000);
      }

      function formatTime(value) {
        if (!value) return "";
        const d = new Date(value);
        if (isNaN(d.getTime())) return value;
        const pad = (n) => (n < 10 ? "0" + n : "" + n);
        return (
          d.getMonth() +
          1 +
          "-" +
          pad(d.getDate()) +
          " " +
          pad(d.getHours()) +
          ":" +
          pad(d.getMinutes()) +
          ":" +
          pad(d.getSeconds())
        );
      }

      async function fetchEvents() {
        const deviceId = deviceIdInput.value.trim();
        if (!deviceId) {
          errorText.textContent = "请先填写 device_id";
          stop();
          return;
        }
        const limit = limitInput.value || "100";

        const params = new URLSearchParams({ limit: limit });
        params.set("device_id", deviceId);
        const eventNameFilter = eventNameInput.value.trim();
        if (eventNameFilter) {
          params.set("event_name", eventNameFilter);
        }
        const since = maxSeenId ?? clearBaselineId;
        if (since) params.set("since_id", String(since));

        try {
          const res = await fetch("/events?" + params.toString());
          if (!res.ok) {
            const data = await res.json().catch(() => ({}));
            throw new Error(data.error || "请求失败：" + res.status);
          }
          const data = await res.json();

          const now = new Date();
          metaText.textContent = `最新拉取：${formatTime(now.toISOString())}，共 ${data.length} 条`;
          errorText.textContent = "";

          if (!Array.isArray(data)) return;

          // 如果是第一次拉取（没有 lastMaxId），就直接整体覆盖；
          // 如果是增量拉取，则在现有行前面插入新行。
          if (!lastMaxId) {
            tbody.innerHTML = "";
          }

          const fragment = document.createDocumentFragment();

          data.forEach((row, index) => {
            const tr = document.createElement("tr");
            if (index === 0) {
              tr.classList.add("highlight");
            }
            const timeTd = document.createElement("td");
            timeTd.textContent = formatTime(row.event_time);

            const deviceIdTd = document.createElement("td");
            deviceIdTd.textContent = row.device_id ?? "";

            const eventNameTd = document.createElement("td");
            const pill = document.createElement("span");
            pill.className = "pill";
            pill.textContent = row.event_name || "";
            eventNameTd.appendChild(pill);

            const contentTd = document.createElement("td");
            const pre = document.createElement("pre");
            let text = "";
            if (row) {
              try {
                text = JSON.stringify(row, null, 2);
              } catch (e) {
                text = String(row);
              }
            }
            pre.textContent = text;
            contentTd.appendChild(pre);

            tr.appendChild(timeTd);
            tr.appendChild(deviceIdTd);
            tr.appendChild(eventNameTd);
            tr.appendChild(contentTd);
            fragment.appendChild(tr);
          });
          if (!lastMaxId) {
            tbody.appendChild(fragment);
          } else {
            // 有增量时，把新数据插到表头
            tbody.insertBefore(fragment, tbody.firstChild);
          }

          if (data.length > 0) {
            const batchMaxId = data.reduce(
              (max, row) => (row.id > max ? row.id : max),
              maxSeenId || 0
            );
            lastMaxId = batchMaxId;
            maxSeenId = batchMaxId;
          }
        } catch (e) {
          console.error(e);
          errorText.textContent = e.message || String(e);
        }
      }

      function start() {
        if (timerId) return;
        const deviceId = deviceIdInput.value.trim();
        if (!deviceId) {
          errorText.textContent = "请先填写 device_id";
          return;
        }
        errorText.textContent = "";
        // 如果切换了 device_id，就从最新开始重新拉，这时清空已见过游标
        if (lastIdentifierForCursor !== deviceId) {
          maxSeenId = null;
          lastIdentifierForCursor = deviceId;
        }
        lastMaxId = null;
        setRunning(true);
        startClock();
        fetchEvents();
        const intervalSec = Math.max(1, Math.min(60, Number(intervalInput.value) || 3));
        timerId = setInterval(fetchEvents, intervalSec * 1000);
      }

      function stop() {
        if (timerId) {
          clearInterval(timerId);
          timerId = null;
        }
        setRunning(false);
      }

      function clearLogs() {
        // 目标：清空后不再把旧数据重新展示出来，只展示“清空之后新增”的数据
        clearBaselineId = maxSeenId ?? clearBaselineId;
        tbody.innerHTML = "";
        lastMaxId = null;
        maxSeenId = null;
        metaText.textContent = "";
        errorText.textContent = "";
      }

      toggleBtn.addEventListener("click", () => {
        if (timerId) {
          stop();
        } else {
          start();
        }
      });

      clearBtn.addEventListener("click", () => {
        clearLogs();
      });


      deviceIdInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          start();
        }
      });
    </script>
  </body>
</html>
"""


@app.route("/")
def index() -> Any:
    return render_template_string(INDEX_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=True)

