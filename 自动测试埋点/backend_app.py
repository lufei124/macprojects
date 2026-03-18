from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template_string, request
import pymysql

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
    # 支持多种业务标识字段：优先 device_id，其次 role_id / user_id
    identifier_candidates = [
        ("device_id", request.args.get("device_id")),
        ("role_id", request.args.get("role_id")),
        ("user_id", request.args.get("user_id")),
    ]
    identifier_field: Optional[str] = None
    identifier_value: Optional[str] = None
    for field, value in identifier_candidates:
        if value and value.strip():
            identifier_field = field
            identifier_value = value.strip()
            break

    if not identifier_field or identifier_value is None:
        return (
            jsonify({"error": "one of device_id / role_id / user_id is required"}),
            400,
        )

    since_id_param = request.args.get("since_id")
    since_id: Optional[int] = None
    if since_id_param:
        try:
            since_id = int(since_id_param)
        except ValueError:
            since_id = None

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
        WHERE {identifier_field} = %s
    """
    params: List[Any] = [identifier_value]

    if event_names:
        placeholders = ",".join(["%s"] * len(event_names))
        query += f" AND event_name IN ({placeholders})"
        params.extend(event_names)

    if since_id is not None:
        query += " AND id > %s"
        params.append(since_id)

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

        raw_content = item.get("content")
        parsed_content: Any = raw_content
        if isinstance(raw_content, str):
            try:
                import json

                parsed_content = json.loads(raw_content)
            except Exception:
                parsed_content = raw_content
        item["content"] = parsed_content
        # 统一时间字段名，前端只关心一个字段
        if "event_time" in item and isinstance(item["event_time"], datetime):
            item["event_time"] = item["event_time"].isoformat()
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
      <div class="subtitle">输入测试用的标识（device_id / role_id / user_id），保持浏览器开着，在手机上走流程时即可实时看到埋点是否落库。</div>

      <div class="card">
        <div class="form-row">
          <div class="field" style="width: 180px;">
            <label for="devicePresetSelect">常用设备</label>
            <select id="devicePresetSelect">
              <option value="">自定义（手动输入）</option>
              <option value="36D2F2D1-524E-4DDB-9CD1-DF06C32E1404">纪伟的手机</option>
            </select>
          </div>
          <div class="field" style="width: 160px;">
            <label for="idFieldSelect">筛选字段</label>
            <select id="idFieldSelect">
              <option value="device_id" selected>device_id</option>
              <option value="role_id">role_id</option>
              <option value="user_id">user_id</option>
            </select>
          </div>
          <div class="field" style="flex: 1 1 220px;">
            <label for="idValueInput">字段值</label>
            <input id="idValueInput" placeholder="或从上方选择常用设备" />
          </div>
          <div class="field" style="flex: 1 1 220px;">
            <label for="eventNameInput">事件名筛选（可选，逗号分隔）</label>
            <input id="eventNameInput" placeholder="如：button_click,event_complete" />
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
      const idFieldSelect = document.getElementById("idFieldSelect");
      const idValueInput = document.getElementById("idValueInput");
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

      devicePresetSelect.addEventListener("change", () => {
        const v = devicePresetSelect.value;
        if (v) {
          idFieldSelect.value = "device_id";
          idValueInput.value = v;
        }
      });

      idValueInput.addEventListener("input", () => {
        const cur = idValueInput.value.trim();
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
        const idField = (idFieldSelect.value || "device_id").trim();
        const idValue = idValueInput.value.trim();
        if (!idValue) {
          errorText.textContent = "请先填写字段值";
          stop();
          return;
        }
        const limit = limitInput.value || "100";

        const params = new URLSearchParams({ limit: limit });
        params.set(idField, idValue);
        const eventNameFilter = eventNameInput.value.trim();
        if (eventNameFilter) {
          params.set("event_name", eventNameFilter);
        }
        if (maxSeenId) {
          params.set("since_id", String(maxSeenId));
        }

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
            deviceIdTd.textContent = row[idField] ?? row.device_id ?? row.role_id ?? row.user_id ?? "";

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
        const idField = (idFieldSelect.value || "device_id").trim();
        const idValue = idValueInput.value.trim();
        if (!idValue) {
          errorText.textContent = "请先填写字段值";
          return;
        }
        errorText.textContent = "";
        // 如果切换了标识字段/值，就从最新开始重新拉，这时清空已见过游标
        const identifier = `${idField}:${idValue}`;
        if (lastIdentifierForCursor !== identifier) {
          maxSeenId = null;
          lastIdentifierForCursor = identifier;
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
        tbody.innerHTML = "";
        // 只清空前端展示，不重置 maxSeenId，这样之后只会拉取“比已见过 id 更大的新数据”
        lastMaxId = null;
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

      idValueInput.addEventListener("keydown", (e) => {
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

