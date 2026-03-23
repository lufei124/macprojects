from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template_string, request
import pymysql
import json

try:
    from config import DB_CONFIGS, DEFAULT_ENV
except ImportError:
    from config.example import DB_CONFIGS, DEFAULT_ENV  # type: ignore


app = Flask(__name__)


def get_connection(env: str = DEFAULT_ENV):
    cfg = DB_CONFIGS.get(env, DB_CONFIGS[DEFAULT_ENV])
    return pymysql.connect(
        host=cfg["host"],
        port=cfg.get("port", 3306),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        cursorclass=pymysql.cursors.DictCursor,
    )


@app.route("/health")
def health() -> Any:
    return jsonify({"status": "ok"})


@app.route("/envs")
def envs() -> Any:
    return jsonify({
        key: {"name": cfg.get("name", key), "available": bool(cfg.get("host"))}
        for key, cfg in DB_CONFIGS.items()
    })


@app.route("/events")
def events() -> Any:
    device_id = request.args.get("device_id", "").strip()
    user_id_param = request.args.get("user_id", "").strip()

    if not device_id and not user_id_param:
        return jsonify({"error": "device_id or user_id is required"}), 400

    env = request.args.get("env", DEFAULT_ENV).strip()
    if env not in DB_CONFIGS:
        return jsonify({"error": f"invalid env: {env}"}), 400

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

    table = DB_CONFIGS[env]["table"]

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
        WHERE 1=1
    """
    params: List[Any] = []

    if device_id:
        query += " AND device_id = %s"
        params.append(device_id)

    if user_id_param:
        try:
            user_id_int = int(user_id_param)
            query += " AND user_id = %s"
            params.append(user_id_int)
        except ValueError:
            return jsonify({"error": "user_id must be an integer"}), 400

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

    with get_connection(env) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows: List[Dict[str, Any]] = cursor.fetchall()

    processed: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key, value in list(item.items()):
            if isinstance(value, bytes):
                if len(value) == 1:
                    item[key] = int.from_bytes(value, byteorder="big")
                else:
                    item[key] = value.decode(errors="ignore")
            elif isinstance(value, datetime):
                item[key] = value.isoformat()

        event_time_val = item.get("event_time")
        if isinstance(event_time_val, int) and event_time_val > 1_000_000_000_000:
            beijing_tz = timezone(timedelta(hours=8))
            item["event_time"] = datetime.fromtimestamp(event_time_val / 1000, tz=beijing_tz).strftime("%Y-%m-%d %H:%M:%S")

        for big_int_key in ("role_id", "user_id"):
            v = item.get(big_int_key)
            if isinstance(v, int) and abs(v) > 9007199254740991:
                item[big_int_key] = str(v)

        raw_content = item.get("content")
        parsed_content: Any = raw_content
        if isinstance(raw_content, str):
            try:
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
<meta charset="utf-8">
<title>埋点实时查看工具</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root { --accent: #2563eb; --accent-light: #eff6ff; --green: #10b981; --green-light: #ecfdf5; --red: #ef4444; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 0; background: #f8fafc; color: #0f172a; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px 16px 40px; }
h1 { font-size: 22px; margin-bottom: 4px; font-weight: 700; letter-spacing: -0.02em; color: #0f172a; }
.subtitle { font-size: 13px; color: #94a3b8; margin-bottom: 20px; }
.card { background: #ffffff; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; margin-bottom: 16px; }
.form-row { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; }
.field { display: flex; flex-direction: column; min-width: 0; }
.field label { font-size: 11px; color: #64748b; margin-bottom: 4px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.04em; }
.field input, .field select { border-radius: 8px; border: 1px solid #d1d5db; padding: 8px 12px; font-size: 14px; min-width: 0; transition: border-color 0.15s, box-shadow 0.15s; }
.field input:focus, .field select:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12); }
.btn { border-radius: 8px; border: none; padding: 10px 20px; font-size: 14px; font-weight: 500; cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: all 0.15s ease; font-family: inherit; }
.btn-primary { background: var(--accent); color: white; }
.btn-primary:hover { background: #1d4ed8; box-shadow: 0 2px 8px rgba(37,99,235,0.3); }
.btn-primary:active { transform: scale(0.97); }
.btn-secondary { background: #f1f5f9; color: #334155; }
.btn-secondary:hover { background: #e2e8f0; }
.status-line { margin-top: 12px; font-size: 12px; color: #6b7280; display: flex; justify-content: space-between; gap: 12px; }
.status-pill { display: inline-flex; align-items: center; gap: 6px; padding: 4px 12px; border-radius: 999px; background: var(--green-light); color: #065f46; font-size: 12px; font-weight: 500; }
.status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; }
@keyframes pulse { 0%, 100% { box-shadow: 0 0 0 0 rgba(16,185,129,0.4); } 50% { box-shadow: 0 0 0 6px rgba(16,185,129,0); } }
.muted { color: #9ca3af; font-size: 12px; }
.error-text { color: var(--red); font-size: 12px; font-weight: 500; }
.scroll-container { margin-top: 4px; max-height: 500px; overflow: auto; border-radius: 10px; border: 1px solid #e2e8f0; background: #fff; }
.scroll-container::-webkit-scrollbar { width: 6px; }
.scroll-container::-webkit-scrollbar-track { background: transparent; }
.scroll-container::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
.scroll-container::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
table { width: 100%; border-collapse: collapse; font-size: 13px; table-layout: fixed; }
thead { background: #f1f5f9; position: sticky; top: 0; z-index: 1; }
th, td { padding: 10px 12px; border-bottom: 1px solid #f1f5f9; text-align: left; }
th { font-weight: 600; color: #475569; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }
td { vertical-align: top; }
tbody tr { transition: background 0.15s ease; }
tbody tr:hover { background: #f8fafc; }
tbody tr.highlight { animation: highlight 2s ease-out; }
@keyframes highlight { 0% { background-color: #dbeafe; } 100% { background-color: transparent; } }
.pill { display: inline-flex; padding: 3px 10px; border-radius: 999px; background: var(--accent-light); color: var(--accent); font-size: 11px; font-weight: 500; font-family: 'JetBrains Mono', monospace; }
pre { margin: 0; font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11px; white-space: pre-wrap; word-break: break-all; max-height: 120px; overflow: auto; line-height: 1.5; }
pre::-webkit-scrollbar { width: 4px; }
pre::-webkit-scrollbar-thumb { background: #d1d5db; border-radius: 2px; }
.toast { position: fixed; bottom: 24px; right: 24px; padding: 10px 18px; background: #0f172a; color: #fff; border-radius: 10px; font-size: 13px; font-weight: 500; transform: translateY(20px); opacity: 0; transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1); z-index: 10000; box-shadow: 0 8px 24px rgba(0,0,0,0.16); }
.toast.show { transform: translateY(0); opacity: 1; }
.modal { position: fixed; inset: 0; z-index: 9999; display: none; align-items: flex-start; justify-content: center; padding: 5vh 16px; }
.modal.active { display: flex; }
.modal-backdrop { position: absolute; inset: 0; background: rgba(15,23,42,0.4); backdrop-filter: blur(4px); -webkit-backdrop-filter: blur(4px); }
.modal-card { position: relative; width: min(600px, 95vw); max-height: 80vh; background: #fff; border-radius: 12px; box-shadow: 0 20px 40px rgba(0,0,0,0.3); display: flex; flex-direction: column; }
.modal-header { display: flex; align-items: center; justify-content: space-between; padding: 16px 20px; border-bottom: 1px solid #e5e7eb; }
.modal-title { font-size: 16px; font-weight: 600; }
.modal-body { padding: 16px 20px; overflow: auto; flex: 1; }
.modal-footer { display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; border-top: 1px solid #e5e7eb; background: #f9fafb; }
.search-box { display: flex; gap: 10px; margin-bottom: 16px; }
.search-box input { flex: 1; border-radius: 8px; border: 1px solid #d1d5db; padding: 10px 14px; font-size: 14px; }
.search-box input:focus { outline: none; border-color: #2563eb; }
.category-section { margin-bottom: 12px; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }
.category-header { display: flex; align-items: center; justify-content: space-between; padding: 10px 14px; background: #f9fafb; cursor: pointer; user-select: none; }
.category-header:hover { background: #f3f4f6; }
.category-title { font-weight: 600; font-size: 14px; }
.category-count { font-size: 12px; color: #6b7280; background: #fff; padding: 2px 8px; border-radius: 999px; }
.category-items { padding: 8px; display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 8px; }
.event-item { display: flex; align-items: flex-start; gap: 8px; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb; background: #fff; cursor: pointer; transition: all 0.15s ease; }
.event-item:hover { border-color: #2563eb; background: #eff6ff; }
.event-item.selected { border-color: #2563eb; background: #eff6ff; }
.event-checkbox { width: 18px; height: 18px; margin-top: 2px; cursor: pointer; }
.event-info { flex: 1; }
.event-name { font-weight: 600; font-size: 13px; margin-bottom: 2px; }
.event-key { font-family: monospace; font-size: 11px; color: #6b7280; }
.btn-icon { width: 36px; height: 36px; border-radius: 8px; border: 1px solid #d1d5db; background: #fff; cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 16px; }
.btn-icon:hover { background: #f3f4f6; }
.selected-count { font-size: 14px; color: #64748b; }
.selected-count strong { color: var(--accent); }
.category-section.collapsed .category-items { display: none; }
.json-key { color: #0369a1; }
.json-string { color: #059669; }
.json-number { color: #d97706; }
.json-boolean { color: #7c3aed; }
.json-null { color: #9ca3af; font-style: italic; }
@media (max-width: 768px) {
  .form-row { flex-direction: column; }
  .field { width: 100%; }
  .btn { width: 100%; justify-content: center; }
  .category-items { grid-template-columns: 1fr; }
  th:nth-child(2), td:nth-child(2) { display: none; }
}
</style>
</head>
<body>
<div class="container">
  <h1>埋点实时查看工具</h1>
  <div class="subtitle">输入 device_id，实时监控埋点数据落库情况</div>

  <div class="card">
    <div class="form-row">
      <div class="field" style="width:140px">
        <label>环境</label>
        <select id="envSelect">
          <option value="test">测试环境</option>
          <option value="uat">UAT环境</option>
          <option value="production">线上环境</option>
        </select>
      </div>
      <div class="field" style="width:180px">
        <label>常用设备</label>
        <select id="devicePresetSelect">
          <option value="">自定义（手动输入）</option>
          <option value="36D2F2D1-524E-4DDB-9CD1-DF06C32E1404">纪伟的手机</option>
          <option value="3da7d7e66ff94487">三星的安卓测试机</option>
          <option value="c7c43a53670b868f">小米测试手机</option>
        </select>
      </div>
      <div class="field" style="flex:1">
        <label>device_id</label>
        <input id="deviceIdInput" placeholder="请输入要监听的 device_id">
      </div>
      <div class="field" style="flex:1">
        <label>事件名筛选（可选）</label>
        <div style="display:flex;gap:8px">
          <input id="eventNameInput" placeholder="逗号分隔或点击选择" style="flex:1">
          <button id="eventNameToggleBtn" class="btn-icon" title="选择事件">...</button>
        </div>
      </div>
      <div class="field" style="width:100px">
        <label>轮询间隔（秒）</label>
        <input id="intervalInput" type="number" min="1" max="60" value="3">
      </div>
      <div class="field" style="width:100px">
        <label>最大条数</label>
        <input id="limitInput" type="number" min="10" max="500" value="100">
      </div>
    </div>
    <div style="display:flex;gap:10px;margin-top:16px;flex-wrap:wrap">
      <button id="toggleBtn" class="btn btn-primary">开始监听</button>
      <button id="clearBtn" class="btn btn-secondary">清空列表</button>
    </div>
    <div class="status-line">
      <div>
        <span id="statusPill" class="status-pill" style="display:none"><span class="status-dot"></span><span id="statusText">监听中</span></span>
        <span id="errorText" class="error-text"></span>
        <span id="clockText" class="muted" style="margin-left:8px"></span>
      </div>
      <span id="metaText" class="muted"></span>
    </div>
  </div>

  <div id="eventNameModal" class="modal">
    <div id="eventNameModalBackdrop" class="modal-backdrop"></div>
    <div class="modal-card">
      <div class="modal-header">
        <div class="modal-title">选择事件名</div>
        <button id="eventNameModalCloseBtn" class="btn-icon">X</button>
      </div>
      <div class="modal-body">
        <div class="search-box">
          <input id="eventNameSearch" placeholder="搜索事件名...">
          <button id="eventNameClearBtn" class="btn btn-secondary">清空</button>
        </div>
        <div id="eventCategories"></div>
      </div>
      <div class="modal-footer">
        <div class="selected-count">已选择 <strong id="selectedCount">0</strong> 个事件</div>
        <div style="display:flex;gap:10px">
          <button id="eventNameModalCancelBtn" class="btn btn-secondary">取消</button>
          <button id="eventNameModalConfirmBtn" class="btn btn-primary">确认</button>
        </div>
      </div>
    </div>
  </div>

  <div class="scroll-container">
    <table>
      <thead>
        <tr>
          <th style="width:130px">时间</th>
          <th style="width:150px">标识</th>
          <th style="width:120px">事件名</th>
          <th>全部字段（JSON）</th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<div id="toast" class="toast"></div>

<script>
var envSelect = document.getElementById("envSelect");
var devicePresetSelect = document.getElementById("devicePresetSelect");
var deviceIdInput = document.getElementById("deviceIdInput");
var intervalInput = document.getElementById("intervalInput");
var limitInput = document.getElementById("limitInput");
var toggleBtn = document.getElementById("toggleBtn");
var clearBtn = document.getElementById("clearBtn");
var tbody = document.getElementById("tbody");
var statusPill = document.getElementById("statusPill");
var statusText = document.getElementById("statusText");
var errorText = document.getElementById("errorText");
var metaText = document.getElementById("metaText");
var clockText = document.getElementById("clockText");
var eventNameInput = document.getElementById("eventNameInput");
var eventNameToggleBtn = document.getElementById("eventNameToggleBtn");
var eventNameModal = document.getElementById("eventNameModal");
var eventNameModalBackdrop = document.getElementById("eventNameModalBackdrop");
var eventNameModalCloseBtn = document.getElementById("eventNameModalCloseBtn");
var eventNameModalCancelBtn = document.getElementById("eventNameModalCancelBtn");
var eventNameModalConfirmBtn = document.getElementById("eventNameModalConfirmBtn");
var eventNameSearch = document.getElementById("eventNameSearch");
var eventNameClearBtn = document.getElementById("eventNameClearBtn");
var eventCategories = document.getElementById("eventCategories");
var selectedCount = document.getElementById("selectedCount");
var toast = document.getElementById("toast");

var timerId = null;
var lastMaxId = null;
var maxSeenId = null;
var lastIdentifierForCursor = null;
var clockTimerId = null;
var clearBaselineId = null;
var selectedEvents = {};

var eventCategoriesData = [
  {name:"用户启动", events:[
    {key:"game_start",name:"游戏启动"},
    {key:"memorySize",name:"启动资源更新结果"},
    {key:"account_create_result",name:"账户创建成功"},
    {key:"role_create_complete",name:"角色创建成功"},
    {key:"screen_view",name:"主要页面曝光"},
    {key:"button_click",name:"关键按钮点击"}
  ]},
  {name:"事件&剧情", events:[
    {key:"event_trigger",name:"事件触发"},
    {key:"event_complete",name:"事件完成"},
    {key:"story_enter",name:"剧情开始"},
    {key:"story_interrupt",name:"剧情中断"},
    {key:"story_complete",name:"剧情完成"}
  ]},
  {name:"游戏内操作", events:[
    {key:"new_round",name:"人生年份推进"},
    {key:"role_death",name:"死亡"}
  ]},
  {name:"三方绑定/登录", events:[
    {key:"bind_attempt",name:"点击绑定按钮"},
    {key:"bind_result",name:"绑定成功"},
    {key:"unbind_result",name:"解除绑定成功"},
    {key:"switch_click",name:"点击切换账号"}
  ]},
  {name:"新手引导", events:[
    {key:"guide_show",name:"引导展示"},
    {key:"guide_close",name:"引导关闭"}
  ]},
  {name:"系统解锁", events:[
    {key:"feature_status_change",name:"节点成功解锁"},
    {key:"feature_locked_click",name:"点击未解锁节点"}
  ]},
  {name:"公告和邮箱", events:[
    {key:"announcement_show",name:"点击查看公告"},
    {key:"mail_receive",name:"收到邮件"},
    {key:"mail_reward",name:"领取邮件"},
    {key:"mail_claim_fail",name:"领取失败"}
  ]}
];

window.onerror = function(msg) { errorText.textContent = msg; };

function highlightJSON(json) {
  if (typeof json !== "string") json = JSON.stringify(json, null, 2);
  var escaped = json.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return escaped.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\\b(true|false)\\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?|\\bnull\\b)/g,
    function(match) {
      if (/^"/.test(match)) {
        if (/:$/.test(match)) return '<span class="json-key">' + match + '</span>';
        return '<span class="json-string">' + match + '</span>';
      }
      if (/true|false/.test(match)) return '<span class="json-boolean">' + match + '</span>';
      if (/null/.test(match)) return '<span class="json-null">' + match + '</span>';
      return '<span class="json-number">' + match + '</span>';
    }
  );
}

function showToast(msg) {
  toast.textContent = msg;
  toast.className = "toast show";
  setTimeout(function() { toast.className = "toast"; }, 2000);
}

devicePresetSelect.onchange = function() {
  if (this.value) {
    deviceIdInput.value = this.value;
  }
};

deviceIdInput.oninput = function() {
  var val = this.value.trim();
  var found = false;
  for (var i = 0; i < devicePresetSelect.options.length; i++) {
    if (devicePresetSelect.options[i].value === val) {
      devicePresetSelect.selectedIndex = i;
      found = true;
      break;
    }
  }
  if (!found) devicePresetSelect.selectedIndex = 0;
};

function formatTime(value) {
  if (!value) return "";
  var d = new Date(value);
  if (isNaN(d.getTime())) return value;
  var pad = function(n) { return n < 10 ? "0" + n : "" + n; };
  return (d.getMonth()+1) + "-" + pad(d.getDate()) + " " + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
}

function setRunning(running) {
  if (running) {
    toggleBtn.textContent = "停止监听";
    toggleBtn.className = "btn btn-primary";
    statusPill.style.display = "inline-flex";
    statusText.textContent = "监听中";
  } else {
    toggleBtn.textContent = "开始监听";
    toggleBtn.className = "btn btn-primary";
    statusPill.style.display = "none";
  }
}

function startClock() {
  if (clockTimerId) return;
  var update = function() { clockText.textContent = formatTime(new Date().toISOString()); };
  update();
  clockTimerId = setInterval(update, 1000);
}

async function loadEnvs() {
  try {
    var res = await fetch("/envs");
    var data = await res.json();
    envSelect.innerHTML = "";
    for (var key in data) {
      if (data[key].available) {
        var opt = document.createElement("option");
        opt.value = key;
        opt.textContent = data[key].name;
        envSelect.appendChild(opt);
      }
    }
  } catch (e) {
    console.error("Failed to load envs:", e);
  }
}

async function fetchEvents() {
  var deviceId = deviceIdInput.value.trim();
  if (!deviceId) {
    errorText.textContent = "请先填写 device_id";
    stop();
    return;
  }
  var params = new URLSearchParams();
  params.set("device_id", deviceId);
  params.set("env", envSelect.value || "test");
  params.set("limit", limitInput.value || "100");
  var eventNameFilter = eventNameInput.value.trim();
  if (eventNameFilter) params.set("event_name", eventNameFilter);
  var since = maxSeenId || clearBaselineId;
  if (since) params.set("since_id", String(since));

  try {
    var res = await fetch("/events?" + params.toString());
    if (!res.ok) {
      var data = await res.json().catch(function() { return {}; });
      throw new Error(data.error || "请求失败：" + res.status);
    }
    var data = await res.json();
    errorText.textContent = "";
    metaText.textContent = "最近拉取: " + formatTime(new Date().toISOString());
    if (!Array.isArray(data)) return;

    if (!lastMaxId) tbody.innerHTML = "";

    var fragment = document.createDocumentFragment();
    for (var i = 0; i < data.length; i++) {
      var row = data[i];
      var tr = document.createElement("tr");
      if (i === 0) tr.className = "highlight";
      tr.innerHTML = '<td style="white-space:nowrap;font-family:\\\'JetBrains Mono\\\',monospace;font-size:12px;color:#334155">' + formatTime(row.event_time) + '</td>' +
        '<td style="font-family:\\\'JetBrains Mono\\\',monospace;font-size:11px;color:#94a3b8">' + (row.device_id || "") + '</td>' +
        '<td><span class="pill">' + (row.event_name || "") + '</span></td>' +
        '<td><pre>' + highlightJSON(row) + '</pre></td>';
      fragment.appendChild(tr);
    }

    if (!lastMaxId) {
      tbody.appendChild(fragment);
    } else {
      tbody.insertBefore(fragment, tbody.firstChild);
    }

    if (data.length > 0) {
      var batchMaxId = maxSeenId || 0;
      for (var j = 0; j < data.length; j++) {
        if (data[j].id > batchMaxId) batchMaxId = data[j].id;
      }
      lastMaxId = batchMaxId;
      maxSeenId = batchMaxId;
    }
  } catch (e) {
    errorText.textContent = e.message || String(e);
  }
}

function start() {
  if (timerId) return;
  var deviceId = deviceIdInput.value.trim();
  if (!deviceId) {
    errorText.textContent = "请先填写 device_id";
    showToast("请先填写 device_id");
    return;
  }
  errorText.textContent = "";
  if (lastIdentifierForCursor !== deviceId) {
    maxSeenId = null;
    lastIdentifierForCursor = deviceId;
  }
  lastMaxId = null;
  setRunning(true);
  startClock();
  fetchEvents();
  var intervalSec = Math.max(1, Math.min(60, Number(intervalInput.value) || 3));
  timerId = setInterval(fetchEvents, intervalSec * 1000);
  showToast("监听已启动");
}

function stop() {
  if (timerId) {
    clearInterval(timerId);
    timerId = null;
  }
  setRunning(false);
  showToast("监听已停止");
}

function clearLogs() {
  clearBaselineId = maxSeenId || clearBaselineId;
  tbody.innerHTML = "";
  lastMaxId = null;
  maxSeenId = null;
  metaText.textContent = "";
  errorText.textContent = "";
  showToast("已清空");
}

toggleBtn.onclick = function() {
  if (timerId) { stop(); } else { start(); }
};

clearBtn.onclick = clearLogs;

deviceIdInput.onkeydown = function(e) {
  if (e.key === "Enter") start();
};

function renderEventCategories(filter) {
  filter = filter || "";
  var q = filter.toLowerCase();
  var html = "";

  for (var c = 0; c < eventCategoriesData.length; c++) {
    var cat = eventCategoriesData[c];
    var filtered = [];
    for (var e = 0; e < cat.events.length; e++) {
      var ev = cat.events[e];
      if (!q || ev.name.toLowerCase().indexOf(q) >= 0 || ev.key.toLowerCase().indexOf(q) >= 0) {
        filtered.push(ev);
      }
    }
    if (filtered.length === 0) continue;

    var itemsHtml = "";
    for (var i = 0; i < filtered.length; i++) {
      var ev = filtered[i];
      var isSel = selectedEvents[ev.key] ? true : false;
      itemsHtml += '<div class="event-item' + (isSel ? ' selected' : '') + '" data-key="' + ev.key + '">';
      itemsHtml += '<input type="checkbox" class="event-checkbox"' + (isSel ? ' checked' : '') + '>';
      itemsHtml += '<div class="event-info"><div class="event-name">' + ev.name + '</div>';
      itemsHtml += '<div class="event-key">' + ev.key + '</div></div></div>';
    }

    html += '<div class="category-section">';
    html += '<div class="category-header"><span class="category-title">' + cat.name + '</span>';
    html += '<span class="category-count">' + filtered.length + '/' + cat.events.length + '</span></div>';
    html += '<div class="category-items">' + itemsHtml + '</div></div>';
  }

  if (!html) html = '<div style="text-align:center;padding:40px;color:#9ca3af">没有找到匹配的事件</div>';
  eventCategories.innerHTML = html;

  var headers = eventCategories.querySelectorAll(".category-header");
  for (var h = 0; h < headers.length; h++) {
    headers[h].onclick = function() {
      this.parentElement.classList.toggle("collapsed");
    };
  }

  var items = eventCategories.querySelectorAll(".event-item");
  for (var k = 0; k < items.length; k++) {
    items[k].onclick = function(e) {
      if (e.target.type === "checkbox") return;
      var cb = this.querySelector(".event-checkbox");
      cb.checked = !cb.checked;
      var key = this.dataset.key;
      if (cb.checked) { selectedEvents[key] = true; this.classList.add("selected"); }
      else { delete selectedEvents[key]; this.classList.remove("selected"); }
      updateSelectedCount();
    };
    items[k].querySelector(".event-checkbox").onchange = function() {
      var item = this.closest(".event-item");
      var key = item.dataset.key;
      if (this.checked) { selectedEvents[key] = true; item.classList.add("selected"); }
      else { delete selectedEvents[key]; item.classList.remove("selected"); }
      updateSelectedCount();
    };
  }
  updateSelectedCount();
}

function updateSelectedCount() {
  var count = 0;
  for (var k in selectedEvents) { if (selectedEvents[k]) count++; }
  selectedCount.textContent = count;
}

function syncEventsFromInput() {
  selectedEvents = {};
  var parts = eventNameInput.value.split(",");
  for (var i = 0; i < parts.length; i++) {
    var v = parts[i].trim();
    if (v) selectedEvents[v] = true;
  }
}

function syncInputFromEvents() {
  var arr = [];
  for (var k in selectedEvents) { if (selectedEvents[k]) arr.push(k); }
  eventNameInput.value = arr.join(",");
}

function setEventModalOpen(open) {
  eventNameModal.className = open ? "modal active" : "modal";
  document.body.style.overflow = open ? "hidden" : "";
  if (open) { syncEventsFromInput(); renderEventCategories(); eventNameSearch.focus(); }
  else { eventNameSearch.value = ""; }
}

eventNameToggleBtn.onclick = function() {
  setEventModalOpen(!eventNameModal.classList.contains("active"));
};

eventNameModalBackdrop.onclick = function() { setEventModalOpen(false); };
eventNameModalCloseBtn.onclick = function() { setEventModalOpen(false); };
eventNameModalCancelBtn.onclick = function() { setEventModalOpen(false); };

eventNameModalConfirmBtn.onclick = function() {
  syncInputFromEvents();
  setEventModalOpen(false);
  showToast("已更新事件筛选");
};

eventNameSearch.oninput = function() { renderEventCategories(this.value); };

eventNameClearBtn.onclick = function() {
  selectedEvents = {};
  renderEventCategories(eventNameSearch.value);
  showToast("已清空选择");
};

document.onkeydown = function(e) {
  if (e.key === "Escape" && eventNameModal.classList.contains("active")) {
    setEventModalOpen(false);
  }
};

loadEnvs();
</script>
</body>
</html>
"""


@app.route("/")
def index() -> Any:
    return render_template_string(INDEX_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888, debug=True)
