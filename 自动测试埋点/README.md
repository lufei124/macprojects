# 埋点实时查看小工具

本工具用于在本地实时查看 MySQL 中某个 `user_id` 对应的埋点数据，方便你在手机上走流程时验证埋点是否成功落库。

## 1. 安装依赖

在该目录下执行：

```bash
pip install -r requirements.txt
```

建议使用虚拟环境（`venv` / `conda` 等）。

## 2. 配置数据库连接

1. 复制示例配置文件：

```bash
cp config.example.py config.py
```

2. 编辑 `config.py`，根据你实际的 MySQL 配置进行修改：

- `host` / `port` / `user` / `password`
- `database`：埋点所在库名，例如 `tracking_db`
- `table`：埋点表名，例如 `event_logs`

表中至少需要有以下字段（字段名可按你实际情况修改，然后到 `backend_app.py` 中同步修改 SQL 里的字段名）：

- `user_id`：用来绑定测试用户
- `created_at`：事件写入时间（`DATETIME` / `TIMESTAMP`）
- `event_name`：事件名
- `properties`：事件属性（`JSON` / `TEXT`），可选

## 3. 启动服务

在该目录下执行：

```bash
python backend_app.py
```

默认会在 `http://127.0.0.1:5000` 启动一个本地 Web 服务。

## 4. 使用方式

1. 浏览器打开 `http://127.0.0.1:5000`。
2. 在页面顶部输入框里填写你本次测试用的 `user_id`。
3. 设置轮询间隔（秒），默认 3 秒；可以根据数据库压力适当调大或调小。
4. 点击「开始监听」，保持浏览器页面打开。
5. 拿起手机，按业务流程操作；每隔一段时间，页面会自动向 `/events` 接口拉取该 `user_id` 的最新埋点并展示在表格中。

表格字段说明（假设字段名与默认配置一致）：

- **时间**：`created_at`，按时间倒序，最新在最上方。
- **user_id**：当前绑定的用户 ID。
- **事件名**：`event_name`。
- **属性**：`properties` 字段的内容，原始 JSON / 文本。

## 5. 注意事项

- 为避免对数据库造成过大压力，接口层对 `limit` 做了上限（默认 100，最大 500），轮询间隔建议不要小于 1 秒。
- 如果你的表结构字段名与示例不一致，请：
  - 修改 `config.py` 中的库名、表名；
  - 然后打开 `backend_app.py`，把 SQL 语句中的字段名（`user_id` / `created_at` / `event_name` / `properties`）替换成你实际的字段名。

## 6. 简单接口说明

- `GET /health`
  - 用于健康检查，返回 `{"status": "ok"}`。
- `GET /events`
  - 查询参数：
    - `user_id`：必填，仅返回该用户的埋点。
    - `limit`：可选，返回条数，默认 100。
    - `since_time`：可选，ISO 格式时间字符串，仅返回该时间之后的数据（当前前端为简化逻辑，每次会整体刷新表格）。

