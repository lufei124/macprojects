# 数据报表同步工具

从数据库拉取日常数据与渠道数据，并写入飞书电子表格（每日总体报表 Sheet + 渠道报表 Sheet）。不生成本地 CSV。

## 目录结构

```
dateprojects/
├── config.py          # 公共配置（数据库、飞书）
├── database.py        # 数据库连接和查询封装
├── logger.py          # 统一日志配置
├── utils.py           # 公共工具
├── reports/
│   ├── __init__.py
│   ├── daily.py       # 每日总体报表（拉数供飞书写入）
│   └── channel.py     # 渠道报表（拉数供飞书写入）
├── feishu_client.py   # 飞书表格读写与同步逻辑
├── main.py            # 统一入口（feishu-sync / scheduler）
├── requirements.txt   # Python 依赖
├── .env.example      # 环境变量示例
└── README.md
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env` 填写数据库连接信息：

```dotenv
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=your_user
DB_PASS=your_password
DB_NAME=your_database
```

## 使用方法

### 同步到飞书（默认）

不写子命令时默认执行飞书同步（同步昨日）；也可显式写 `feishu-sync`：

```bash
# 同步昨日（默认）
python3 main.py

# 同步最近 N 天
python3 main.py --days 8

# 同步指定某一天
python3 main.py --date 2026-03-01

# 同步指定日期区间
python3 main.py --start-date 2026-03-01 --end-date 2026-03-05

# 仅同步渠道表
python3 main.py --channel-only --days 8

# 仅同步日常表
python3 main.py --daily-only --days 8
```

### 定时任务

每天指定时间自动执行飞书同步：

```bash
python3 main.py scheduler --time 08:00
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `report_type` | `feishu-sync` 同步到飞书，`scheduler` 定时任务 | `feishu-sync` |
| `--time` | 定时任务执行时间，格式 HH:MM | - |
| `--date` | 指定日期，格式 YYYY-MM-DD | 昨天 |
| `--days` | 查询最近 N 天的数据 | 1 |
| `--start-date` | 开始日期，格式 YYYY-MM-DD | - |
| `--end-date` | 结束日期，格式 YYYY-MM-DD | 昨天 |
| `--channel-only` | 仅同步渠道表 | 否 |
| `--daily-only` | 仅同步日常表 | 否 |

## 配置说明

项目通过环境变量配置（`.env`）：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_HOST` | 数据库地址 | 必填 |
| `DB_PORT` | 数据库端口 | `3306` |
| `DB_USER` | 数据库用户名 | 必填 |
| `DB_PASS` | 数据库密码 | 必填 |
| `DB_NAME` | 数据库名 | 必填 |
| `TARGET_CHANNELS` | 渠道列表（逗号分隔），用于渠道报表 | `200001,200002,200008,200009,200011` |
| `LOG_LEVEL` | 日志级别 | `INFO` |
| `FEISHU_APP_ID` | 飞书应用 App ID | - |
| `FEISHU_APP_SECRET` | 飞书应用 App Secret | - |
| `FEISHU_SHEET_TOKEN` | 目标电子表格 token | - |
| `FEISHU_SHEET_ID_DAILY` | 每日总体报表 Sheet ID | - |
| `FEISHU_SHEET_ID_CHANNEL` | 渠道报表 Sheet ID | - |
| `FEISHU_DAILY_BASE_DATE` | 日常表基准日期（如 `2024-10-01` 在 A2） | `2024-10-01` |
| `FEISHU_DAILY_BASE_ROW` | 日常表基准行号（如 A2 则为 2） | `2` |
| `FEISHU_CHANNEL_BASE_DATE` | 渠道表基准日期（可选；当前渠道表按表内 A/C 列匹配行，不再用此算行号） | `2025-02-01` |
| `FEISHU_CHANNEL_BASE_ROW` | 渠道表基准行号（可选；同上） | `2` |

## 飞书机器人接入

1. 在飞书开放平台创建自建应用，获取 **App ID / App Secret**，并在权限中勾选电子表格读取/写入相关权限。
2. 在飞书中创建电子表格，获取其 `spreadsheetToken` 以及各 Sheet 的 `sheetId`，填入 `.env` 中：

```dotenv
FEISHU_APP_ID=cli_xxxxxxxx
FEISHU_APP_SECRET=your_app_secret
FEISHU_SHEET_TOKEN=your_spreadsheet_token
FEISHU_SHEET_ID_DAILY=your_daily_sheet_id
FEISHU_SHEET_ID_CHANNEL=your_channel_sheet_id
```

3. 运行同步命令（示例）：

```bash
# 同步最近 8 天到飞书（每日 + 渠道）
python3 main.py --days 8

# 同步指定日期范围
python3 main.py --start-date 2026-03-08 --end-date 2026-03-15
```

当前同步策略为：从远程数据库拉取指定日期区间的数据，按现有报表逻辑计算后，**仅按字段更新对应单元格**：

- **日常表**：以 `FEISHU_DAILY_BASE_DATE / FEISHU_DAILY_BASE_ROW` 为基准，按日期定位行，再按表头中文名定位列，逐字段写入（例如：`全部新增启动人数（仅app）`→B 列等）。
- **渠道表**：先读取表内 **A 列日期 + C 列渠道号**，构建 (日期, 渠道号) → 行号 索引；再根据该索引匹配待写数据，仅对表内存在的 (日期, 渠道) 行写入对应列（按表头中文名定位列：`活跃用户数`→D、`新增用户数`→E、`新增听导览用户数`→F、`当天听导览总用户数`→I、`会员付费次数`→K、`会员付费金额`→L）。表内未出现的 (日期, 渠道) 会跳过不写。

没拉到的数据会写成 0（已经带百分号等格式的字符串会保持原样写入）。

## 注意事项

1. 首次使用请确保数据库连接正常
2. 定时任务模式会持续运行，按 `Ctrl+C` 停止
3. 建议使用 `screen` 或 `tmux` 在后台运行定时任务
