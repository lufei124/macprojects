# AGENTS.md - 数据报表同步工具

本文件为 AI 编码代理提供项目上下文和开发规范。

## 项目概述

Python 项目，用于从 MySQL 数据库拉取日常数据与渠道数据，并写入飞书电子表格。

## 目录结构

```
dateprojects/
├── config.py          # 公共配置（数据库、飞书凭证）
├── database.py        # 数据库连接和查询封装
├── logger.py          # 统一日志配置
├── utils.py           # 公共工具函数
├── feishu_client.py   # 飞书表格读写与同步逻辑
├── main.py            # 统一入口脚本
├── reports/
│   ├── __init__.py
│   ├── daily.py       # 每日总体报表
│   └── channel.py     # 渠道报表
└── requirements.txt   # Python 依赖
```

## 环境与依赖

- Python 3.x
- 依赖：`pymysql>=1.1.0`, `python-dotenv>=1.0.0`, `requests>=2.32.0`
- 安装依赖：`pip install -r requirements.txt`
- 配置：`cp .env.example .env` 并填写数据库和飞书凭证

## 运行命令

```bash
# 同步昨日数据到飞书（默认）
python3 main.py

# 同步最近 N 天
python3 main.py --days 8

# 同步指定日期
python3 main.py --date 2026-03-01

# 同步指定日期区间
python3 main.py --start-date 2026-03-01 --end-date 2026-03-05

# 仅同步渠道表
python3 main.py --channel-only --days 8

# 定时任务模式
python3 main.py scheduler --time 08:00
```

## 测试

项目当前**无自动化测试框架**。添加测试时请：
- 使用 `pytest` 作为测试框架
- 测试文件命名 `test_*.py`
- 运行单个测试：`pytest tests/test_<module>.py::<test_name> -v`

## 代码风格规范

### 导入顺序
1. Python 标准库（`sys`, `os`, `time` 等）
2. 第三方库（`pymysql`, `requests`, `dotenv`）
3. 项目内部模块（`config`, `logger`, `database` 等）

各组之间空一行分隔。优先使用绝对导入。

### 命名约定
- **函数/变量/模块**：`snake_case`（如 `get_connection`, `feishu_client`）
- **类名**：`PascalCase`（如 `FeishuConfigError`, `DailyReporter`）
- **常量/配置项**：`UPPER_SNAKE_CASE`（如 `DB_HOST`, `FEISHU_APP_ID`）
- **私有函数/变量**：前缀 `_`（如 `_get_env`, `_normalize_date`）
- **中文注释和文档字符串**：直接使用中文编写 docstring 和注释

### 类型注解
- 在函数签名中使用类型注解（如 `def get_tenant_access_token() -> str:`）
- 复杂类型从 `typing` 模块导入（`Dict`, `List`, `Tuple`）
- 使用 `from __future__ import annotations` 支持延迟求值

### 错误处理
- 自定义异常类继承 `RuntimeError` 或 `ValueError`
- 日志记录使用 `logger.exception()` 记录完整堆栈
- 环境变量缺失时抛出 `ValueError` 并给出明确提示
- API 调用后检查返回的 `code` 字段，非 0 时抛出异常

### 日志规范
- 使用 `logger.py` 中的 `get_logger(__name__)` 获取 logger
- 格式：`%(asctime)s | %(levelname)s | %(name)s | %(message)s`
- 关键操作使用 `logger.info()`，异常使用 `logger.exception()`

### 资源管理
- 数据库连接使用 `@contextmanager` 装饰器管理生命周期
- 确保 `cursor.close()` 和 `conn.close()` 在 finally 块中调用

### 文件头
每个模块以三引号 docstring 开头，简要说明模块用途。

## 环境变量清单

必须配置：`DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME`
飞书相关：`FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `FEISHU_SHEET_TOKEN`
可选：`DB_PORT`（默认 3306）、`LOG_LEVEL`（默认 INFO）、`TARGET_CHANNELS`

## 注意事项

- 本项目无 Cursor rules 或 Copilot rules 配置文件
- 代码注释和文档使用中文
- 飞书 API 调用需配置 `timeout` 参数
- 敏感信息通过 `.env` 文件管理，切勿提交到代码库
