DB_CONFIG = {
    "host": "123.60.11.74",
    "port": 13306,
    "user": "root",
    "password": "1234+asdf",
    "database": "restart_life_test",
    "table": "event_logs",
}

# 将本文件复制为 config.py，并把以上值替换成你实际的 MySQL 配置：
# - database: 埋点所在库名，例如 tracking_db
# - table:    埋点表名，例如 event_logs
# 要求表中至少包含以下字段（字段名可按你实际情况修改，并在 app.py 中对应调整）：
# - user_id:    用于绑定测试用户
# - created_at: 事件写入时间（DATETIME / TIMESTAMP）
# - event_name: 事件名
# - properties: 事件属性（JSON / TEXT），可选
