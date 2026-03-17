"""数据库连接和查询封装"""
from contextlib import contextmanager

import pymysql
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
from logger import get_logger

logger = get_logger(__name__)


def get_connection():
    """获取数据库连接"""
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
    )


@contextmanager
def get_dict_cursor():
    """获取 DictCursor，并自动管理连接生命周期"""
    conn = get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


def run_group_sql(cursor, sql, params=None):
    """执行带有 GROUP BY 的批量查询，一次性查回多天数据"""
    try:
        cursor.execute(sql, params or ())
        return cursor.fetchall()
    except Exception as e:
        logger.exception("SQL执行错误: %s", e)
        raise
