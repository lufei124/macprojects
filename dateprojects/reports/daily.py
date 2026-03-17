"""每日总体报表（供飞书同步拉数用，不落 CSV）"""
from datetime import datetime, timedelta
from database import get_dict_cursor, run_group_sql
from logger import get_logger


# ================= 定义表头顺序 =================
HEADERS = [
    "日期", "全部新增启动人数（仅app）", "新增同比上周", "新用户当天注册",
    "新用户当天注册转化率", "次日留存（仅app）", "海外新增启动人数",
    "国内新增启动人数", "总启动人数", "日活同比上周", "海外总启动人数",
    "国内总启动人数（APP）", "登录用户数（APP）", "登录率", "导览播报人数",
    "当天听导览用户占比", "播报次数", "人均播报次数", "播放完成次数", "完播率",
    "文物识别使用用户数", "文物识别使用次数", "文物识别点击用户占比",
    "文物识别人均使用次数", "国内会员付费用户数", "国内会员付费金额",
    "海外会员付费用户数", "海外会员付费金额"
]

logger = get_logger(__name__)


def safe_div(a, b, as_percent=False):
    """安全除法"""
    try:
        val = float(a) / float(b)
        return f"{val:.2%}" if as_percent else round(val, 2)
    except (ValueError, TypeError, ZeroDivisionError):
        return "0.00%" if as_percent else 0


class DailyReporter:
    """每日总体报表类"""

    def __init__(self, start_date=None, end_date=None):
        """初始化报表生成器

        Args:
            start_date: 开始日期，datetime对象
            end_date: 结束日期，datetime对象
        """
        self.start_date = start_date
        self.end_date = end_date

    def _ensure_date_range(self):
        """确保有有效的日期范围"""
        if self.start_date is None or self.end_date is None:
            today = datetime.now()
            self.end_date = today - timedelta(days=1)     # 昨天
            self.start_date = self.end_date

    def fetch_metric_only(self):
        """只拉取“全部新增启动人数（仅app）”用于联调"""
        logger.info("正在连接数据库（单指标模式）...")

        self._ensure_date_range()

        start_date_str = self.start_date.strftime("%Y-%m-%d")
        end_date_str = self.end_date.strftime("%Y-%m-%d")

        days = (self.end_date - self.start_date).days + 1
        logger.info("开启%s日跑批（单指标），提取 %s 到 %s 的数据...", days, start_date_str, end_date_str)

        # 预先构建日期 -> 指标字典（只保留日期和全部新增启动人数）
        all_data_dict = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            date_key = current_date.strftime("%Y-%m-%d")
            all_data_dict[date_key] = {
                "日期": date_key,
                "全部新增启动人数（仅app）": 0,
            }
            current_date += timedelta(days=1)

        try:
            with get_dict_cursor() as cursor:
                logger.info("[1/1] 计算全部新增启动人数（仅app）...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, count(DISTINCT device_id) as val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
            and `event` in ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
            AND SOURCE <> 'test' AND platform in ('ios','android') and channel != ''
            AND DATE(created_at) = DATE(device_activated_at)
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row["log_date"])
                    if date_key in all_data_dict:
                        all_data_dict[date_key]["全部新增启动人数（仅app）"] = row["val"]
        except Exception as e:
            logger.exception("每日总体报表（单指标）生成失败: %s", e)
            raise RuntimeError("每日总体报表（单指标）生成失败") from e

        sorted_dates = sorted(all_data_dict.keys())
        return [all_data_dict[d] for d in sorted_dates]

    def fetch_data(self):
        """从数据库拉取数据并返回 list[dict]（不落地 CSV）"""
        logger.info("正在连接数据库...")

        self._ensure_date_range()

        start_date_str = self.start_date.strftime("%Y-%m-%d")
        end_date_str = self.end_date.strftime("%Y-%m-%d")

        # 计算天数
        days = (self.end_date - self.start_date).days + 1
        logger.info("开启%s日跑批，提取 %s 到 %s 的数据...", days, start_date_str, end_date_str)

        # 预先构建一个日期 -> 指标字典
        all_data_dict = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            date_key = current_date.strftime("%Y-%m-%d")
            all_data_dict[date_key] = {header: "" for header in HEADERS}
            all_data_dict[date_key]["日期"] = date_key
            all_data_dict[date_key]["全部新增启动人数（仅app）"] = 0
            all_data_dict[date_key]["新用户当天注册"] = 0
            all_data_dict[date_key]["海外新增启动人数"] = 0
            all_data_dict[date_key]["总启动人数"] = 0
            all_data_dict[date_key]["海外总启动人数"] = 0
            all_data_dict[date_key]["登录用户数（APP）"] = 0
            all_data_dict[date_key]["导览播报人数"] = 0
            all_data_dict[date_key]["播报次数"] = 0
            all_data_dict[date_key]["播放完成次数"] = 0
            current_date += timedelta(days=1)

        try:
            with get_dict_cursor() as cursor:
                # ================= 开始执行 =================
                logger.info("[1/11] 计算全部新增启动人数（仅app）...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, count(DISTINCT device_id) as val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
            and `event` in ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
            AND SOURCE <> 'test' AND platform in ('ios','android') and channel != ''
            AND DATE(created_at) = DATE(device_activated_at)
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["全部新增启动人数（仅app）"] = row['val']

                logger.info("[2/11] 计算新用户当天注册...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, count(DISTINCT device_id) as val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
            and `event` in ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
            AND SOURCE <> 'test' AND platform in ('ios','android') and channel != ''
            AND DATE(created_at) = DATE(device_activated_at) and user_id is not null
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["新用户当天注册"] = row['val']

                logger.info("[3/11] 计算次日留存（仅app）...")
                res = run_group_sql(cursor, f"""
            WITH new_users AS (
                SELECT DATE(created_at) AS active_date, device_id FROM t_tracking_logs
                WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH') AND SOURCE <> 'test'
                AND platform IN ('ios','android') AND channel <> '' AND DATE(created_at) = DATE(device_activated_at)
            ),
            next_day_active AS (
                SELECT n.active_date, COUNT(DISTINCT l.device_id) AS retained_users
                FROM new_users n JOIN t_tracking_logs l ON n.device_id = l.device_id
                AND DATE(l.created_at) = DATE_ADD(n.active_date, INTERVAL 1 DAY)
                AND l.`event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH') AND l.SOURCE <> 'test'
                AND l.platform IN ('ios','android') AND l.channel <> '' GROUP BY n.active_date
            )
            SELECT n.active_date as log_date, CONCAT(ROUND(IFNULL(a.retained_users, 0) * 100.0 / NULLIF(COUNT(DISTINCT n.device_id), 0), 2), '%%') AS rate
            FROM new_users n LEFT JOIN next_day_active a ON n.active_date = a.active_date GROUP BY n.active_date;
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["次日留存（仅app）"] = row['rate']

                logger.info("[4/11] 计算海外新增启动人数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) as val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND DATE(created_at) = DATE(device_activated_at)
            AND region IN ('google') and `event` in ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
            AND SOURCE <> 'test' AND platform in ('ios','android') and channel != ''
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["海外新增启动人数"] = row['val']

                logger.info("[5/11] 计算总启动人数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) AS val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND device_activated_at IS NOT NULL
            AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH') AND SOURCE <> 'test'
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["总启动人数"] = row['val']

                logger.info("[6/11] 计算海外总启动人数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) AS val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND device_activated_at IS NOT NULL
            AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH') AND SOURCE <> 'test' AND region = 'google'
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["海外总启动人数"] = row['val']

                logger.info("[7/11] 计算登录用户数（APP）...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) AS val FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND device_activated_at IS NOT NULL
            AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH') AND SOURCE <> 'test' AND user_id IS NOT NULL
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["登录用户数（APP）"] = row['val']

                logger.info("[8/11] 计算导览播报人数和播报次数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) AS u_count, COUNT(*) AS e_count FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND event in ('GUIDE_PLAY','GUIDE_FINISH')
            AND SOURCE <> 'test' AND device_activated_at IS NOT NULL
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict:
                        all_data_dict[date_key]["导览播报人数"] = row['u_count']
                        all_data_dict[date_key]["播报次数"] = row['e_count']

                logger.info("[9/11] 计算播放完成次数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(*) AS e_count FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND JSON_EXTRACT(payload, '$.endType') + 0 = 0
            AND event in ('GUIDE_FINISH') AND SOURCE <> 'test' AND device_activated_at IS NOT NULL
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict: all_data_dict[date_key]["播放完成次数"] = row['e_count']

                logger.info("[10/11] 计算文物识别使用数据...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT device_id) AS u_count, COUNT(*) AS e_count FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND event = 'MAIN_OBJECT_REQUEST'
            AND source <> 'test' AND device_activated_at IS NOT NULL
            AND ((platform IN ('ios','android','h5') AND channel <> '') OR platform = 'weapp')
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict:
                        all_data_dict[date_key]["文物识别使用用户数"] = row['u_count']
                        all_data_dict[date_key]["文物识别使用次数"] = row['e_count']

                logger.info("[11/11] 计算会员付费数据...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT user_id) AS u_count, SUM(JSON_EXTRACT(payload, '$.amount')/100) AS total
            FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND event = 'PAY_SUCCESS'
            and (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.istest')) ='false' or JSON_UNQUOTE(JSON_EXTRACT(payload, '$.istest')) is null)
            and JSON_UNQUOTE(JSON_EXTRACT(payload, '$.paymentMethod')) <> 'BEST_PAY'
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict:
                        all_data_dict[date_key]["国内会员付费用户数"] = row['u_count'] or 0
                        all_data_dict[date_key]["国内会员付费金额"] = round(row['total'] or 0, 2)

                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) as log_date, COUNT(DISTINCT user_id) AS u_count, SUM(JSON_EXTRACT(payload, '$.amount')/100) AS total
            FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}' AND event = 'PAY_SUCCESS' AND Region='Google'
            GROUP BY DATE(created_at)
        """)
                for row in res:
                    date_key = str(row['log_date'])
                    if date_key in all_data_dict:
                        all_data_dict[date_key]["海外会员付费用户数"] = row['u_count'] or 0
                        all_data_dict[date_key]["海外会员付费金额"] = round(row['total'] or 0, 2)
        except Exception as e:
            logger.exception("每日总体报表生成失败: %s", e)
            raise RuntimeError("每日总体报表生成失败") from e

        logger.info("数据库提取完毕，正在执行公式计算...")

        # 将字典转为按日期排序的列表，并执行二次公式计算
        final_data_list = []
        sorted_dates = sorted(all_data_dict.keys())

        for d in sorted_dates:
            row = all_data_dict[d]
            if not row.get("次日留存（仅app）"):
                row["次日留存（仅app）"] = "0.00%"

            # 二次计算公式
            row["国内新增启动人数"] = row["全部新增启动人数（仅app）"] - row.get("海外新增启动人数", 0)
            row["新用户当天注册转化率"] = safe_div(row["新用户当天注册"], row["全部新增启动人数（仅app）"], True)
            row["国内总启动人数（APP）"] = row["总启动人数"] - row.get("海外总启动人数", 0)
            row["登录率"] = safe_div(row["登录用户数（APP）"], row["总启动人数"], True)
            row["当天听导览用户占比"] = safe_div(row["导览播报人数"], row["总启动人数"], True)
            row["完播率"] = safe_div(row["播放完成次数"] or 0, row["播报次数"], True)
            row["人均播报次数"] = safe_div(row["播报次数"], row["导览播报人数"])
            row["文物识别人均使用次数"] = safe_div(row["文物识别使用次数"] or 0, row.get("文物识别使用用户数", 0))
            row["文物识别点击用户占比"] = safe_div(row.get("文物识别使用用户数", 0), row["总启动人数"], True)

            final_data_list.append(row)

        return final_data_list

    def to_rows_for_feishu(self, data_list=None):
        """将内部数据转换为飞书二维数组行，按 HEADERS 顺序"""
        if data_list is None:
            data_list = self.fetch_data()
        rows = []
        for row in data_list:
            rows.append([row.get(h, "") for h in HEADERS])
        return rows


if __name__ == "__main__":
    reporter = DailyReporter()
    reporter.fetch_data()
