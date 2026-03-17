"""渠道报表（供飞书同步拉数用，不落 CSV）"""
from datetime import datetime, timedelta
from config import TARGET_CHANNELS
from database import get_dict_cursor, run_group_sql
from logger import get_logger


# ================= 定义表头顺序 =================
HEADERS = [
    "日期", "渠道号", "活跃用户数", "新增用户数",
    "新增听导览用户数", "当天听导览总用户数", "会员付费次数", "会员付费金额"
]

logger = get_logger(__name__)


class ChannelReporter:
    """渠道报表类"""

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

    def fetch_data(self):
        """从数据库拉取数据并返回 list[dict]（不落地 CSV）"""
        logger.info("正在连接数据库...")

        self._ensure_date_range()

        start_date_str = self.start_date.strftime("%Y-%m-%d")
        end_date_str = self.end_date.strftime("%Y-%m-%d")

        # 计算天数
        days = (self.end_date - self.start_date).days + 1
        logger.info("开启渠道报表 %s 日跑批，提取 %s 到 %s 的数据...", days, start_date_str, end_date_str)

        # 预先构建结构
        all_data_dict = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            date_key = current_date.strftime("%Y-%m-%d")
            all_data_dict[date_key] = {}
            for ch in TARGET_CHANNELS:
                all_data_dict[date_key][ch] = {header: 0 for header in HEADERS}
                all_data_dict[date_key][ch]["日期"] = date_key
                all_data_dict[date_key][ch]["渠道号"] = ch
            current_date += timedelta(days=1)

        # ================= 开始执行 =================
        channel_list = "', '".join(TARGET_CHANNELS)

        try:
            with get_dict_cursor() as cursor:
                logger.info("[1/5] 计算活跃用户数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) AS log_date, custom_channel, COUNT(DISTINCT device_id) AS val
            FROM t_tracking_logs
            WHERE platform = 'android'
              AND DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND custom_channel IN ('{channel_list}')
              AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
              AND SOURCE <> 'test'
              AND device_activated_at IS NOT NULL
            GROUP BY DATE(created_at), custom_channel
        """)
                for row in res:
                    d, ch = str(row['log_date']), str(row['custom_channel'])
                    if d in all_data_dict and ch in all_data_dict[d]:
                        all_data_dict[d][ch]["活跃用户数"] = row['val']

                logger.info("[2/5] 计算新增用户数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) AS log_date, custom_channel, COUNT(DISTINCT device_id) AS val
            FROM t_tracking_logs
            WHERE platform = 'android'
              AND DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND DATE(created_at) = DATE(device_activated_at)
              AND custom_channel IN ('{channel_list}')
              AND `event` IN ('PAGE_VIEW','PAGE_EXIT','APP_LAUNCH')
              AND SOURCE <> 'test'
            GROUP BY DATE(created_at), custom_channel
        """)
                for row in res:
                    d, ch = str(row['log_date']), str(row['custom_channel'])
                    if d in all_data_dict and ch in all_data_dict[d]:
                        all_data_dict[d][ch]["新增用户数"] = row['val']

                logger.info("[3/5] 计算新增听导览用户数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) AS log_date, custom_channel, COUNT(DISTINCT device_id) AS val
            FROM t_tracking_logs
            WHERE event IN ('GUIDE_PLAY','GUIDE_FINISH')
              AND platform = 'android'
              AND DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND DATE(created_at) = DATE(device_activated_at)
              AND custom_channel IN ('{channel_list}')
              AND SOURCE <> 'test'
            GROUP BY DATE(created_at), custom_channel
        """)
                for row in res:
                    d, ch = str(row['log_date']), str(row['custom_channel'])
                    if d in all_data_dict and ch in all_data_dict[d]:
                        all_data_dict[d][ch]["新增听导览用户数"] = row['val']

                logger.info("[4/5] 计算当天听导览总用户数...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) AS log_date, custom_channel, COUNT(DISTINCT device_id) AS val
            FROM t_tracking_logs
            WHERE event IN ('GUIDE_PLAY','GUIDE_FINISH')
              AND platform = 'android'
              AND DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND custom_channel IN ('{channel_list}')
              AND SOURCE <> 'test'
            GROUP BY DATE(created_at), custom_channel
        """)
                for row in res:
                    d, ch = str(row['log_date']), str(row['custom_channel'])
                    if d in all_data_dict and ch in all_data_dict[d]:
                        all_data_dict[d][ch]["当天听导览总用户数"] = row['val']

                logger.info("[5/5] 计算会员付费次数和会员付费金额...")
                res = run_group_sql(cursor, f"""
            SELECT DATE(created_at) AS log_date, custom_channel,
                   COUNT(*) AS u_count,
                   SUM(JSON_EXTRACT(payload, '$.amount')/100) AS total_amount
            FROM t_tracking_logs
            WHERE DATE(created_at) BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND event = 'PAY_SUCCESS'
              AND custom_channel IN ('{channel_list}')
              AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.paymentMethod')) <> 'BEST_PAY'
              AND (JSON_UNQUOTE(JSON_EXTRACT(payload, '$.istest')) ='false' OR JSON_UNQUOTE(JSON_EXTRACT(payload, '$.istest')) IS NULL)
            GROUP BY DATE(created_at), custom_channel
        """)
                for row in res:
                    d, ch = str(row['log_date']), str(row['custom_channel'])
                    if d in all_data_dict and ch in all_data_dict[d]:
                        all_data_dict[d][ch]["会员付费次数"] = row['u_count'] or 0
                        all_data_dict[d][ch]["会员付费金额"] = round(row['total_amount'] or 0, 2)
        except Exception as e:
            logger.exception("渠道报表生成失败: %s", e)
            raise RuntimeError("渠道报表生成失败") from e

        logger.info("数据库提取完毕，正在组装表格...")

        # 平铺成一维列表
        final_data_list = []
        sorted_dates = sorted(all_data_dict.keys())

        for d in sorted_dates:
            for ch in TARGET_CHANNELS:
                final_data_list.append(all_data_dict[d][ch])

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
    reporter = ChannelReporter()
    reporter.fetch_data()
