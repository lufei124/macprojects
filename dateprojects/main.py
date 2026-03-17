"""统一入口脚本：从数据库拉取日常/渠道数据并写入飞书电子表格"""
import sys
import argparse
from datetime import datetime, timedelta
from config import (
    FEISHU_SHEET_TOKEN,
    FEISHU_SHEET_ID_DAILY,
    FEISHU_SHEET_ID_CHANNEL,
)
from reports import DailyReporter, ChannelReporter
from reports.daily import HEADERS as DAILY_HEADERS
from reports.channel import HEADERS as CHANNEL_HEADERS
from logger import get_logger

logger = get_logger(__name__)


def main():
    """主入口函数"""
    parser = argparse.ArgumentParser(
        description='从数据库拉取日常/渠道数据并写入飞书电子表格',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                          # 同步昨日数据到飞书（默认）
  python main.py feishu-sync --days 8     # 同步最近 8 天
  python main.py feishu-sync --date 2026-03-01
  python main.py feishu-sync --channel-only --days 8   # 仅同步渠道表
  python main.py scheduler --time 08:00    # 定时任务（每天 08:00 执行飞书同步）
        """
    )

    parser.add_argument(
        'report_type',
        nargs='?',
        default='feishu-sync',
        choices=['feishu-sync', 'scheduler'],
        help='模式: feishu-sync 同步到飞书 (默认), scheduler 定时任务'
    )

    parser.add_argument(
        '--time',
        dest='time',
        help='定时任务执行时间，格式 HH:MM (例如 08:00)，仅 scheduler 时有效'
    )

    # 日期相关参数
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        '--date',
        dest='date',
        help='指定日期，格式 YYYY-MM-DD (默认: 昨天)'
    )
    date_group.add_argument(
        '--days',
        type=int,
        default=1,
        help='查询最近N天的数据 (默认: 1)'
    )
    date_group.add_argument(
        '--start-date',
        dest='start_date',
        help='开始日期，格式 YYYY-MM-DD'
    )

    parser.add_argument(
        '--end-date',
        dest='end_date',
        help='结束日期，格式 YYYY-MM-DD (默认: 昨天)'
    )

    # 飞书同步范围（仅 feishu-sync 时有效）
    parser.add_argument(
        '--channel-only',
        action='store_true',
        dest='channel_only',
        help='仅同步渠道表（不同步日常表），用于验证渠道写入'
    )
    parser.add_argument(
        '--daily-only',
        action='store_true',
        dest='daily_only',
        help='仅同步日常表（不同步渠道表）'
    )

    args = parser.parse_args()

    try:
        if args.report_type == 'scheduler':
            run_scheduler(args)
        else:
            run_feishu_sync(args)
    except Exception as e:
        logger.exception("执行失败: %s", e)
        sys.exit(1)


def parse_date_range(args):
    """解析日期参数，返回 (start_date, end_date)"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # 如果指定了 start_date，则使用 start_date 和 end_date
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        else:
            end_date = start_date
    # 如果指定了 --date 参数
    elif args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d")
        end_date = start_date
    # 默认使用 --days 参数
    else:
        end_date = yesterday
        start_date = today - timedelta(days=args.days)

    return start_date, end_date


def run_feishu_sync(args):
    """从数据库增量同步数据到飞书电子表格"""
    from feishu_client import (
        FEISHU_SHEET_TOKEN as TOKEN,
        upsert_metric_by_date,
        upsert_metric_by_date_channel,
    )

    start_date, end_date = parse_date_range(args)

    logger.info("=" * 50)
    logger.info("开始执行飞书同步")
    logger.info("日期范围: %s ~ %s", start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    logger.info("=" * 50)

    if not TOKEN:
        logger.error("FEISHU_SHEET_TOKEN 未配置，无法同步到飞书")
        sys.exit(1)

    run_daily = (not args.channel_only) and FEISHU_SHEET_ID_DAILY
    run_channel = (not args.daily_only) and FEISHU_SHEET_ID_CHANNEL

    # 每日总体报表（按“日期”逐字段更新）
    if run_daily:
        daily = DailyReporter(start_date, end_date)
        # 拉取完整每日数据（包含全部衍生指标），然后按字段逐列写入飞书
        daily_data = daily.fetch_data()
        daily_rows = daily.to_rows_for_feishu(daily_data)

        metric_fields = [
            "全部新增启动人数（仅app）",   # B
            "新用户当天注册",           # D
            "次日留存（仅app）",        # F
            "海外新增启动人数",         # G
            "国内新增启动人数",         # H
            "总启动人数",              # I
            # "日活同比上周",          # J（暂不写入）
            "海外总启动人数",          # K
            "国内总启动人数（APP）",   # L
            "登录用户数（APP）",       # M
            "导览播报人数",            # O
            "播报次数",                # Q
            "播放完成次数",            # S
            "文物识别使用用户数",      # U
            "文物识别使用次数",        # V
            "文物识别人均使用次数",    # X
            "国内会员付费用户数",      # Y
            "国内会员付费金额",        # Z
            "海外会员付费用户数",      # AF
            "海外会员付费金额",        # AG
        ]

        for field_name in metric_fields:
            try:
                col_idx = DAILY_HEADERS.index(field_name)
            except ValueError:
                logger.warning("在 DAILY_HEADERS 中找不到字段: %s", field_name)
                continue

            # 构造仅包含“日期 + 当前指标”的二维数组
            metric_rows = []
            for row in daily_rows:
                if not row:
                    continue
                # 没拉到的数据统一写入 0（保留百分号等字符串原样）
                val = row[col_idx]
                if val in (None, ""):
                    val = 0
                metric_rows.append([row[0], val])

            upsert_metric_by_date(
                TOKEN,
                FEISHU_SHEET_ID_DAILY,
                metric_rows,
                date_col_index_in_rows=0,
                metric_col_index_in_rows=1,
                metric_header=field_name,
            )
            logger.info(
                "已向飞书每日 Sheet 按日期更新字段“%s”，共 %s 行。",
                field_name,
                len(metric_rows),
            )

    # 渠道报表（按表内 A 列日期 + C 列渠道号匹配行，写入 D/E/F/I/K/L）
    if run_channel:
        channel = ChannelReporter(start_date, end_date)
        channel_data = channel.fetch_data()
        channel_rows = channel.to_rows_for_feishu(channel_data)

        metric_fields_channel = [
            "活跃用户数",          # C
            "新增用户数",          # D
            "新增听导览用户数",    # E
            "当天听导览总用户数",  # F / I（以飞书表头为准）
            "会员付费次数",        # K
            "会员付费金额",        # L
        ]

        for field_name in metric_fields_channel:
            try:
                col_idx = CHANNEL_HEADERS.index(field_name)
            except ValueError:
                logger.warning("在 CHANNEL_HEADERS 中找不到字段: %s", field_name)
                continue

            metric_rows = []
            for row in channel_rows:
                if not row:
                    continue
                date_val = row[0]
                channel_val = row[1]
                val = row[col_idx]
                if val in (None, ""):
                    val = 0
                metric_rows.append([date_val, channel_val, val])

            upsert_metric_by_date_channel(
                TOKEN,
                FEISHU_SHEET_ID_CHANNEL,
                metric_rows,
                date_col_index_in_rows=0,
                channel_col_index_in_rows=1,
                metric_col_index_in_rows=2,
                metric_header=field_name,
            )
            logger.info(
                "已向飞书渠道 Sheet 按 (日期, 渠道) 更新字段“%s”，共 %s 行。",
                field_name,
                len(metric_rows),
            )

    logger.info("=" * 50)
    logger.info("飞书同步完成")
    logger.info("=" * 50)


def run_scheduler(args):
    """定时任务模式：每天指定时间执行飞书同步"""
    import time

    if not args.time:
        logger.error("定时任务模式需要指定 --time 参数，例如 --time 08:00")
        sys.exit(1)

    target_hour, target_minute = map(int, args.time.split(':'))

    logger.info("定时任务已启动，将在每天 %s 执行飞书同步。按 Ctrl+C 停止。", args.time)

    while True:
        now = datetime.now()

        if now.hour == target_hour and now.minute == target_minute:
            logger.info("=" * 50)
            logger.info("到达执行时间: %s", now.strftime('%Y-%m-%d %H:%M:%S'))
            logger.info("=" * 50)
            run_feishu_sync(args)
            logger.info("=" * 50)
            logger.info("本次执行完成，等待下次执行")
            logger.info("=" * 50)
            time.sleep(60)
        else:
            time.sleep(30)


if __name__ == "__main__":
    main()
