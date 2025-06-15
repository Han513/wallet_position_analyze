import datetime
import logging
from history_full_patch import main as history_main
from realtime_consumer import run_realtime_analysis

def run_full_analysis(start_date, end_date):
    logging.info(f"[PIPELINE] 全量分析: {start_date} ~ {end_date}")
    history_main(start_date, end_date)

def run_patch_analysis(start_date, end_date):
    logging.info(f"[PIPELINE] 補全分析: {start_date} ~ {end_date}")
    history_main(start_date, end_date)

def main():
    today = datetime.date.today()
    full_range_start = today - datetime.timedelta(days=30)
    full_range_end = today

    # 1. 全量分析（同步执行）
    run_full_analysis(full_range_start, full_range_end)

    # 2. 快照全量分析结束时的日期
    snapshot_day = full_range_end
    patch_end = snapshot_day - datetime.timedelta(days=1)
    patch_start = full_range_start

    # 3. 补全分析（同步执行）
    run_patch_analysis(patch_start, patch_end)

    # 4. 实时分析（同步执行）
    realtime_start = snapshot_day
    run_realtime_analysis(realtime_start)

if __name__ == '__main__':
    main() 