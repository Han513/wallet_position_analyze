import logging
import os
# from analyze_kafka_optimized import main as kafka_main

def run_realtime_analysis(start_time):
    logging.info(f"[REALTIME] 啟動Kafka消費: {start_time} 起")
    os.environ['KAFKA_START_TIME'] = str(start_time)
    # kafka_main() 