import time
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lit, desc
from scripts.etl import (
    extract_data,
    transform_data,
    filter_relevent_kickstarters,
    build_dimensional_tables,
    aggregate_category_metrics
)

PROCESSED_LOG_PATH = "data/processed_files.txt"
RAW_PATH = "data/raw"
OUTPUT_PATH = "data/output"

def load_processed_files():
    if not os.path.exists(PROCESSED_LOG_PATH):
        return set()
    with open(PROCESSED_LOG_PATH, "r") as f:
        return set(line.strip() for line in f.readlines())

def mark_file_as_processed(filename):
    with open(PROCESSED_LOG_PATH, "a") as f:
        f.write(f"{filename}\n")

def process_existing_raw_files(spark, raw_path, output_path, processed_files):
    handler = NewCsvHandler(spark, RAW_PATH, OUTPUT_PATH, processed_files)
    for filename in os.listdir(raw_path):
        if filename.endswith(".csv") and filename not in processed_files:
            logging.info(f"New file from process start: {filename}")
            handler.process_file(raw_path + "/" + filename, filename)

class NewCsvHandler(FileSystemEventHandler):
    def __init__(self, spark, raw_path, output_path, processed_files):
        self.spark = spark
        self.raw_path = raw_path
        self.output_path = output_path
        self.processed_files = processed_files

    def on_created(self, event):
        if event.src_path.endswith(".csv"):
            filename = os.path.basename(event.src_path)
            if filename in self.processed_files:
                return

            logging.info(f"New file detected: {filename}")

            self.process_file(event.src_path, filename)

    def process_file(self, filePath, filename):
        snapshot = filename.split("-")[-1].replace(".csv", "")
        df = extract_data(self.spark, filePath)
        if df:
            df = transform_data(df).withColumn("snapshot", lit(snapshot))
            clean_df = filter_relevent_kickstarters(df)
            fact_df, dim_df = build_dimensional_tables(clean_df)
            summary_df = aggregate_category_metrics(clean_df).withColumn("snapshot", lit(snapshot))

            fact_df.write.mode("append").parquet(f"{self.output_path}/fact_campaign")
            dim_df.write.mode("append").parquet(f"{self.output_path}/dim_campaign")
            summary_df.write.mode("append").parquet(f"{self.output_path}/summary_campaign")

            self.update_top_20_campaigns()

            mark_file_as_processed(filename)
            self.processed_files.add(filename)
            logging.info(f"Processed and saved: {filename}")

    def update_top_20_campaigns(self):
        try:
            df = self.spark.read.parquet(f"{self.output_path}/fact_campaign")

            df = df.filter(
                (col("campaign_duration") > 0) &
                (col("state") == "successful")
            )

            df = df.withColumn("goal_per_day", round(col("goal") / col("campaign_duration"), 2))

            top_campaigns = df.dropDuplicates(["ID"]).orderBy(desc("goal_per_day")).limit(20)
            top_campaigns.write.mode("overwrite") \
                .option("header", True) \
                .csv(f"{self.output_path}/top_campaigns")
            logging.info("Top 20 campaigns updated and saved in csv file.")
            top_campaigns.show()
        except Exception as e:
            logging.error(f"[ERROR] Updating top 20 campaigns: {e}")

def start_watcher():
    spark = SparkSession.builder.appName("KickstarterWatcher").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")

    processed_files = load_processed_files()

    process_existing_raw_files(spark, RAW_PATH, OUTPUT_PATH, processed_files)

    event_handler = NewCsvHandler(spark, RAW_PATH, OUTPUT_PATH, processed_files)
    observer = Observer()
    observer.schedule(event_handler, RAW_PATH, recursive=False)
    observer.start()
    print(f"Watching folder: {RAW_PATH}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()
