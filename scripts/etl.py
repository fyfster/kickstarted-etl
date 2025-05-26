from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, round
from pyspark.sql.functions import to_date, datediff, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime
import logging

def extract_data(spark, path):
    try:
        kickStarter = spark.read.option("Header", "true")\
                    .option("InferSchema", "true")\
                    .csv(path)
        return kickStarter
    except Exception as e:
        logging.error(f"An error occurred while extracting: {e}")
        return None

def transform_data(kickStarter):
    try:
        kickStarter = kickStarter.toDF(*[c.strip() for c in kickStarter.columns])
        kickStarter = kickStarter.withColumn("launched_date", to_date(to_timestamp("launched", "yyyy-MM-dd HH:mm:ss")))
        kickStarter = kickStarter.withColumn("deadline_date", to_date(to_timestamp("deadline", "yyyy-MM-dd HH:mm:ss")))
        kickStarter = kickStarter.withColumn("goal", col("goal").cast(DoubleType()))
        kickStarter = kickStarter.withColumn("pledged", col("pledged").cast(DoubleType()))
        kickStarter = kickStarter.withColumn("backers", col("backers").cast(IntegerType()))
        kickStarter = kickStarter.withColumn("usd_pledged", col("usd pledged").cast(DoubleType()))
        kickStarter = kickStarter.withColumn("campaign_duration", datediff(col("deadline_date"), col("launched_date")))

        return kickStarter
    except Exception as e:
        logging.error(f"An error occurred while transforming: {e}")
        return None

def filter_relevant_kickstarters(kickStarter):
    cleanKickStarter = kickStarter.filter(
        (col("launched_date").isNotNull()) &
        (col("deadline_date").isNotNull()) &
        (col("goal") > 0) &
        (col("campaign_duration") > 0)
    )
    return cleanKickStarter


def build_dimensional_tables(cleanKickStarter):
    factCampaign = cleanKickStarter.select(
        "ID", "name", "goal", "launched_date","deadline_date", "campaign_duration", "state", "pledged", "usd_pledged", "backers"
    )
    dimCampaign = cleanKickStarter.select(
        "category", "main_category"
    )
    return factCampaign, dimCampaign

def aggregate_category_metrics(cleanKickStarter):
    categorySuccessRate = cleanKickStarter.groupBy("main_category") \
        .agg(
            count("*").alias("total_campaigns"),
            count(when(col("state") == "successful", True)).alias("successfull_campaign"),
            round(count(when(col("state") == "successful", True)) / count("*"), 2).alias("successfull_rate"),
        )
    return categorySuccessRate

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Started pipeline {datetime.now()}")

    spark = SparkSession.builder.appName("KickStarter").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")

    kickStarter = extract_data(spark, "/home/razvan/Projects/kickstarted-etl/data/raw/ks-projects-201612.csv")
    if not kickStarter:
        return

    kickStarter = transform_data(kickStarter)
    if not kickStarter:
        return

    try:
        cleanKickStarter = filter_relevant_kickstarters(kickStarter)

        fact, dim = build_dimensional_tables(cleanKickStarter)
        summary = aggregate_category_metrics(cleanKickStarter)
    except Exception as e:
        logging.error(f"An error occurred while loading: {e}")
        return None

    fact.show(3)
    dim.show(3)
    summary.show(5)

    outputPath = "/home/razvan/Projects/kickstarted-etl/data/output/"
    fact.write.mode("overwrite").parquet(f"{outputPath}fact_campaign")
    dim.write.mode("overwrite").parquet(f"{outputPath}dim_campaign")
    summary.write.mode("overwrite").parquet(f"{outputPath}summary_campaign")
    print("Finish saving on disk")
    logging.info(f"Finished pipeline {datetime.now()}")
    spark.stop()

if __name__ == "__main__":
    main()