from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REPO_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = REPO_ROOT / "data" / "raw" / "sensor_readings.parquet"
OUTPUT_DIR = REPO_ROOT / "data" / "spark"
ROBOT_DAILY_OUTPUT = OUTPUT_DIR / "robot_daily_quality"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("sentinel-sensor-quality-job")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Spark job started ===")
    print(f"Input: {INPUT_PATH}")

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run: python -m generator.cli --out data/raw"
        )

    df = spark.read.parquet(str(INPUT_PATH))

    print("\n=== Raw nested schema ===")
    df.printSchema()

    raw_count = df.count()
    print(f"\nRaw row count: {raw_count:,}")

    # Flatten nested structs into analysis-friendly columns.
    flattened = (
        df
        .select(
            F.col("reading_id"),
            F.col("robot_id"),
            F.col("reading_ts").alias("reading_ts"),
            F.to_date("reading_ts").alias("reading_date"),

            F.col("battery.voltage_v").cast("double").alias("battery_voltage_v"),
            F.col("battery.soc_pct").cast("double").alias("battery_soc_pct"),
            F.col("battery.temp_c").cast("double").alias("battery_temp_c"),

            F.col("motor.temp_c").cast("double").alias("motor_temp_c"),
            F.col("motor.rpm").cast("double").alias("motor_rpm"),
            F.col("motor.load_pct").cast("double").alias("motor_load_pct"),
            F.col("motor.vibration_g").cast("double").alias("vibration_g"),

            F.col("wheels.odometer_m").cast("double").alias("odometer_m"),

            F.col("navigation.zone_id").alias("zone_id"),
            F.col("environment.floor_condition").alias("floor_condition"),
            F.col("operational_status"),
            F.col("power_draw_watts").cast("double").alias("power_draw_watts"),
        )
    )

    print("\n=== Flattened schema ===")
    flattened.printSchema()

    # Quality flags.
    flagged = (
        flattened
        .withColumn(
            "is_missing_robot_id",
            F.col("robot_id").isNull() | (F.trim(F.col("robot_id")) == "")
        )
        .withColumn(
            "is_missing_reading_id",
            F.col("reading_id").isNull() | (F.trim(F.col("reading_id")) == "")
        )
        .withColumn(
            "is_bad_soc",
            (F.col("battery_soc_pct") < 0) | (F.col("battery_soc_pct") > 100)
        )
        .withColumn(
            "is_bad_motor_temp",
            (F.col("motor_temp_c") < -20) | (F.col("motor_temp_c") > 150)
        )
        .withColumn(
            "is_bad_vibration",
            (F.col("vibration_g") < 0) | (F.col("vibration_g") > 20)
        )
        .withColumn(
            "is_bad_odometer",
            F.col("odometer_m") < 0
        )
    )

    print("\n=== Data quality summary ===")
    quality_summary = flagged.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.col("is_missing_robot_id").cast("int")).alias("missing_robot_id_rows"),
        F.sum(F.col("is_missing_reading_id").cast("int")).alias("missing_reading_id_rows"),
        F.sum(F.col("is_bad_soc").cast("int")).alias("bad_soc_rows"),
        F.sum(F.col("is_bad_motor_temp").cast("int")).alias("bad_motor_temp_rows"),
        F.sum(F.col("is_bad_vibration").cast("int")).alias("bad_vibration_rows"),
        F.sum(F.col("is_bad_odometer").cast("int")).alias("bad_odometer_rows"),
    )

    quality_summary.show(truncate=False)

    # Preserve rows, but null out severe invalid numeric values before aggregation.
    cleaned_values = (
        flagged
        .withColumn(
            "battery_soc_pct_clean",
            F.when(F.col("is_bad_soc"), None).otherwise(F.col("battery_soc_pct"))
        )
        .withColumn(
            "motor_temp_c_clean",
            F.when(F.col("is_bad_motor_temp"), None).otherwise(F.col("motor_temp_c"))
        )
        .withColumn(
            "vibration_g_clean",
            F.when(F.col("is_bad_vibration"), None).otherwise(F.col("vibration_g"))
        )
        .withColumn(
            "odometer_m_clean",
            F.when(F.col("is_bad_odometer"), None).otherwise(F.col("odometer_m"))
        )
    )

    robot_daily = (
        cleaned_values
        .where(
            F.col("robot_id").isNotNull()
            & F.col("reading_date").isNotNull()
        )
        .groupBy("robot_id", "reading_date")
        .agg(
            F.count("*").alias("reading_count"),
            F.avg("battery_voltage_v").alias("avg_battery_voltage_v"),
            F.avg("battery_soc_pct_clean").alias("avg_battery_soc_pct"),
            F.min("battery_soc_pct_clean").alias("min_battery_soc_pct"),
            F.avg("battery_temp_c").alias("avg_battery_temp_c"),
            F.avg("motor_temp_c_clean").alias("avg_motor_temp_c"),
            F.max("motor_temp_c_clean").alias("max_motor_temp_c"),
            F.avg("motor_rpm").alias("avg_motor_rpm"),
            F.avg("motor_load_pct").alias("avg_motor_load_pct"),
            F.avg("vibration_g_clean").alias("avg_vibration_g"),
            F.max("vibration_g_clean").alias("max_vibration_g"),
            F.max("odometer_m_clean").alias("max_odometer_m"),
            F.sum(F.col("is_bad_soc").cast("int")).alias("bad_soc_count"),
            F.sum(F.col("is_bad_motor_temp").cast("int")).alias("bad_motor_temp_count"),
            F.sum(F.col("is_bad_vibration").cast("int")).alias("bad_vibration_count"),
            F.sum(F.col("is_bad_odometer").cast("int")).alias("bad_odometer_count"),
        )
        .orderBy("robot_id", "reading_date")
    )

    daily_count = robot_daily.count()
    print(f"\nRobot-day output rows: {daily_count:,}")

    print("\n=== Sample robot-day aggregates ===")
    robot_daily.show(10, truncate=False)

    print(f"\nWriting robot-day output to: {ROBOT_DAILY_OUTPUT}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_DIR / "robot_daily_quality.csv"

    print(f"\nWriting robot-day output to: {output_file}")

    robot_daily_pd = robot_daily.toPandas()
    robot_daily_pd.to_csv(output_file, index=False)

    print(f"Rows written: {len(robot_daily_pd):,}")
    print("\n=== Spark job complete ===")
    spark.stop()


if __name__ == "__main__":
    main()