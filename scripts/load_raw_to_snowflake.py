from pathlib import Path
import os
import re
import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

RAW_FILES = {
    "robot_models_raw.csv": "RAW_ROBOT_MODELS",
    "zones_raw.csv": "RAW_ZONES",
    "robots_raw.csv": "RAW_ROBOTS",
    "robot_attribute_changes_raw.csv": "RAW_ROBOT_ATTRIBUTE_CHANGES",
    "sensor_readings_raw.csv": "RAW_SENSOR_READINGS",
    "tasks_raw.csv": "RAW_TASKS",
    "maintenance_tickets_raw.csv": "RAW_MAINTENANCE_TICKETS",
}


def clean_col_name(col: str, index: int) -> str:
    col = col.strip()
    col = re.sub(r"[^A-Za-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col)
    col = col.strip("_").upper()

    if not col:
        col = f"COL_{index}"

    if re.match(r"^[0-9]", col):
        col = f"COL_{col}"

    return col


def read_header(csv_path: Path) -> list[str]:
    import csv

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

    seen = {}
    cleaned = []

    for i, col in enumerate(header):
        base = clean_col_name(col, i)
        count = seen.get(base, 0)
        seen[base] = count + 1

        if count > 0:
            cleaned.append(f"{base}_{count + 1}")
        else:
            cleaned.append(base)

    return cleaned


def main() -> None:
    required_env = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_PRIVATE_KEY_PATH",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    ]

    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="SENTINEL_DBT_USER",
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        private_key_file_pwd=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
        role="SENTINEL_DBT_ROLE",
        warehouse="SENTINEL_DEV_WH",
        database="SENTINEL_ROBOT",
        schema="RAW",
    )

    try:
        cur = conn.cursor()

        cur.execute("USE DATABASE SENTINEL_ROBOT")
        cur.execute("USE SCHEMA RAW")
        cur.execute("USE WAREHOUSE SENTINEL_DEV_WH")

        cur.execute("""
            CREATE OR REPLACE FILE FORMAT CSV_HEADER_FORMAT
              TYPE = CSV
              FIELD_DELIMITER = ','
              SKIP_HEADER = 1
              FIELD_OPTIONALLY_ENCLOSED_BY = '"'
              NULL_IF = ('', 'NULL', 'null', 'N/A', 'NA', 'nan', 'None', '-', '#N/A')
              EMPTY_FIELD_AS_NULL = TRUE
              ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """)

        cur.execute("""
            CREATE OR REPLACE STAGE LOCAL_FILE_STAGE
              FILE_FORMAT = CSV_HEADER_FORMAT
        """)

        for filename, table_name in RAW_FILES.items():
            csv_path = DATA_DIR / filename

            if not csv_path.exists():
                raise FileNotFoundError(f"Missing file: {csv_path}")

            columns = read_header(csv_path)

            print(f"\nLoading {filename} -> RAW.{table_name}")
            print(f"Columns: {len(columns)}")

            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')

            if not columns:
                print(f"Skipping load for {filename}: file has no header/columns.")

                cur.execute(f"""
                    CREATE TABLE "{table_name}" (
                        "_LOADED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        "_SOURCE_FILE" VARCHAR
                    )
                """)

                continue

            column_sql = ",\n    ".join([f'"{col}" VARCHAR' for col in columns])

            cur.execute(f"""
                CREATE TABLE "{table_name}" (
                    {column_sql},
                    "_LOADED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    "_SOURCE_FILE" VARCHAR
                )
            """)

            stage_path = f"@LOCAL_FILE_STAGE/{filename}"
            file_uri = csv_path.as_posix()

            cur.execute(f"""
                PUT 'file://{file_uri}' {stage_path}
                AUTO_COMPRESS = FALSE
                OVERWRITE = TRUE
            """)

            column_list = ", ".join([f'"{col}"' for col in columns])

            cur.execute(f"""
                COPY INTO "{table_name}" ({column_list})
                FROM {stage_path}
                FILE_FORMAT = CSV_HEADER_FORMAT
                MATCH_BY_COLUMN_NAME = NONE
                ON_ERROR = CONTINUE
            """)

            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            row_count = cur.fetchone()[0]
            print(f"Loaded rows: {row_count}")

        print("\nRaw load complete.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()