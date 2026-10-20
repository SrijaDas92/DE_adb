import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
    
    


AUDIT_TABLE = "tx_dev.control.tax_ingestion_audit"


def standardize_column_name(column_name):
    """Convert a column name to lowercase snake_case."""
    column_name = column_name.strip()
    column_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name)
    return column_name.strip("_").lower()


# def ingest_csv(
#     spark,
#     source_path,
#     target_table,
#     required_columns,
#     source_system="TAX_REPORTING",
#     load_type="FULL",
#     dry_run=False,
# ): # changing the it for multiple format and making it parameterised


def ingest_file(
    spark,
    source_path,
    target_table,
    required_columns,
    file_format="csv",
    sheet_name="Lifecycle",
    source_system="TAX_REPORTING",
    load_type="FULL",
    dry_run=False,
    period_column="reporting_period",
    period_value=None,
    year_column="tax_year",
    year_value=None,
):
    """Ingest a CSV file into a Bronze Delta table with audit logging."""

    

    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    source_file_name = source_path.rsplit("/", 1)[-1]
    source_record_count = 0
    target_record_count = 0
    status = "STARTED"
    error_message = None

    try:
        # Read source file
        if file_format.lower() == "csv":
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(source_path)
            )

        elif file_format.lower() in ("xlsx", "excel"):
            import pandas as pd

            pandas_df = pd.read_excel(
                source_path,
                sheet_name=sheet_name,
                engine="openpyxl",
            )

            df = spark.createDataFrame(pandas_df)

            # # Old code with hardcoded sheet name
            # pandas_df = pd.read_excel(
            #     source_path,
            #     sheet_name="Lifecycle",
            #     engine="openpyxl",
            # )

        else:
            raise ValueError(
                f"Unsupported file format: {file_format}"
            )

        # # Read source (old CSV-only reader)
        # df = (
        #     spark.read
        #     .option("header", True)
        #     .option("inferSchema", True)
        #     .csv(source_path)
        # )

        # Standardize columns
        for old_name in df.columns:
            df = df.withColumnRenamed(
                old_name,
                standardize_column_name(old_name),
            )

        source_record_count = df.count()

        # Validate required columns
        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Required columns are missing: {missing_columns}"
            )

        # Validate required values
        null_condition = F.lit(False)

        for column in required_columns:
            null_condition = (
                null_condition | F.col(column).isNull()
            )

        invalid_count = df.filter(null_condition).count()

        if invalid_count > 0:
            raise ValueError(
                f"{invalid_count} records contain null required values."
            )

        # Validate reporting period for period-based replacement
        if load_type == "REPLACE_PERIOD":
            if period_value is None or year_value is None:
                raise ValueError(
                    "REPLACE_PERIOD requires period_value and year_value."
                )

            mismatched_records = df.filter(
                (F.col(period_column) != F.lit(period_value))
                | (F.col(year_column) != F.lit(year_value))
            ).count()

            if mismatched_records > 0:
                raise ValueError(
                    f"{mismatched_records} records do not match "
                    f"year={year_value}, period={period_value}."
                )

            print(
                f"Period validation passed: "
                f"{year_column}={year_value}, "
                f"{period_column}={period_value}"
            )

        # Add ingestion metadata
        df = (
            df
            .withColumn(
                "source_file_name",
                F.lit(source_file_name),
            )
            .withColumn(
                "source_system",
                F.lit(source_system),
            )
            .withColumn(
                "ingestion_timestamp",
                F.current_timestamp(),
            )
            .withColumn(
                "ingestion_date",
                F.current_date(),
            )
        )

        # Write to target using the configured load strategy
        if dry_run:
            status = "DRY_RUN_SUCCESS"

        elif load_type == "FULL":
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

            target_record_count = spark.table(target_table).count()

            if load_type == "FULL":
                if target_record_count != source_record_count:
                    raise ValueError(
                        f"Post-load validation failed: "
                        f"expected {source_record_count} records, "
                        f"found {target_record_count}."
                    )

            status = "SUCCESS"

        elif load_type == "APPEND":
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

            target_record_count = spark.table(target_table).count()

            if load_type == "FULL":
                if target_record_count != source_record_count:
                    raise ValueError(
                        f"Post-load validation failed: "
                        f"expected {source_record_count} records, "
                        f"found {target_record_count}."
                    )

            status = "SUCCESS"

        elif load_type == "REPLACE_PERIOD":
            if period_value is None or year_value is None:
                raise ValueError(
                    "REPLACE_PERIOD requires period_value and year_value."
                )

            if period_column not in df.columns or year_column not in df.columns:
                raise ValueError(
                    f"Source must contain {year_column} and {period_column}."
                )

            if not spark.catalog.tableExists(target_table):
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable(target_table)
                )
            else:
                from delta.tables import DeltaTable

                target = DeltaTable.forName(spark, target_table)

                target.delete(
                    (
                        F.col(year_column) == F.lit(year_value)
                    )
                    & (
                        F.col(period_column) == F.lit(period_value)
                    )
                )

                (
                    df.write
                    .format("delta")
                    .mode("append")
                    .saveAsTable(target_table)
                )

            target_record_count = spark.table(target_table).count()
            status = "SUCCESS"

        else:
            raise ValueError(
                f"Unsupported load strategy: {load_type}"
            )

        # # Old write section (simple overwrite/append)
        # if dry_run:
        #     status = "DRY_RUN_SUCCESS"
        # else:
        #     (
        #         df.write
        #         .format("delta")
        #         .mode("overwrite" if load_type == "FULL" else "append")
        #         .saveAsTable(target_table)
        #     )
        #
        #     target_record_count = spark.table(target_table).count()
        #     status = "SUCCESS"

    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)[:4000]

    # Write audit record
    end_time = datetime.now(timezone.utc)

    audit_schema = """
        run_id STRING,
        source_system STRING,
        source_file_name STRING,
        target_table STRING,
        load_type STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        source_record_count BIGINT,
        target_record_count BIGINT,
        status STRING,
        error_message STRING
    """

    audit_record = [(
        run_id,
        source_system,
        source_file_name,
        target_table,
        load_type,
        start_time,
        end_time,
        source_record_count,
        target_record_count,
        status,
        error_message,
    )]

    audit_df = spark.createDataFrame(
        audit_record,
        schema=audit_schema,
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )

    print(f"Run ID: {run_id}")
    print(f"Target: {target_table}")
    print(f"Source records: {source_record_count}")
    print(f"Target records: {target_record_count}")
    print(f"Status: {status}")

    if status == "FAILED":
        raise RuntimeError(
            f"Ingestion failed for {source_file_name}: {error_message}"
        )

    return {
        "run_id": run_id,
        "status": status,
        "source_record_count": source_record_count,
        "target_record_count": target_record_count,
    }