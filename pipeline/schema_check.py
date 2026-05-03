from pathlib import Path

from pyspark.sql import SparkSession

try:
    from delta import configure_spark_with_delta_pip
except Exception as ex:
    configure_spark_with_delta_pip = None
    print("Could not import configure_spark_with_delta_pip:", ex)


builder = (
    SparkSession.builder
    .appName("Stage3SchemaCheck")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

if configure_spark_with_delta_pip is not None:
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
else:
    spark = builder.getOrCreate()


tables = [
    "/data/output/stream_gold/current_balances",
    "/data/output/stream_gold/recent_transactions",
]

for path in tables:
    print("=" * 100)
    print("TABLE:", path)

    delta_log = Path(path) / "_delta_log"
    print("DELTA_LOG_EXISTS:", delta_log.exists())

    try:
        df = spark.read.format("delta").load(path)
        print("READ_MODE: delta")
    except Exception as ex:
        print("DELTA_READ_FAILED:", ex)
        print("Trying parquet fallback for schema inspection only...")
        df = spark.read.parquet(path)
        print("READ_MODE: parquet")

    print("ROW_COUNT:", df.count())
    print("COLUMNS:", df.columns)
    df.printSchema()

spark.stop()
