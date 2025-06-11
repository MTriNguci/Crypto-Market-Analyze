from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, array_distinct

import os
from pyspark.sql.functions import from_unixtime

# Hàm lấy file Parquet mới nhất từ thư mục HDFS
def get_latest_parquet_file(spark, hdfs_directory):
    files_df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.parquet") \
        .load(hdfs_directory)

    files_df = files_df.withColumn("file_name", col("path"))

    print("Danh sách file .parquet tìm thấy trong thư mục:")
    files = files_df.select("file_name").collect()
    for f in files:
        print(f["file_name"])

    latest_file_row = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()

    if latest_file_row:
        return latest_file_row[0]["file_name"]
    else:
        print("Không tìm thấy file .parquet trong thư mục.")
        return None

# === MAIN PIPELINE ===
def transform_news():
    # --- Cấu hình ---
    JDBC_DRIVER_PATH = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/sqljdbc_12.10/enu/jars/mssql-jdbc-12.10.0.jre8.jar"  # Đường dẫn JDBC driver (Windows path)
    HDFS_HOST ="localhost" 
    
    HDFS_DIRECTORY = f"hdfs://{HDFS_HOST}:9000/user/cryptomarket/datalake/news"
    JDBC_URL = "jdbc:sqlserver://34.87.159.67:1433;databaseName=e2edb"

    JDBC_PROPERTIES = {
        "user": "sqlserver",
        "password": "minhtri123",
        "encrypt": "true",  # Google Cloud SQL hỗ trợ TLS
        "trustServerCertificate": "true",  # Hoặc dùng trustStore trong config bảo mật hơn
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    JDBC_TABLE_NAME = "alphavantage_news"

    # --- Khởi tạo SparkSession ---
    spark = SparkSession.builder \
        .appName("Transform and Upload NEWS to Azure SQL") \
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_HOST}:9000")\
        .config("spark.hadoop.fs.default.name", f"hdfs://{HDFS_HOST}:9000") \
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH) \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()

    # --- Lấy file mới nhất ---
    latest_parquet_file = get_latest_parquet_file(spark, HDFS_DIRECTORY)

    if not latest_parquet_file:
        print("Không có file để xử lý. Kết thúc.")
        spark.stop()
        return

    print(f"Đang xử lý file: {latest_parquet_file}")

    # --- Đọc Parquet ---
    raw_df = spark.read.parquet(latest_parquet_file)
    raw_df.printSchema()

    # Trường hợp dữ liệu là Map<String, Struct>
    # data_map = raw_df.select("data.*")

    flat_df = raw_df.select(
        col("title").cast("string").alias("title"),
        col("url").cast("string").alias("url"),
        (col("time_published")/ 1_000_000_000).cast("timestamp").alias("time_published"),
        col("summary").cast("string").alias("summary"),
        col("source").cast("string").alias("source"),
        col("overall_sentiment_score").cast("double").alias("overall_sentiment_score"),
        col("overall_sentiment_label").cast("string").alias("overall_sentiment_label"),
        col("type").cast("string").alias("type"),
        col("topic").cast("string").alias("topic"),
        col("topic_relevance_score").alias("topic_relevance_score"),
        col("ticker").cast("string").alias("ticker"),
        col("ticker_sentiment_score").cast("double").alias("ticker_sentiment_score"),
        col("ticker_sentiment_label").cast("string").alias("ticker_sentiment_label")
    )

    # --- Ghi vào Azure SQL ---
    flat_df.write.jdbc(
        url=JDBC_URL,
        table=JDBC_TABLE_NAME,
        mode="append",  # hoặc "append" nếu không muốn ghi đè
        properties=JDBC_PROPERTIES
    )

    print("Đã ghi dữ liệu vào Azure SQL thành công.")
    spark.stop()


# if __name__ == "__main__":
#     main()


# root
#  |-- title: string (nullable = true)
#  |-- url: string (nullable = true)
#  |-- time_published: string (nullable = true)
#  |-- summary: string (nullable = true)
#  |-- source: string (nullable = true)
#  |-- overall_sentiment_score: double (nullable = true)
#  |-- overall_sentiment_label: string (nullable = true)
#  |-- type: string (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- topic_relevance_score: string (nullable = true)
#  |-- ticker: string (nullable = true)
#  |-- ticker_sentiment_score: string (nullable = true)
#  |-- ticker_sentiment_label: string (nullable = true)