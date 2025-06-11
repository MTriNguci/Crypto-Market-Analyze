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
def transform_idmap():
    # --- Cấu hình ---
    JDBC_DRIVER_PATH = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/sqljdbc_12.10/enu/jars/mssql-jdbc-12.10.0.jre8.jar"  # Đường dẫn JDBC driver (Windows path)
    HDFS_HOST = "localhost" 
    
    HDFS_DIRECTORY = f"hdfs://{HDFS_HOST}:9000/user/cryptomarket/datalake/coinmarketcap/idmap"
    JDBC_URL = "jdbc:sqlserver://34.87.159.67:1433;databaseName=e2edb"
    JDBC_PROPERTIES = {
        "user": "sqlserver",
        "password": "minhtri123",
        "encrypt": "true",  # Google Cloud SQL hỗ trợ TLS
        "trustServerCertificate": "true",  # Hoặc dùng trustStore trong config bảo mật hơn
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    JDBC_TABLE_NAME = "coin_idmap"

    # --- Khởi tạo SparkSession ---
    spark = SparkSession.builder \
        .appName("Transform and Upload idmap to Azure SQL") \
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
        col("id").cast("int"),
        col("name").cast("string"),
        col("symbol").cast("string"),
        col("category").cast("string"),
        col("description").cast("string"),
        col("slug").cast("string"),
        col("logo").cast("string"),
        col("subreddit").cast("string"),
        col("notice").cast("string"),
        concat_ws(",", array_distinct(col("tags"))).alias("tags"),
        concat_ws(",", array_distinct(col("tag-names"))).alias("tag_names"),
        concat_ws(",", array_distinct(col("tag-groups"))).alias("tag_groups"),
        (col("date_added") / 1_000_000_000).cast("timestamp").alias("date_added"),
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


if __name__ == "__main__":
    transform_idmap()

    # Flatten các trường cần thiết
#     root
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- symbol: string (nullable = true)
#  |-- category: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- slug: string (nullable = true)
#  |-- logo: string (nullable = true)
#  |-- subreddit: string (nullable = true)
#  |-- notice: string (nullable = true)
#  |-- tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- tag-names: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- tag-groups: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- urls: struct (nullable = true)
#  |    |-- announcement: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- chat: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- explorer: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- facebook: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- message_board: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- reddit: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- source_code: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- technical_doc: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- twitter: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- website: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |-- platform: struct (nullable = true)
#  |    |-- id: string (nullable = true)
#  |    |-- name: string (nullable = true)
#  |    |-- slug: string (nullable = true)
#  |    |-- symbol: string (nullable = true)
#  |    |-- token_address: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |    |-- name: string (nullable = true)
#  |    |-- slug: string (nullable = true)
#  |    |-- symbol: string (nullable = true)
#  |    |-- token_address: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |    |-- slug: string (nullable = true)
#  |    |-- symbol: string (nullable = true)
#  |    |-- token_address: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |    |-- symbol: string (nullable = true)
#  |    |-- token_address: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |    |-- token_address: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |-- twitter_username: string (nullable = true)
#  |-- is_hidden: long (nullable = true)
#  |-- is_hidden: long (nullable = true)
#  |-- date_launched: string (nullable = true)
#  |-- date_launched: string (nullable = true)
#  |-- contract_address: array (nullable = true)
#  |-- contract_address: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- contract_address: string (nullable = true)
#  |    |    |-- platform: struct (nullable = true)
#  |    |    |    |-- coin: struct (nullable = true)
#  |    |    |    |    |-- id: string (nullable = true)
#  |    |    |    |    |-- name: string (nullable = true)
#  |    |    |    |    |-- slug: string (nullable = true)
#  |    |    |    |    |-- symbol: string (nullable = true)
#  |    |    |    |-- name: string (nullable = true)
#  |-- self_reported_circulating_supply: double (nullable = true)
#  |-- self_reported_tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- self_reported_market_cap: double (nullable = true)
#  |-- infinite_supply: boolean (nullable = true)