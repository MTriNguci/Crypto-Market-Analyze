
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import subprocess
import os


# 1. Lấy file mới nhất trong thư mục
def get_historical_file(directory, extension):
    # Lấy danh sách các file trong thư mục chứa 'historical' và đúng đuôi mở rộng
    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(extension) and "historical" in f
    ]
    
    # Nếu không có file nào, trả về None
    if not files:
        return None

    # Trả về file mới nhất dựa trên thời gian sửa đổi
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

    


def load_json_from_file(filepath):
    # Đọc dữ liệu JSON từ file
    with open(filepath, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


# 2. Lưu dữ liệu JSON dưới dạng Parquet
def save_json_to_parquet(data, output_filepath):
    # Chuyển đổi dữ liệu JSON thành pyarrow Table
    if isinstance(data, dict):
        if 'data' in data:
            data = data['data']
        if 'news' in output_filepath and 'feeds' in data:    # for news data
            if data['feeds']:  # nếu feed không rỗng
                data = flatten_news_data(data['feeds'])
            else:
                print("Feed is empty, skipping processing.")
                return
    
    if 'cmc100' in output_filepath:
        if 'historical' in output_filepath:
            # Nếu là dữ liệu cmc100 historical, cần xử lý thêm
            print(type(data))
            # print(data.keys())
            data = flatten_historical_constituents(data)
        else:
            print("Not a historical file, skipping processing.")
            return
    
    if 'fng' in output_filepath:
        # if isinstance(data, dict) and 'value' in data:
        #     data = [data]  # Bọc thành list
        df = pd.DataFrame(data)
        if 'timestamp' in df.columns:
            # Đổi tên cột 'timestamp' → 'update_time'
            df.rename(columns={'timestamp': 'update_time'}, inplace=True)
            # Ép kiểu thời gian từ epoch (số giây) → ISO 8601
            df['update_time'] = pd.to_datetime(df['update_time'], unit='s', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        data = df.to_dict(orient='records')

    if isinstance(data, dict):
        data = list(data.values())

    # Kiểm tra lại sau khi xử lý
    if not isinstance(data, list):
        raise ValueError("Expected a list of records (dicts) to convert to DataFrame")




    # Tạo DataFrame và chuyển sang Parquet
    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)

    print("[DEBUG] Saving Parquet: df shape =", df.shape)
    print("[DEBUG] df columns:", df.columns)
    print(df.head())

    # Chuyển đổi các cột có kiểu dữ liệu 'object' và chứa 'T' thành datetime
    for col in df.columns:
        if col in ['last_update', 'last_updated','update_time','date_added', 'time_published'] and df[col].dtype == 'object':
            if df[col].str.contains('T').any():
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    # ✅ Ép kiểu datetime về milliseconds để tránh lỗi Parquet với Spark
    for col in df.select_dtypes(include=['datetime64[ns, UTC]']).columns:
        df[col] = df[col].dt.floor('ms')  # hoặc .dt.round('ms')

    df.to_csv(output_filepath.replace('.parquet', '.csv'), index=False,encoding='utf-8',quoting=1 )
    table = pa.Table.from_pandas(df)

    # print(table.to_pandas().head())
    pq.write_table(table, output_filepath)



def escape_path(path):
    return path.replace(' ', '%20')


# 3. Upload file Parquet lên HDFS
def upload_to_hdfs(local_path, hdfs_path):
    local_path = escape_path(local_path)
    try:
        # Tạo thư mục HDFS nếu chưa tồn tại
        subprocess.run(
            ['/home/minhtri/hadoop/bin/hdfs', 'dfs', '-mkdir', '-p', hdfs_path],
            check=True
        )

        # Upload file từ local_path lên HDFS
        subprocess.run(
            ['/home/minhtri/hadoop/bin/hdfs', 'dfs', '-put', '-f', local_path, hdfs_path],
            check=True
        )

        print(f"✅ Uploaded file {local_path} to HDFS at {hdfs_path}")

    except subprocess.CalledProcessError as e:
        print(f"❌ HDFS upload failed: {e}")






def load_db_to_dl(input_directory, output_directory, hdfs_directory):
    extension = '.json'

    # Lấy file JSON mới nhất trong thư mục
    latest_file = get_historical_file(input_directory, extension)
    print(f"[INFO] Latest JSON file found: {latest_file}")
    if latest_file:
        # Đọc dữ liệu từ file JSON
        data = load_json_from_file(latest_file)
        print(f"Read file: {latest_file}")
        
        # Tạo tên file Parquet từ tên file JSON
        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
        
        # Lưu dữ liệu JSON dưới dạng Parquet
        save_json_to_parquet(data, output_filepath)
        print(f"Saved Parquet file: {output_filepath}")
        
        # Upload file Parquet lên HDFS
        upload_to_hdfs(output_filepath, hdfs_directory)
    else:
        print("No JSON files found in the directory")

def load_api_to_parquet():
    # Đường dẫn thư mục chứa file JSON
    api_sources = [
        {
            "name": "fng",
            "input_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/fng/historical',
            "output_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/completed/coinmarketcap/fng/historical',
            "hdfs_dir": '/user/cryptomarket/datalake/coinmarketcap/fng/historical',
        },
        {
            "name": "cmc100",
            "input_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/cmc100/historical',
            "output_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/completed/coinmarketcap/cmc100/historical',
            "hdfs_dir": '/user/cryptomarket/datalake/coinmarketcap/cmc100/historical',
        },
        
        {
            "name": "news",
            "input_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/news/historical',
            "output_dir": '/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/completed/news/historical',
            "hdfs_dir": '/user/cryptomarket/datalake/news/historical',
        },
        ]


    # Duyệt qua từng loại API và xử lý
    for source in api_sources:
        print(f"\n=== Processing {source['name']} ===")
        load_db_to_dl(source["input_dir"], source["output_dir"], source["hdfs_dir"])


def flatten_historical_constituents(data_list):
    """
    Nhận vào một list các object {"value", "constituents", "update_time"}
    Trả ra danh sách các constituent đã được thêm thông tin index_value và last_update.
    """
    if not isinstance(data_list, list):
        raise ValueError("Expected a list of historical records")

    all_constituents = []

    for record in data_list:
        constituents = record.get("constituents", [])
        index_value = record.get("value")
        last_update = record.get("update_time")

        for item in constituents:
            if 'name' not in item or 'symbol' not in item:
                print("[WARN] Missing name or symbol:", item)
            # item["index_value"] = index_value
            # item["last_update"] = last_update
            # all_constituents.append(item)
            item_copy = item.copy()
            item_copy["index_value"] = index_value
            item_copy["last_update"] = last_update
            all_constituents.append(item_copy)

    return all_constituents




def flatten_news_data(data):
    """
    Flatten các bản tin trong danh sách data['feed'].
    Trả về danh sách các bản ghi đã flatten (cho Pandas DataFrame).
    """
    flattened_records = []

    for item in data:
        base_info = {
            "title": item.get("title"),
            "url": item.get("url"),
            "time_published": item.get("time_published"),
            "summary": item.get("summary"),
            "source": item.get("source"),
            "overall_sentiment_score": item.get("overall_sentiment_score"),
            "overall_sentiment_label": item.get("overall_sentiment_label")
        }

        # Nếu có topics thì flatten mỗi topic một bản ghi
        for topic in item.get("topics", []):
            record = base_info.copy()
            record.update({
                "type": "topic",
                "topic": topic.get("topic"),
                "topic_relevance_score": topic.get("relevance_score")
            })
            flattened_records.append(record)

        # Nếu có ticker_sentiment thì flatten mỗi ticker một bản ghi
        for ticker in item.get("ticker_sentiment", []):
            record = base_info.copy()
            record.update({
                "type": "ticker",
                "ticker": ticker.get("ticker"),
                "ticker_sentiment_score": ticker.get("ticker_sentiment_score"),
                "ticker_sentiment_label": ticker.get("ticker_sentiment_label")
            })
            flattened_records.append(record)
    csv_output_path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/completed/news/news_historical_data.csv"
    df = pd.DataFrame(flattened_records)
    df.to_csv(csv_output_path, index=False)
    print(f"Flattened news data saved to: {csv_output_path}")
    return flattened_records


# def main():
#     # Gọi hàm để xử lý dữ liệu từ API và lưu vào Parquet
#     load_api_to_parquet()
# if __name__ == "__main__":
#     main()
     
if __name__ == "__main__":
    # Gọi hàm để xử lý dữ liệu từ API và lưu vào Parquet
    load_api_to_parquet()
