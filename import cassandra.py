import json
from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider # Uncomment if Cassandra auth is enabled
from datetime import datetime

# --- Configuration ---
CASSANDRA_HOSTS = ['localhost'] # Your Docker host for Cassandra
CASSANDRA_PORT = 9042           # The host port mapped in docker-compose.yml

# --- CUSTOMIZE THESE TO MATCH YOUR CQL SCHEMA AND JSON FILE ---
KEYSPACE_NAME = 'watch_history_db'      # The Keyspace you created in cqlsh
TABLE_NAME = 'watch_history_new'            # The Table you created in cqlsh
JSON_FILE_PATH = r"D:\Kuliah\ROSBD\ml-latest-small\movie_db.watch_history.json" #ganti ke path dataset 2 yang watch history

# --- Authentication (if you enabled authentication for Cassandra) ---
# If your Cassandra has authentication enabled (not default in basic setup), uncomment and fill these:
# CASSANDRA_USERNAME = 'your_cassandra_username'
# CASSANDRA_PASSWORD = 'your_cassandra_password'

# Initialize cluster outside try-except for finally block
cluster = None
session = None

try:
    # --- Connect to Cassandra ---
    # If using authentication, uncomment and use auth_provider:
    # auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
    # cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT) # For basic setup without auth
    
    session = cluster.connect(KEYSPACE_NAME)
    print(f"Connected to Cassandra cluster: {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
    print(f"Using Keyspace: '{KEYSPACE_NAME}'")

except Exception as e:
    print(f"ERROR: Could not connect to Cassandra. Please ensure your Docker container is running and ports are correct.")
    print(f"Details: {e}")
    exit()

# --- Read JSON Data ---
json_data = []
try:
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    print(f"Read {len(json_data)} records from '{JSON_FILE_PATH}'.")

except FileNotFoundError:
    print(f"ERROR: JSON file not found at '{JSON_FILE_PATH}'.")
    print("Please ensure the file exists and the path is correct in the script.")
    exit()
except json.JSONDecodeError as e:
    print(f"ERROR: Invalid JSON format in '{JSON_FILE_PATH}'. Details: {e}")
    print("Ensure the file contains valid JSON (e.g., a list of objects or a single object).")
    exit()
except Exception as e:
    print(f"An unexpected error occurred while reading the JSON file: {e}")
    exit()

# --- Prepare and Insert Data ---
# The order of columns in VALUES(...) must match the order of columns in the prepared statement.
insert_statement = session.prepare(
    f"INSERT INTO {TABLE_NAME} (user_id, movie_id, watch_time, duration_watched) VALUES (?, ?, ?, ?)"
)

try:
    inserted_count = 0
    for record in json_data:
        try:
            user_id = record.get('user_id')

            # --- PERBAIKAN PENTING DI SINI ---
            # Ambil movie_id sebagai string dari JSON, lalu konversi ke int
            movie_id_str = record.get('movie_id')
            if movie_id_str is not None:
                try:
                    movie_id = int(movie_id_str) # Konversi ke integer
                except ValueError:
                    print(f"Peringatan: movie_id '{movie_id_str}' tidak dapat dikonversi ke integer untuk record: {record}. Baris ini akan dilewati.")
                    continue # Lewati baris ini jika konversi gagal
            else:
                movie_id = None # Atau tangani sebagai error jika movie_id wajib

            # Handle the "$date" object for watch_time
            watch_time_obj = record.get('watch_time')
            watch_time_str = watch_time_obj.get('$date') if watch_time_obj and '$date' in watch_time_obj else None

            if watch_time_str:
                watch_time = datetime.fromisoformat(watch_time_str.replace('Z', '+00:00')) # Handles 'Z' for UTC
            else:
                watch_time = None

            duration_watched = int(record.get('duration_watched'))

            session.execute(insert_statement, (user_id, movie_id, watch_time, duration_watched))
            inserted_count += 1
            # Sebaiknya cetak setelah sejumlah record atau di akhir untuk menghindari output yang terlalu banyak
            # print(f"Successfully inserted {inserted_count} records into Cassandra table '{TABLE_NAME}'.")

        except Exception as e:
            print(f"Warning: Failed to insert record: {record}. Error: {e}")
            # print(f"Error details: {e}") # Bisa ditambahkan untuk detail error yang lebih baik

    print(f"Successfully inserted {inserted_count} records into Cassandra table '{TABLE_NAME}'.")

except Exception as e: # Tangani exception di luar loop juga
    print(f"An error occurred during data insertion: {e}")

finally:
    if cluster:
        cluster.shutdown()
        print("Cassandra connection closed.")