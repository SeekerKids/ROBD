#Py
import csv
from pymongo import MongoClient

# --- Configuration for your MongoDB connection ---
# Replace with your Dockerized MongoDB host and port
# If you changed the host port, use that here (e.g., 27018)
MONGO_HOST = 'localhost'
MONGO_PORT = 27017 # This should be the host port you mapped in docker-compose.yml

DATABASE_NAME = 'moviedb_from_python' # This will be the name of your new database
COLLECTION_NAME = 'movies_collection' # This will be the name of your new collection

# --- CORRECTED CSV_FILE_PATH ---
# Use a raw string (r"...") for Windows paths to avoid issues with backslashes,
# or use forward slashes (which Python handles correctly on Windows too).
# Also, remove the extra double quotes around the path.
CSV_FILE_PATH = r"D:\Kuliah\ROSBD\ml-latest-small\movies.csv" #ini diganti tempat naruh dataset 1 yg movie

# --- MongoDB Authentication Credentials (from your docker-compose.yml) ---
MONGO_USERNAME = 'user' # As defined by MONGO_INITDB_ROOT_USERNAME
MONGO_PASSWORD = 'pass' # As defined by MONGO_INITDB_ROOT_PASSWORD
AUTH_SOURCE = 'admin'   # The database where the user is defined

# Initialize client outside try-except to ensure it's accessible in finally
client = None
db = None
collection = None

try:
    # --- Establish MongoDB Connection ---
    # Now including username, password, and authSource for authentication
    client = MongoClient(MONGO_HOST, MONGO_PORT, username=MONGO_USERNAME, password=MONGO_PASSWORD, authSource=AUTH_SOURCE)

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    print(f"Successfully connected to MongoDB: {MONGO_HOST}:{MONGO_PORT}")
    print(f"Targeting database: '{DATABASE_NAME}', collection: '{COLLECTION_NAME}'")

    # --- Read CSV and Prepare Data for Insertion ---
    documents_to_insert = []
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            # Each row will be a dictionary where keys are column headers
            for row in csv_reader:
                # --- Data Type Conversion (Important for numbers) ---
                # 'movies.csv' typically has 'movieId' as a number. Convert it.
                if 'movieId' in row and row['movieId']:
                    try:
                        row['movieId'] = int(row['movieId'])
                    except ValueError:
                        print(f"Warning: Failed to convert movieId '{row['movieId']}' to integer. Keeping as string.")
                
                # Add more type conversions here if other columns are numbers, booleans, etc.
                
                documents_to_insert.append(row)

        print(f"Read {len(documents_to_insert)} rows from '{CSV_FILE_PATH}'.")

    except FileNotFoundError:
        print(f"ERROR: The CSV file was not found at '{CSV_FILE_PATH}'.")
        print("Please ensure the 'movies.csv' file is in the same directory as this Python script,")
        print("or update the 'CSV_FILE_PATH' variable in the script with the correct full path.")
        # Re-raise or exit, depending on desired behavior
        exit()
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        exit()

    # --- Insert Data into MongoDB ---
    if documents_to_insert:
        try:
            # Insert all documents in one go (more efficient for large datasets)
            result = collection.insert_many(documents_to_insert)
            print(f"Successfully inserted {len(result.inserted_ids)} documents.")
        except Exception as e:
            print(f"ERROR: Failed to insert documents into MongoDB: {e}")
    else:
        print("No documents to insert (CSV was empty or had no valid data).")

except Exception as e:
    print(f"An unexpected error occurred during the overall process: {e}")

finally:
    # --- Ensure MongoDB Connection is Closed ---
    if client:
        client.close()
        print("MongoDB connection closed.")