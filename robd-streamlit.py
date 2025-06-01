import streamlit as st
import pandas as pd
import json
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from collections import defaultdict, Counter
import time

# --- Configuration for MongoDB ---
MONGO_HOST = 'localhost'
MONGO_PORT = 27017 # Your MongoDB host port
MONGO_DB_NAME = 'moviedb_from_python'
MONGO_COLLECTION_NAME = 'movies_collection'
MONGO_USERNAME = 'user' # MongoDB username
MONGO_PASSWORD = 'pass' # MongoDB password
MONGO_AUTH_SOURCE = 'admin' # Authentication database for MongoDB

# --- Configuration for Cassandra ---
CASSANDRA_HOSTS = ['localhost'] # Your Cassandra host IP/name
CASSANDRA_PORT = 9042           # Your Cassandra host port
CASSANDRA_KEYSPACE = 'watch_history_db'
CASSANDRA_TABLE = 'watch_history_new'

# --- (Optional) Cassandra Authentication ---
# If your Cassandra has authentication enabled (not default in basic setup), uncomment and fill these:
# CASSANDRA_CASSANDRA_USERNAME = 'your_cassandra_username'
# CASSANDRA_CASSANDRA_PASSWORD = 'your_cassandra_password'


# --- Database Connection Functions (Cached for efficiency) ---

@st.cache_resource
def get_mongo_client():
    """Establishes and caches MongoDB connection."""
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT, 
                             username=MONGO_USERNAME, password=MONGO_PASSWORD, 
                             authSource=MONGO_AUTH_SOURCE)
        # The ping command is cheap and does not require auth.
        # It's a good way to check if the server is alive.
        client.admin.command('ping') 
        st.success(f"Connected to MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB_NAME}")
        return client
    except Exception as e:
        st.error(f"ERROR: Could not connect to MongoDB. Please ensure your Docker container is running and ports are correct.")
        st.error(f"Details: {e}")
        return None

@st.cache_resource
def get_cassandra_cluster_session():
    """Establishes and caches Cassandra connection."""
    try:
        # If using authentication, uncomment and use auth_provider:
        # auth_provider = PlainTextAuthProvider(username=CASSANDRA_CASSANDRA_USERNAME, password=CASSANDRA_CASSANDRA_PASSWORD)
        # cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
        
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT) # For basic setup without auth
        session = cluster.connect(CASSANDRA_KEYSPACE)
        st.success(f"Connected to Cassandra: {CASSANDRA_HOSTS}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
        return cluster, session
    except Exception as e:
        st.error(f"ERROR: Could not connect to Cassandra. Please ensure your Docker container is running and ports are correct.")
        st.error(f"Details: {e}")
        return None, None

# --- Data Loading Functions (Cached for efficiency) ---

@st.cache_data(ttl=3600) # Cache data for 1 hour
# FIX: Added underscore to _mongo_client_obj to prevent UnhashableParamError
def load_movie_genres(_mongo_client_obj):
    """Loads movie genre lookup from MongoDB."""
    if not _mongo_client_obj:
        return {}
    
    mongo_db = _mongo_client_obj[MONGO_DB_NAME]
    mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    movie_genres_lookup = {}
    try:
        for movie in mongo_movies_collection.find({}, {'movieId': 1, 'genres': 1, '_id': 0}):
            movie_id = movie.get('movieId')
            genres_str = movie.get('genres', '')
            movie_genres_lookup[movie_id] = [g.strip() for g in genres_str.split('|') if g.strip()]
        st.info(f"Loaded {len(movie_genres_lookup)} movie entries from MongoDB.")
    except Exception as e:
        st.error(f"Error loading movie genres from MongoDB: {e}")
    return movie_genres_lookup

@st.cache_data(ttl=3600) # Cache data for 1 hour
# FIX: Added underscore to _cassandra_session_obj to prevent UnhashableParamError
def load_watch_history(_cassandra_session_obj):
    """Loads watch history records from Cassandra."""
    if not _cassandra_session_obj:
        return pd.DataFrame() # Return empty DataFrame if no session
    
    watch_history_records_list = [] # Temporary list to hold dicts
    try:
        rows = _cassandra_session_obj.execute(f"SELECT user_id, movie_id, watch_time, duration_watched FROM {CASSANDRA_TABLE}")
        for row in rows:
            # Convert Cassandra Row object to a dictionary for serialization
            watch_history_records_list.append(row._asdict()) 
        
        # Convert list of dictionaries to Pandas DataFrame
        df = pd.DataFrame(watch_history_records_list)
        st.info(f"Retrieved {len(df)} watch history records from Cassandra.")
        return df
    except Exception as e:
        st.error(f"Error retrieving watch history from Cassandra: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

@st.cache_data(ttl=3600) # Cache data for 1 hour
def get_sample_movie_titles(_mongo_client_obj, limit=10):
    """Loads a sample of movie titles from MongoDB."""
    if not _mongo_client_obj:
        return []
    
    mongo_db = _mongo_client_obj[MONGO_DB_NAME]
    mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]
    
    titles = []
    try:
        # Fetch a sample of titles, limit to 'limit'
        for movie in mongo_movies_collection.find({}, {'title': 1, '_id': 0}).limit(limit):
            titles.append(movie.get('title'))
        st.info(f"Loaded {len(titles)} sample movie titles from MongoDB.")
    except Exception as e:
        st.error(f"Error loading sample movie titles: {e}")
    return titles


# --- Analysis Functions ---

def analyze_daily_watches(records_df):
    """Calculates number of movies watched each day."""
    if records_df.empty:
        return pd.DataFrame(columns=['Date', 'Movies Watched'])
    
    # Ensure watch_time is datetime type for operations
    records_df['watch_time'] = pd.to_datetime(records_df['watch_time'])
    
    overall_movies_watched_daily = records_df['watch_time'].dt.strftime('%Y-%m-%d').value_counts().reset_index()
    overall_movies_watched_daily.columns = ['Date', 'Movies Watched']
    overall_movies_watched_daily['Date'] = pd.to_datetime(overall_movies_watched_daily['Date'])
    overall_movies_watched_daily = overall_movies_watched_daily.sort_values('Date').reset_index(drop=True)
    return overall_movies_watched_daily

def analyze_hourly_watches(records_df):
    """Calculates time of day when movies are usually watched."""
    if records_df.empty:
        return pd.DataFrame(columns=['Hour (24h)', 'Movies Watched'])

    records_df['watch_time'] = pd.to_datetime(records_df['watch_time'])
    overall_watch_time_of_day_counts = records_df['watch_time'].dt.hour.value_counts().reset_index()
    overall_watch_time_of_day_counts.columns = ['Hour (24h)', 'Movies Watched']
    overall_watch_time_of_day_counts = overall_watch_time_of_day_counts.sort_values('Hour (24h)').reset_index(drop=True)
    return overall_watch_time_of_day_counts

def analyze_genres(records_df, movie_genres_lookup):
    """Analyzes overall top genres and top genres by hour."""
    if records_df.empty:
        return pd.DataFrame(columns=['Genre', 'Total Views']), pd.DataFrame(columns=['Hour', 'Genre', 'Views'])

    records_df['watch_time'] = pd.to_datetime(records_df['watch_time'])
    
    overall_genre_counts = Counter()
    overall_genre_time_counts = defaultdict(Counter)

    for index, record in records_df.iterrows(): # Iterate over DataFrame rows
        movie_id = record['movie_id']
        watch_hour = record['watch_time'].hour

        genres = movie_genres_lookup.get(movie_id, [])
        
        for genre in genres:
            overall_genre_counts[genre] += 1
            overall_genre_time_counts[watch_hour][genre] += 1
            
    # Convert overall genres to DataFrame
    overall_genres_df = pd.DataFrame(overall_genre_counts.most_common(10), columns=['Genre', 'Total Views'])

    # Convert genres by hour to a list of dicts for DataFrame
    genres_by_hour_data = []
    for hour, genres_counter in sorted(overall_genre_time_counts.items()):
        if genres_counter:
            for genre, count in genres_counter.most_common(5): # Top 5 per hour
                genres_by_hour_data.append({'Hour': f"{hour:02d}:00", 'Genre': genre, 'Views': count})
    genres_by_hour_df = pd.DataFrame(genres_by_hour_data)

    return overall_genres_df, genres_by_hour_df


# --- Streamlit App Layout ---

st.set_page_config(layout="wide", page_title="Movie Watch Data Dashboard")

st.title("🎬 Movie Watch Data Analysis Dashboard")

st.markdown("""
This dashboard provides insights into movie watching patterns by connecting to MongoDB (for movie metadata) 
and Cassandra (for watch history).
""")

# --- Database Connection Status ---
st.sidebar.header("Database Connection Status")
mongo_client = get_mongo_client()
cassandra_cluster, cassandra_session = get_cassandra_cluster_session()

# Only proceed if both connections are successful
if mongo_client and cassandra_session:
    # --- Load Data ---
    st.sidebar.header("Data Loading Status")
    # Pass the client/session objects with underscore prefix
    movie_genres_lookup = load_movie_genres(mongo_client)
    watch_history_records_df = load_watch_history(cassandra_session) # Now returns a DataFrame

    if movie_genres_lookup and not watch_history_records_df.empty:
        st.sidebar.success("All data loaded successfully!")

        # --- Query 1: Banyaknya film yang ditonton setiap hari ---
        st.header("1. Jumlah Film Ditonton Setiap Hari")
        daily_watches_df = analyze_daily_watches(watch_history_records_df)
        st.dataframe(daily_watches_df)
        st.line_chart(daily_watches_df.set_index('Date'))


        # --- Query 2: Waktu user biasa menonton film ---
        st.header("2. Waktu User Biasa Menonton Film (Per Jam)")
        hourly_watches_df = analyze_hourly_watches(watch_history_records_df)
        st.dataframe(hourly_watches_df)
        st.bar_chart(hourly_watches_df.set_index('Hour (24h)'))


        # --- Query 3: Genre film apa saja yang biasa ditonton user, dan berdasarkan waktunya apakah ada genre favorit yang ditonton di waktu tertentu ---
        st.header("3. Analisis Genre Film")
        overall_genres_df, genres_by_hour_df = analyze_genres(watch_history_records_df, movie_genres_lookup)

        st.subheader("Top 10 Genre Favorit Keseluruhan Pengguna")
        st.dataframe(overall_genres_df)
        st.bar_chart(overall_genres_df.set_index('Genre'))

        st.subheader("Top 5 Genre Favorit Per Waktu (Jam) Keseluruhan Pengguna")
        if not genres_by_hour_df.empty:
            st.dataframe(genres_by_hour_df)
            # You might want a more complex chart here, e.g., a grouped bar chart if feasible
            # For simplicity, let's show a table.
        else:
            st.write("Tidak ada data genre per jam yang tersedia.")

    else:
        st.warning("Could not load all necessary data for analysis. Please check logs or ensure data exists.")
else:
    st.error("Dashboard cannot function without successful database connections. Please check your Docker containers and configurations.")

st.markdown("---")
st.header("🎬 Movie Watcher Details")
st.write("Enter a movie title to see how many people watched it and their watch times.")

# --- Movie Title Suggestions ---
if mongo_client:
    sample_titles = get_sample_movie_titles(mongo_client, limit=10)
    if sample_titles:
        st.subheader("Or select from suggestions:")
        # Add an empty string at the beginning to allow no selection
        selected_suggestion = st.selectbox("Choose a movie title", [""] + sample_titles, key="movie_title_suggestion")
        if selected_suggestion:
            st.session_state.movie_title_input = selected_suggestion # Update the text input state

# Initialize session state for text input if not already present
if 'movie_title_input' not in st.session_state:
    st.session_state.movie_title_input = ""

movie_title_input = st.text_input("Enter Movie Title (e.g., Toy Story (1995))", 
                                  value=st.session_state.movie_title_input, 
                                  key="movie_title_text_input") # Changed key to avoid conflict

if st.button("Get Watcher Details"):
    if mongo_client and cassandra_session:
        mongo_db = mongo_client[MONGO_DB_NAME]
        mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]
        
        try:
            # 1. Find movie_id from MongoDB
            movie_doc = mongo_movies_collection.find_one({"title": movie_title_input}, {"movieId": 1, "_id": 0})
            
            if movie_doc:
                movie_id = movie_doc.get("movieId") # Convert to string to match Cassandra's movie_id type
                st.subheader(f"Watch Details for: **{movie_title_input}** (Movie ID: {movie_id})")

                # For this demonstration, we'll iterate through the pre-loaded DataFrame
                # which is already cached, avoiding direct heavy Cassandra query.
                
                if not watch_history_records_df.empty:
                    # Filter the pre-loaded DataFrame for the specific movie_id
                    movie_watch_history = watch_history_records_df[
                        watch_history_records_df['movie_id'] == movie_id
                    ]
                    
                    if not movie_watch_history.empty:
                        unique_watchers_count = movie_watch_history['user_id'].nunique()
                        st.write(f"**Total Unique Watchers:** {unique_watchers_count} people")
                        
                        st.write("---")
                        st.subheader("Watch Times and Users:")
                        # Display relevant columns
                        st.dataframe(movie_watch_history[['user_id', 'watch_time', 'duration_watched']].sort_values('watch_time'))
                    else:
                        st.info(f"No watch history found for '{movie_title_input}'.")
                else:
                    st.warning("Watch history data is not loaded.")
            else:
                st.warning(f"Movie '{movie_title_input}' not found in MongoDB. Please check the title spelling.")
        
        except Exception as e:
            st.error(f"An error occurred while fetching movie watcher details: {e}")
    else:
        st.warning("Database connections are not established.")


# --- Time-Based Search Section ---
st.markdown("---")
st.header("🕓 Search Movies Watched at Specific Date & Hour")

from datetime import datetime, timedelta

# --- Time Input UI ---
col1, col2 = st.columns(2)
with col1:
    selected_date = st.date_input("Select Date", value=datetime(2025, 5, 9).date(), key="date_filter_input")
with col2:
    selected_hour = st.slider("Select Hour", 0, 23, 18, key="hour_filter_input")

# --- Search Button ---
if st.button("Search by Time"):
    if cassandra_session and mongo_client:
        start_dt = datetime.combine(selected_date, datetime.min.time()).replace(hour=selected_hour)
        end_dt = start_dt + timedelta(hours=1)

        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Cassandra query
        cql = f"""
        SELECT user_id, movie_id, watch_time, duration_watched 
        FROM {CASSANDRA_TABLE} 
        WHERE watch_time >= '{start_str}' AND watch_time < '{end_str}' 
        ALLOW FILTERING
        """

        try:
            rows = cassandra_session.execute(cql)
            results = [row._asdict() for row in rows]

            if results:
                df = pd.DataFrame(results)

                # Lookup movie titles from MongoDB
                movies_col = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
                movie_map = {
                    str(m['movieId']): m['title']
                    for m in movies_col.find({}, {"movieId": 1, "title": 1, "_id": 0})
                }

                df['title'] = df['movie_id'].map(movie_map)
                df['watch_time'] = pd.to_datetime(df['watch_time'])

                st.success(f"🎬 Found {len(df)} watch records between {start_str} and {end_str}.")
                st.dataframe(df[['title', 'user_id', 'watch_time', 'duration_watched']].sort_values('watch_time'))

            else:
                st.info("No movies were watched in that time range.")
        except Exception as e:
            st.error(f"Error during time-based query: {e}")
    else:
        st.warning("MongoDB or Cassandra is not connected.")

st.markdown("---")

from pymongo import TEXT
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]

st.title("🔍 Sistem Query MongoDB & Cassandra")
col_sistem1, col_sistem2,col_sistem3 = st.columns(3)

with col_sistem1:
    # 1. QUERY MONGODB
    st.subheader("📦 Query MongoDB: Jumlah Film Genre Tertentu")
    genre_input = st.text_input("Masukkan genre (contoh: Action)", key="genre1")

    if genre_input:
        index_names = mongo_movies_collection.index_information()
        for name in index_names:
            if "genres" in name:
                mongo_movies_collection.drop_index(name)
        st.info("Index `genres` dihapus untuk pengujian non-indexed.")
        time.sleep(5)  # Tunggu agar index terhapus

        start = time.time()
        # Query tanpa index
        count_noindex = mongo_movies_collection.count_documents({"genres": {"$regex": genre_input, "$options": "i"}})
        time_noindex = time.time() - start
        st.write(f"Tanpa Index: {count_noindex} film — ⏱️ {time_noindex:.4f} detik")

        # Query dengan index
        mongo_movies_collection.create_index([("genres", TEXT)])
        st.info("Index `genres` dibuat ulang untuk pengujian indexed.")
        time.sleep(5)
        start = time.time()
        count_indexed = mongo_movies_collection.count_documents({"$text": {"$search": genre_input}})
        time_indexed = time.time() - start

        st.write(f"Dengan Index: {count_indexed} film — ⏱️ {time_indexed:.4f} detik")

with col_sistem2:
    # 2. QUERY CASSANDRA
    st.subheader("🕒 Query Cassandra: Jumlah film yang ditonton oleh user")

    user_input = st.text_input("Masukkan `user_id` untuk melihat riwayat nonton")
    if user_input:
        user_id = user_input.strip()

        try:
            ### --- NON-INDEXED QUERY ---
            # Hapus index dulu jika ada
            try:
                cassandra_session.execute("DROP INDEX IF EXISTS watch_history_new_user_id_idx")
                st.info("Index `user_id` dihapus untuk pengujian non-indexed.")
            except Exception as e:
                st.warning(f"Gagal menghapus index: {e}")

            # Query tanpa index
            start = time.time()
            rows_noindex = cassandra_session.execute(
                f"SELECT movie_id FROM watch_history_new WHERE user_id = '{user_id}'allow filtering"
            )
            movie_ids_noindex = [r.movie_id for r in rows_noindex]
            time_noindex = time.time() - start
            ### --- HASIL  Tanpa INDEX ---
            st.write(f"📦 Tanpa Index: {len(movie_ids_noindex)} film — ⏱️ {time_noindex:.4f} detik")
            st.write("Querry Index:")

            ### --- INDEXED QUERY ---
            # Buat index kembali
            try:
                cassandra_session.execute("CREATE INDEX IF NOT EXISTS ON watch_history_new (user_id)")
                st.info("Index `user_id` dibuat ulang untuk pengujian indexed.")
            except Exception as e:
                st.warning(f"Gagal membuat index: {e}")

            # Tunggu sejenak agar index aktif (jika perlu)
            time.sleep(5)

            # Query dengan index
            start = time.time()
            rows_indexed = cassandra_session.execute(
                f"SELECT movie_id FROM watch_history_new WHERE user_id = '{user_id}'"
            )
            movie_ids_indexed = [r.movie_id for r in rows_indexed]
            time_indexed = time.time() - start

            ### --- HASIL INDEX ---
            st.write(f"🚀 Dengan Index: {len(movie_ids_indexed)} film — ⏱️ {time_indexed:.4f} detik")

        except Exception as e:
            st.error(f"Gagal melakukan query: {e}")

with col_sistem3:
    # 3. QUERY GABUNGAN
    st.subheader("🔗 Gabungkan Cassandra + MongoDB (Data Film yang Ditonton User)")

    selected_user_compare = st.text_input("Masukkan `user_id` untuk membandingkan")

    if selected_user_compare:
        st.markdown("### 🚫 Non-Indexed Query (Tanpa Optimasi)")
        try:
            # NON-INDEXED VERSION
            # Hapus index dulu jika ada
            try:
                cassandra_session.execute("DROP INDEX IF EXISTS watch_history_new_user_id_idx")
                st.info("Index `user_id` dihapus untuk pengujian non-indexed.")
            except Exception as e:
                st.warning(f"Gagal menghapus index: {e}")

            index_names = mongo_movies_collection.index_information()
            for name in index_names:
                if "movieId" in name:
                    mongo_movies_collection.drop_index(name)
            st.info("Index `movieId` dihapus untuk pengujian non-indexed.")
            time.sleep(5)# Tunggu agar index terhapus

            start_non = time.time()
            # Cassandra (tanpa index akan tetap perlu ALLOW FILTERING)
            query_non_indexed = f"""
            SELECT movie_id FROM watch_history_new WHERE user_id = '{selected_user_compare}' ALLOW FILTERING
            """
            rows_non = cassandra_session.execute(query_non_indexed)
            df_non = pd.DataFrame([r._asdict() for r in rows_non])

            if df_non.empty:
                st.warning("User tidak ditemukan pada query non-indexed.")
            else:
                movie_ids_non = df_non["movie_id"].dropna().unique().tolist()
                mongo_movies_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
                cursor_non = mongo_movies_collection.find(
                    {"movieId": {"$in": movie_ids_non}},
                    {"_id": 0, "movieId": 1, "title": 1, "genres": 1}
                )
                movie_map_non = {
                    doc["movieId"]: {
                        "title": doc.get("title", ""),
                        "genres": doc.get("genres", "")
                    } for doc in cursor_non
                }
                df_non["title"] = df_non["movie_id"].map(lambda m: movie_map_non.get(m, {}).get("title", ""))
                df_non["genres"] = df_non["movie_id"].map(lambda m: movie_map_non.get(m, {}).get("genres", ""))
                st.dataframe(df_non[["movie_id", "title", "genres"]].drop_duplicates())

            end_non = time.time()
            st.info(f"⏱️ Non-Indexed Execution Time: {end_non - start_non:.4f} detik")

        except Exception as e:
            st.error(f"Gagal menjalankan query non-indexed: {e}")

        st.markdown("### ✅ Indexed Query (Optimized)")
        try:
            # INDEXED VERSION
            # Buat index Cassandra jika belum ada
            cassandra_session.execute("CREATE INDEX IF NOT EXISTS watch_history_new_user_id_idx ON watch_history_new (user_id)")
            st.info("Index `user_id` dibuat untuk pengujian indexed.")
            # Buat index MongoDB jika belum ada
            mongo_movies_collection.create_index("movieId", name="movieId_index", background=True)
            st.info("Index `movieId` dibuat untuk pengujian indexed.")
            time.sleep(5)  # Tunggu agar index dibuat

            start_opt = time.time()

            # Cassandra: index harus sudah dibuat di field user_id
            query_indexed = f"""
            SELECT movie_id FROM watch_history_new WHERE user_id = '{selected_user_compare}'
            """
            rows_opt = cassandra_session.execute(query_indexed)
            df_opt = pd.DataFrame([r._asdict() for r in rows_opt])

            if df_opt.empty:
                st.warning("User tidak ditemukan pada query indexed.")
            else:
                movie_ids_opt = df_opt["movie_id"].dropna().unique().tolist()
                mongo_movies_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]

                # MongoDB indexing on movieId assumed to be created
                cursor_opt = mongo_movies_collection.find(
                    {"movieId": {"$in": movie_ids_opt}},
                    {"_id": 0, "movieId": 1, "title": 1, "genres": 1}
                )
                movie_map_opt = {
                    doc["movieId"]: {
                        "title": doc.get("title", ""),
                        "genres": doc.get("genres", "")
                    } for doc in cursor_opt
                }
                df_opt["title"] = df_opt["movie_id"].map(lambda m: movie_map_opt.get(m, {}).get("title", ""))
                df_opt["genres"] = df_opt["movie_id"].map(lambda m: movie_map_opt.get(m, {}).get("genres", ""))
                st.dataframe(df_opt[["movie_id", "title", "genres"]].drop_duplicates())

            end_opt = time.time()
            st.success(f"⚡ Indexed Execution Time: {end_opt - start_opt:.4f} detik")

        except Exception as e:
            st.error(f"Gagal menjalankan query indexed: {e}")



# Input Queries Gabungan MongoDB dan Cassandra
st.markdown("---")
st.title("🧠 Optimasi Index")
st.header("📝 Input Query Gabungan MongoDB dan Cassandra")
user_mongo_query_str = st.text_area("MongoDB Query Filter (JSON)", height=100, key="mongo_query_input", value="{}")
cassandra_query_input = st.text_area("CQL Cassandra Query", height=100, key="cassandra_query_input", value="Select * from watch_history_new")

# Tombol tunggal untuk menjalankan kedua query
if st.button("🔍 Jalankan Kedua Query dan Gabungkan Hasil"):
    mongo_results_df = pd.DataFrame()
    cassandra_results_df = pd.DataFrame()
    merged_df = pd.DataFrame()
    mongo_db = mongo_client[MONGO_DB_NAME]
    mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]
    start_timemango = time.time()
    start_timemerge = time.time()
    # === Eksekusi MongoDB Query ===
    if mongo_client:
        try:
            query_filter = json.loads(user_mongo_query_str)

            mongo_results_cursor = mongo_movies_collection.find(query_filter, {"_id": 0, "movieId": 1, "title": 1, "genres": 1})
            mongo_results = list(mongo_results_cursor)
            if mongo_results:
                mongo_results_df = pd.DataFrame(mongo_results)
                mongo_results_df.rename(columns={'movieId': 'movie_id'}, inplace=True)
                st.subheader("🎬 Hasil Query MongoDB:")
                st.dataframe(mongo_results_df)
            else:
                st.info("MongoDB: Tidak ada dokumen yang cocok.")
        except json.JSONDecodeError:
            st.error("MongoDB: Format JSON tidak valid.")
        except Exception as e:
            st.error(f"MongoDB: Terjadi kesalahan: {e}")
    else:
        st.warning("MongoDB: Koneksi belum tersedia.")

    end_timemango = time.time()
    mongo_duration = end_timemango - start_timemango
    st.success(f"⏱️ Waktu eksekusi MongoDB: {mongo_duration:.3f} detik")
    # === Eksekusi Cassandra Query ===
    start_timecassandra = time.time()
    if cassandra_session:
        try:
            rows = cassandra_session.execute(cassandra_query_input)
            cassandra_results = [row._asdict() for row in rows]
            if cassandra_results:
                cassandra_results_df = pd.DataFrame(cassandra_results)
                st.subheader("🎥 Hasil Query Cassandra:")
                st.dataframe(cassandra_results_df)
            else:
                st.info("Cassandra: Tidak ada hasil ditemukan.")
        except Exception as e:
            st.error(f"Cassandra: Terjadi kesalahan: {e}")
    else:
        st.warning("Cassandra: Koneksi belum tersedia.")

    end_timecassandra = time.time()
    cassandra_duration = end_timecassandra - start_timecassandra
    st.success(f"⏱️ Waktu eksekusi Cassandra: {cassandra_duration:.3f} detik")

    # === Gabungkan hasil berdasarkan movie_id ===
    if not mongo_results_df.empty and not cassandra_results_df.empty:
        merged_df = pd.merge(cassandra_results_df, mongo_results_df, on='movie_id', how='outer')
        st.subheader("🔗 Gabungan Hasil (MongoDB + Cassandra):")
        st.dataframe(merged_df)
    elif mongo_results_df.empty or cassandra_results_df.empty:
        st.info("Gabungan: Salah satu hasil query kosong, tidak dapat melakukan penggabungan.")
    
    end_timemerge = time.time()
    merge_duration = end_timemerge - start_timemerge
    st.success(f"⏱️ Waktu eksekusi Merge: {merge_duration:.3f} detik")

st.subheader("Buat Index untuk Optimasi Query (Menampilkan user_id, title, dan genres)")

mongo_db = mongo_client[MONGO_DB_NAME]
mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]
# Jalankan hanya sekali
if st.button("🔧 Buat Index MongoDB pada `movieId`"):
    try:
        mongo_movies_collection.create_index("movieId")
        st.success("Index `movieId` berhasil dibuat di MongoDB.")
    except Exception as e:
        st.error(f"Gagal membuat index: {e}")

if st.button("🔧 Buat Index Cassandra pada `user_id`"):
    try:
        cassandra_session.execute("CREATE INDEX IF NOT EXISTS ON watch_history_new (user_id)")
        st.success("Index `movie_id` berhasil dibuat di Cassandra.")
    except Exception as e:
        st.error(f"Gagal membuat index: {e}")

def get_movie_details(movie_ids: list):
    movie_details = {}
    if movie_ids:
        cursor = mongo_movies_collection.find(
            {"movieId": {"$in": movie_ids}},
            {"_id": 0, "movieId": 1, "title": 1, "genres": 1}
        )
        for doc in cursor:
            movie_details[doc["movieId"]] = {
                "title": doc.get("title", ""),
                "genres": doc.get("genres", "")
            }
    return movie_details


st.subheader("🔍 Gabungkan Data Cassandra + MongoDB (Optimized by Index)")

selected_user = st.text_input("Masukkan `user_id` untuk cari riwayat nonton")

if st.button("🔗 Gabungkan (dengan Index)"):
    if not selected_user:
        st.warning("Masukkan user_id terlebih dahulu.")
    else:
        try:
            start_time = time.time()

            # Query Cassandra pakai index user_id
            cass_query = f"""
            SELECT user_id, movie_id FROM watch_history_new
            WHERE user_id = '{selected_user}'
            """
            rows = cassandra_session.execute(cass_query)
            cass_df = pd.DataFrame([r._asdict() for r in rows])

            if not cass_df.empty:
                # Ambil movie_ids unik
                movie_ids = cass_df["movie_id"].dropna().unique().tolist()

                # Query MongoDB pakai index movieId
                movie_map = get_movie_details(movie_ids)

                # Gabungkan
                cass_df["title"] = cass_df["movie_id"].map(lambda m: movie_map.get(m, {}).get("title", ""))
                cass_df["genres"] = cass_df["movie_id"].map(lambda m: movie_map.get(m, {}).get("genres", ""))

                # Tampilkan hanya kolom penting
                display_df = cass_df[["user_id", "title", "genres"]]
                st.dataframe(display_df)
            else:
                st.info("Tidak ada data untuk user tersebut.")

            end_time = time.time()
            st.success(f"⏱️ Waktu eksekusi: {end_time - start_time:.3f} detik")

        except Exception as e:
            st.error(f"❌ Gagal menggabungkan data: {e}")

## Input Query Dinamis untuk MongoDB dan Cassandra
st.markdown("---")
st.title("Input Dynamic Query")
st.header("🎛️ Filter Mango Query")
colm1, colm2 = st.columns(2)
with colm1:
    movie_id_input = st.text_input("Movie ID (kosongkan jika tidak ingin filter) Example: 1")
with colm2:
    genre_input = st.text_input("Genre (kosongkan jika tidak ingin filter) Example: Comedy")
title_keyword = st.text_input("Judul (kosongkan jika tidak ingin filter) Example: Toy Story(1995)")

mongo_filter = {}
if movie_id_input:
    mongo_filter['movieId'] = int(movie_id_input)
if genre_input:
    mongo_filter['genres'] = genre_input
if title_keyword:
    mongo_filter['title'] = title_keyword  

st.write("**Mongo Filter:**", mongo_filter)

st.header("🎛️ Filter Cassandra Query")
import datetime

# ── Kolom 1 dan 2 untuk user_id dan movie_id ──
col1, col2 = st.columns(2)
with col1:
    user_id_input = st.text_input("User ID (opsional) Example: user_1")
with col2:
    movie_id_input = st.text_input("Movie ID (opsional) Example: 1")

# ── Kolom 3 dan 4 untuk durasi ──
col3, col4 = st.columns(2)
with col3:
    duration_operator = st.radio(
        "Operator Duration",
        options=["", "=", ">", "<", ">=", "<="],
        index=0,
        horizontal=True
    )
with col4:
    duration_value = st.text_input("Duration (menit) (opsional) Example: 20")
# ── Rentang tanggal dan waktu ──
st.header("⏱️ Rentang Waktu Watch Time")

col5, col6 = st.columns(2)
with col5:
    start_date = st.date_input("Tanggal Mulai", datetime.date(2025, 5, 1))
    start_time = st.time_input("Jam Mulai", datetime.time(0, 0))
with col6:
    end_date = st.date_input("Tanggal Akhir", datetime.date(2025, 5, 16))
    end_time = st.time_input("Jam Akhir", datetime.time(23, 59))
start_datetime = datetime.datetime.combine(start_date, start_time)
end_datetime = datetime.datetime.combine(end_date, end_time)

cql_base = "SELECT user_id, movie_id, watch_time, duration_watched FROM watch_history_new"

conditions = []
if user_id_input:
    conditions.append(f"user_id = '{user_id_input}'")
if movie_id_input:
    try:
        movie_id_val = int(movie_id_input)
        conditions.append(f"movie_id = {movie_id_val}")
    except ValueError:
        st.error("Movie ID harus berupa angka")
if duration_operator and duration_value:
    try:
        duration_val = int(duration_value)
        conditions.append(f"duration_watched {duration_operator} {duration_val}")
    except ValueError:
        st.error("Duration watched harus berupa angka")
conditions.append(f"watch_time >= '{start_datetime.isoformat()}'")
conditions.append(f"watch_time <= '{end_datetime.isoformat()}'")
if conditions:
    cql_query = cql_base + " WHERE " + " AND ".join(conditions)+ " ALLOW FILTERING"
else:
    cql_query = cql_base + " LIMIT 100"

st.markdown(f"**CQL Query:**\n```cql\n{cql_query}\n```")

if st.button("Jalankan Query Dinamis"):
    # Mongo
    
    try:
        mongo_db = mongo_client[MONGO_DB_NAME]
        mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]

        # Pakai mongo_filter dari input form (genre/title)
        mongo_results_cursor = mongo_movies_collection.find(
            mongo_filter, {"_id": 0, "movieId": 1, "title": 1, "genres": 1}
        )
        mongo_results = list(mongo_results_cursor)
        
        if mongo_results:
            mongo_results_df = pd.DataFrame(mongo_results)
            mongo_results_df.rename(columns={'movieId': 'movie_id'}, inplace=True)
            st.subheader("🎬 Hasil Query MongoDB:")
            st.dataframe(mongo_results_df)
        else:
            st.info("MongoDB: Tidak ada hasil ditemukan.")
    except Exception as e:
        st.error(f"MongoDB error: {e}")

    # Cassandra
    try:
        rows = cassandra_session.execute(cql_query)
        cassandra_results = [r._asdict() for r in rows]
        cassandra_results_df = pd.DataFrame(cassandra_results)
        st.dataframe(cassandra_results_df)
    except Exception as e:
        st.error(f"Cassandra error: {e}")

    # Merge dan tampilkan jika memungkinkan
    # ... lakukan merge seperti biasa ...


# Fungsi bantu MongoDB
def get_sample_fields(collection):
    sample = collection.find_one()
    if sample:
        return [k for k in sample.keys() if k != "_id"]
    return []

def get_mongo_collections(client, db_name):
    return client[db_name].list_collection_names()

def insert_mongo_document(collection, data_dict):
    try:
        collection.insert_one(data_dict)
        st.success("📥 Dokumen berhasil ditambahkan.")
    except Exception as e:
        st.error(f"Gagal menambahkan dokumen: {e}")

def delete_mongo_documents(collection, query_dict):
    try:
        result = collection.delete_many(query_dict)
        st.success(f"🗑️ {result.deleted_count} dokumen berhasil dihapus.")
    except Exception as e:
        st.error(f"Gagal menghapus dokumen: {e}")

def update_mongo_documents(collection, filter_dict, update_dict):
    try:
        result = collection.update_many(filter_dict, {"$set": update_dict})
        st.success(f"✏️ {result.modified_count} dokumen berhasil diupdate.")
    except Exception as e:
        st.error(f"Gagal mengupdate dokumen: {e}")

def find_mongo_documents(collection, query_dict):
    try:
        cursor = collection.find(query_dict)
        docs = list(cursor)
        if docs:
            st.dataframe(pd.DataFrame(docs).drop(columns="_id", errors="ignore"))
        else:
            st.info("🔍 Tidak ada dokumen yang ditemukan.")
    except Exception as e:
        st.error(f"Gagal melakukan pencarian: {e}")

st.markdown("---")
st.title("🍃 MongoDB and Cassandra CRUD")

import ast

def auto_cast(val):
    try:
        return ast.literal_eval(val)
    except:
        return val  # Tetap string jika gagal cast
st.header("🍃 MongoDB")
st.subheader("📝 Tambah Data ke Collection MongoDB")

# Ambil daftar collection
collection_names = mongo_client[MONGO_DB_NAME].list_collection_names()
selected_collection = st.selectbox("Pilih Collection", collection_names, key="mongo_add")

# Input key-value
st.subheader("🔑 Masukkan Data (Key-Value)")
num_fields = st.number_input("Jumlah Kolom/Data", min_value=1, max_value=20, value=3, step=1)

user_data = {}
for i in range(num_fields):
    col_key, col_value = st.columns(2) 

    with col_key:
        key = st.text_input(f"Nama Kolom {i+1}", key=f"key_{i}", placeholder="Mis: id")

    with col_value:
        value = st.text_input(f"Isi Nilai {i+1}", key=f"value_{i}", placeholder="Mis: 1")
    if key:
        user_data[key] = auto_cast(value)

if st.button("➕ Tambahkan Data ke MongoDB"):
    if selected_collection and user_data:
        try:
            mongo_client[MONGO_DB_NAME][selected_collection].insert_one(user_data)
            st.success("✅ Dokumen berhasil ditambahkan.")
        except Exception as e:
            st.error(f"❌ Gagal menambahkan dokumen: {e}")
    else:
        st.warning("⚠️ Harap isi semua kolom dan pilih collection.")

st.markdown("---")
st.header("📂 Lihat Isi Collection MongoDB")

selected_collection_view = st.selectbox("Pilih Collection untuk Ditampilkan", collection_names, key="mongo_view")

if st.button("📊 Tampilkan Data"):
    try:
        docs = list(mongo_client[MONGO_DB_NAME][selected_collection_view].find({}, {"_id": 0}))  # tanpa _id
        if docs:
            df = pd.DataFrame(docs)
            st.dataframe(df)
        else:
            st.info("Collection kosong.")
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")

st.markdown("---")
st.header("📁 Manajemen Collection MongoDB")

# Buat Collection
col_createCollection, col_deleteCollect = st.columns(2)
with col_createCollection:
    st.subheader("📦 Buat Collection Baru")
    new_col_name = st.text_input("Nama Collection Baru")
    if st.button("➕ Buat Collection"):
        if new_col_name:
            try:
                mongo_client[MONGO_DB_NAME].create_collection(new_col_name)
                st.success(f"Collection `{new_col_name}` berhasil dibuat.")
            except Exception as e:
                st.error(f"Gagal membuat collection: {e}")
        else:
            st.warning("Isi nama collection terlebih dahulu.")

# Hapus Collection
with col_deleteCollect:
    st.subheader("🗑️ Hapus Collection")
    existing_collections = mongo_client[MONGO_DB_NAME].list_collection_names()
    col_to_delete = st.selectbox("Pilih Collection yang ingin dihapus", existing_collections)
    if st.button("🗑️ Hapus Collection"):
        try:
            mongo_client[MONGO_DB_NAME].drop_collection(col_to_delete)
            st.success(f"Collection `{col_to_delete}` berhasil dihapus.")
        except Exception as e:
            st.error(f"Gagal menghapus collection: {e}")


st.markdown("---")
st.header("📝 Tambah Data ke Collection MongoDB")
# Pilih collection
mongo_collections = get_mongo_collections(mongo_client, MONGO_DB_NAME)
selected_collection = st.selectbox("Pilih Collection", mongo_collections)
mongo_collection = mongo_client[MONGO_DB_NAME][selected_collection]

fields = get_sample_fields(mongo_collection)

# --- INSERT ---
col_insert, col_delete = st.columns(2)
with col_insert: 
    st.header("📥 Tambah Dokumen (Form)")

    insert_inputs = {}
    for field in fields:
        insert_inputs[field] = st.text_input(f"Nilai untuk `{field}`", key=f"mongo_insert_{field}")

    if st.button("➕ Tambahkan Dokumen"):
        insert_data = {}
        for k, v in insert_inputs.items():
            if v != "":
                insert_data[k] = auto_cast(v)

        if insert_data:
            try:
                mongo_collection.insert_one(insert_data)
                st.success("✅ Dokumen berhasil ditambahkan.")
            except Exception as e:
                st.error(f"❌ Gagal menambahkan dokumen: {e}")
        else:
            st.warning("⚠️ Isi minimal satu field untuk insert.")

# --- DELETE ---
with col_delete:
    st.header("🗑️ Hapus Dokumen Berdasarkan Filter")
    delete_filters = {}
    for field in fields:
        val = st.text_input(f"Filter `{field}`", key=f"mongo_delete_{field}")
        if val != "":
            delete_filters[field] = auto_cast(val)

    if st.button("🗑️ Hapus Dokumen"):
        if delete_filters:
            delete_mongo_documents(mongo_collection, delete_filters)
        else:
            st.warning("Isi minimal satu field sebagai filter.")

# --- UPDATE ---
st.header("✏️ Update Dokumen")
col_update_before, col_update_after = st.columns(2)
with col_update_before:
    st.subheader("🔄 Update Dokumen Berdasarkan Filter")
    st.write(" Filter untuk Dokumen yang Akan Diupdate")
    update_filters = {}
    for field in fields:
        val = st.text_input(f"Filter `{field}`", key=f"mongo_update_filter_{field}")
        if val != "":
            update_filters[field] = auto_cast(val)
with col_update_after:
    st.header("🔄 Nilai Baru untuk Update")
    st.write("Nilai Baru yang Akan Diupdate")
    update_new_values = {}
    for field in fields:
        val = st.text_input(f"Update `{field}` ke", key=f"mongo_update_value_{field}")
        if val != "":
            update_new_values[field] = auto_cast(val)

if st.button("✏️ Update Dokumen"):
    if update_filters and update_new_values:
        update_mongo_documents(mongo_collection, update_filters, update_new_values)
    else:
        st.warning("Isi minimal satu filter dan satu nilai baru.")

# --- READ / FIND ---
st.header("🔍 Cari Dokumen (Optional Filter)")
find_filters = {}
for field in fields:
    val = st.text_input(f"Filter `{field}`", key=f"mongo_find_{field}")
    if val != "":
        find_filters[field] = auto_cast(val)

if st.button("🔍 Cari Dokumen"):
    find_mongo_documents(mongo_collection, find_filters)



st.markdown("---")
st.header("⚡ Cassandra CRUD Operations")

# --- Fungsi bantu ---
def get_tables(session):
    keyspace = session.keyspace
    rows = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}'")
    return [r.table_name for r in rows]

def create_table(session, table_name, columns_def, primary_key):
    try:
        cql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def}, PRIMARY KEY ({primary_key}))"
        session.execute(cql)
        st.success(f"Tabel `{table_name}` berhasil dibuat!")
    except Exception as e:
        st.error(f"Gagal membuat tabel: {e}")

def drop_table(session, table_name):
    try:
        session.execute(f"DROP TABLE IF EXISTS {table_name}")
        st.success(f"Tabel `{table_name}` berhasil dihapus!")
    except Exception as e:
        st.error(f"Gagal menghapus tabel: {e}")

def get_table_columns(session, table_name):
    # Ambil info kolom dan tipe data dari system_schema.columns
    query = f"""
    SELECT column_name, type FROM system_schema.columns 
    WHERE keyspace_name = '{session.keyspace}' AND table_name = '{table_name}'
    """
    rows = session.execute(query)
    return {row.column_name: row.type for row in rows}

def get_table_columns(session, table_name):
    # Ambil info kolom dan tipe data dari system_schema.columns
    query = f"""
    SELECT column_name, type FROM system_schema.columns 
    WHERE keyspace_name = '{session.keyspace}' AND table_name = '{table_name}'
    """
    rows = session.execute(query)
    return {row.column_name: row.type for row in rows}

def insert_data(session, table_name, data_dict):
    try:
        # Ambil tipe kolom
        column_types = get_table_columns(session, table_name)

        cols = []
        vals = []
        for col, val in data_dict.items():
            cols.append(col)
            col_type = column_types.get(col, "text")

            # Konversi tipe sesuai
            if val == "":
                # Jika kosong, bisa masukkan null
                vals.append("null")
            elif col_type in ['int', 'bigint', 'varint', 'smallint', 'tinyint']:
                # Coba konversi ke int
                try:
                    int_val = int(val)
                    vals.append(str(int_val))
                except:
                    raise ValueError(f"Kolom {col} harus berupa angka (int)")
            elif col_type in ['float', 'double', 'decimal']:
                try:
                    float_val = float(val)
                    vals.append(str(float_val))
                except:
                    raise ValueError(f"Kolom {col} harus berupa angka (float)")
            else:
                # Anggap tipe string/text, beri tanda kutip dan escape kutip di dalam val
                escaped_val = val.replace("'", "''")
                vals.append(f"'{escaped_val}'")

        cols_str = ", ".join(cols)
        vals_str = ", ".join(vals)
        cql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"
        session.execute(cql)
        st.success("Data berhasil ditambahkan.")
    except Exception as e:
        st.error(f"Gagal menambahkan data: {e}")

def delete_data(session, table_name, where_clause):
    try:
        cql = f"DELETE FROM {table_name} WHERE {where_clause}"
        session.execute(cql)
        st.success("Data berhasil dihapus.")
    except Exception as e:
        st.error(f"Gagal menghapus data: {e}")

def update_data(session, table_name, set_clause, where_clause):
    try:
        cql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        session.execute(cql)
        st.success("Data berhasil diperbarui.")
    except Exception as e:
        st.error(f"Gagal mengupdate data: {e}")


# Ambil daftar tabel
def get_table_names(session):
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{session.keyspace}'"
    return [row.table_name for row in session.execute(query)]

# Ambil kolom dari sebuah tabel
def get_columns_for_table(session, table_name):
    query = f"""
    SELECT column_name FROM system_schema.columns
    WHERE keyspace_name = '{session.keyspace}' AND table_name = '{table_name}'
    """
    return [row.column_name for row in session.execute(query)]


# --- UI ---

st.subheader("📋 Daftar Tabel yang Tersedia")
tables = get_tables(cassandra_session)
st.write(tables)

st.markdown("---")
colAddCassandra, colDropCassandra = st.columns(2)
with colAddCassandra:
    st.subheader("➕ Buat Tabel Baru")

    table_name_new = st.text_input("Nama Tabel Baru")
    columns_text = st.text_area("Definisikan Kolom (contoh: id int, name text, age int)")
    primary_key_new = st.text_input("Primary Key (contoh: id)")

    if st.button("Buat Tabels"):
        if table_name_new and columns_text and primary_key_new:
            create_table(cassandra_session, table_name_new, columns_text, primary_key_new)
        else:
            st.warning("Isi semua field terlebih dahulu.")

with colDropCassandra:
    st.subheader("🗑️ Hapus Tabel")

    table_to_drop = st.selectbox("Pilih Tabel untuk Dihapus", tables)
    if st.button("Hapus Tabels"):
        if table_to_drop:
            drop_table(cassandra_session, table_to_drop)
        else:
            st.warning("Pilih tabel terlebih dahulu.")

st.markdown("---")
st.subheader("📊 Lihat Data dari Tabel Cassandra")

table_to_view = st.selectbox("Pilih Tabel untuk Ditampilkan", tables, key="view")

if table_to_view:
    try:
        rows = cassandra_session.execute(f"SELECT * FROM {table_to_view}")
        data = rows.all()
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df)
        else:
            st.info("Tabel ini masih kosong.")
    except Exception as e:
        st.error(f"Gagal menampilkan data: {e}")

st.markdown("---")
colInsertCassandra, colDeleteCassandra = st.columns(2)
with colInsertCassandra:
    st.subheader("✏️ Insert Data ke Tabel")

    table_to_insert = st.selectbox("Pilih Tabel untuk Insert", tables, key="insert")
    if table_to_insert:
        col_names = cassandra_session.execute(f"SELECT * FROM system_schema.columns WHERE keyspace_name='{cassandra_session.keyspace}' AND table_name='{table_to_insert}'")
        cols = [c.column_name for c in col_names]
        data_inputs = {}
        for col in cols:
            data_inputs[col] = st.text_input(f"Nilai untuk kolom `{col}`", key=f"insert_{col}")

        if st.button("Insert Datas"):
            if all(v != "" for v in data_inputs.values()):
                insert_data(cassandra_session, table_to_insert, data_inputs)
            else:
                st.warning("Isi semua kolom sebelum insert.")

with colDeleteCassandra:
    st.subheader("❌ Delete Data dari Tabel")
    table_to_delete = st.selectbox("Pilih Tabel untuk Delete", tables, key="delete")
    if table_to_delete:
        pk_cols = cassandra_session.execute(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{cassandra_session.keyspace}' AND table_name='{table_to_delete}' AND kind='partition_key' allow filtering")
        pk_list = [pk.column_name for pk in pk_cols]

        where_conditions = []
        for pk in pk_list:
            val = st.text_input(f"Nilai Primary Key `{pk}` untuk Delete", key=f"delete_{pk}")
            if val:
                if val.isdigit():
                    where_conditions.append(f"{pk} = {val}")
                else:
                    where_conditions.append(f"{pk} = '{val}'")

        if st.button("Delete Datas"):
            if where_conditions:
                where_clause = " AND ".join(where_conditions)
                delete_data(cassandra_session, table_to_delete, where_clause)
            else:
                st.warning("Masukkan nilai primary key untuk delete.")

st.markdown("---")
st.subheader("📝 Update Data di Tabel")

table_to_update = st.selectbox("Pilih Tabel untuk Update", tables, key="update")
if table_to_update:
    # Kolom update bebas
    col_names_update = cassandra_session.execute(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{cassandra_session.keyspace}' AND table_name='{table_to_update}'")
    cols_update = [c.column_name for c in col_names_update]

    set_col = st.selectbox("Pilih Kolom untuk Diupdate", cols_update)
    new_val = st.text_input(f"Nilai Baru untuk `{set_col}`")

    pk_cols_update = cassandra_session.execute(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{cassandra_session.keyspace}' AND table_name='{table_to_update}' AND kind='partition_key' allow filtering")
    pk_list_update = [pk.column_name for pk in pk_cols_update]

    where_conditions_update = []
    for pk in pk_list_update:
        val = st.text_input(f"Nilai Primary Key `{pk}` untuk Kondisi Update", key=f"update_{pk}")
        if val:
            if val.isdigit():
                where_conditions_update.append(f"{pk} = {val}")
            else:
                where_conditions_update.append(f"{pk} = '{val}'")

    if st.button("Update Datas"):
        if new_val and where_conditions_update:
            set_clause = f"{set_col} = '{new_val}'" if not new_val.isdigit() else f"{set_col} = {new_val}"
            where_clause = " AND ".join(where_conditions_update)
            update_data(cassandra_session, table_to_update, set_clause, where_clause)
        else:
            st.warning("Isi nilai baru dan kondisi primary key untuk update.")      



st.markdown("---")
st.title("Indexing dan Query Data")
st.header("🔍Querry Data Manual") 
st.write("Perform direct searches on MongoDB or Cassandra.")

# --- Tabbed interface for search types ---
search_tab = st.tabs(["MongoDB Search", "Cassandra Search"])

with search_tab[0]: # MongoDB Search Tab
    st.subheader("Search Movies in MongoDB (Custom Query)")
    st.write("Masukkan query MongoDB `find` (JSON) atau pipeline `aggregate` (list of stages).")

    user_mongo_query_str = st.text_area("MongoDB Query Filter or Aggregation (JSON)", height=100, key="mongo_query_input_tab", value="[\n  { \"$project\": { \"genres\": { \"$split\": [\"$genres\", \"|\"] } } },\n  { \"$unwind\": \"$genres\" },\n  { \"$group\": { \"_id\": \"$genres\" } },\n  { \"$sort\": { \"_id\": 1 } }\n]"

)

    if st.button("Execute MongoDB Query", key="exec_mongo_query_button"):
        if mongo_client:
            try:
                query_input = json.loads(user_mongo_query_str)
                mongo_db = mongo_client[MONGO_DB_NAME]
                mongo_movies_collection = mongo_db[MONGO_COLLECTION_NAME]

                if isinstance(query_input, dict):
                    # Query find()
                    results_cursor = mongo_movies_collection.find(query_input, {'_id': 0})
                    st.info("Menjalankan query `.find()`")
                elif isinstance(query_input, list):
                    # Pipeline aggregate()
                    results_cursor = mongo_movies_collection.aggregate(query_input)
                    st.info("Menjalankan query `.aggregate()`")
                else:
                    st.error("Input harus berupa JSON Object (`{}`) atau List (`[]`).")
                    results_cursor = []

                query_results = list(results_cursor)
                if query_results:
                    results_df = pd.DataFrame(query_results)
                    st.subheader("MongoDB Query Results:")
                    st.dataframe(results_df)
                else:
                    st.info("Tidak ada dokumen yang cocok dengan query.")

            except json.JSONDecodeError:
                st.error("Format JSON tidak valid. Pastikan JSON ditulis dengan benar.")
            except Exception as e:
                st.error(f"Terjadi error saat menjalankan query MongoDB: {e}")
        else:
            st.warning("MongoDB client belum terhubung.")


with search_tab[1]: # Cassandra Search Tab
    st.subheader("Search Watch History in Cassandra (Custom CQL)")
    st.write("Enter a CQL `SELECT` query (e.g., `SELECT * FROM watch_history LIMIT 10;`).")
    st.warning("Queries requiring `ALLOW FILTERING` can be inefficient on large datasets.")

    user_cassandra_query_str = st.text_area("Cassandra CQL Query", height=100, key="cassandra_query_input_tab", value = "SELECT * FROM watch_history LIMIT 10;")

    if st.button("Execute Cassandra Query", key="exec_cassandra_query_button"):
        if cassandra_session:
            try:
                # Execute the CQL query
                rows = cassandra_session.execute(user_cassandra_query_str)
                
                # Convert results to a list of dictionaries, then to a DataFrame
                query_results = []
                for row in rows:
                    query_results.append(row._asdict())
                
                if query_results:
                    results_df = pd.DataFrame(query_results)
                    st.subheader("Cassandra Query Results:")
                    st.dataframe(results_df)
                else:
                    st.info("No records found matching your query.")

            except Exception as e:
                st.error(f"An error occurred during Cassandra query execution: {e}")
        else:
            st.warning("Cassandra session is not connected.")


# Membuat index
def create_index(session, table_name, column_name, index_name=None):
    try:
        if not index_name:
            index_name = f"{table_name}_{column_name}_idx"
        cql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
        session.execute(cql)
        st.success(f"Index `{index_name}` berhasil dibuat.")
    except Exception as e:
        st.error(f"Gagal membuat index: {e}")

# Menampilkan semua index pada keyspace
def list_indexes(session):
    query = f"""
    SELECT table_name, index_name, kind, options 
    FROM system_schema.indexes 
    WHERE keyspace_name = '{session.keyspace}'
    """
    rows = session.execute(query)
    return rows

# Menghapus index
def drop_index(session, index_name):
    try:
        cql = f"DROP INDEX IF EXISTS {index_name}"
        session.execute(cql)
        st.success(f"Index `{index_name}` berhasil dihapus.")
    except Exception as e:
        st.error(f"Gagal menghapus index: {e}")


st.markdown("---")
st.header("🔎 Manajemen Index Cassandra")

tab1, tab2, tab3 = st.tabs(["➕ Buat Index", "📋 Lihat Index", "🗑️ Hapus Index"])

with tab1:
    st.subheader("➕ Buat Index")
    table_for_index = st.selectbox("Pilih Tabel", tables, key="create_index_table")
    if table_for_index:
        columns = get_columns_for_table(cassandra_session, table_for_index)
        column_for_index = st.selectbox("Pilih Kolom untuk Index", columns)
        custom_index_name = st.text_input("Nama Index (opsional)")
        if st.button("Buat Index"):
            create_index(cassandra_session, table_for_index, column_for_index, custom_index_name)

with tab2:
    st.subheader("📋 Daftar Index pada Keyspace")
    index_rows = list_indexes(cassandra_session)
    if index_rows:
        data = [{"Tabel": r.table_name, "Index": r.index_name, "Kolom": r.options.get("target"), "Tipe": r.kind} for r in index_rows]
        st.table(data)
    else:
        st.info("Belum ada index di keyspace ini.")

with tab3:
    st.subheader("🗑️ Hapus Index")
    index_rows = list_indexes(cassandra_session)
    index_names = [r.index_name for r in index_rows]
    index_to_delete = st.selectbox("Pilih Index", index_names)
    if st.button("Hapus Index"):
        drop_index(cassandra_session, index_to_delete)


st.markdown("---")

# --- Pilih Koleksi ---
col_SelecetCollection, col_CreateIndex = st.columns(2)
with col_SelecetCollection:
    st.header("📂 Pilih Koleksi MongoDB")
    all_collections = mongo_db.list_collection_names()
    selected_collection_name = st.selectbox("Pilih koleksi", all_collections, key="mongo_select_collection")
    # Ambil koleksi yang dipilih
    mongo_collection = mongo_db[selected_collection_name]

    # --- Ambil Field dari Dokumen Pertama ---
    sample_doc = mongo_collection.find_one()
    if not sample_doc:
        st.warning("Koleksi ini tidak memiliki dokumen.")
        st.stop()

    field_names = [k for k in sample_doc.keys() if k != '_id']

with col_CreateIndex:
    # --- Buat Text Index ---
    st.header("🛠️ Buat Text Index")

    text_fields = st.multiselect("Pilih kolom untuk dijadikan text index", field_names)

    use_weights = st.checkbox("Gunakan bobot (weights)?")
    weights = {}
    if use_weights:
        for field in text_fields:
            weight = st.number_input(f"Bobot untuk `{field}`", min_value=1, value=1, step=1, key=f"weight_{field}")
            weights[field] = weight

    if st.button("🚀 Buat Index"):
        if not text_fields:
            st.warning("Pilih minimal satu kolom.")
        else:
            try:
                index_spec = [(field, TEXT) for field in text_fields]
                if use_weights and weights:
                    mongo_collection.create_index(index_spec, weights=weights)
                else:
                    mongo_collection.create_index(index_spec)
                st.success(f"Text index berhasil dibuat untuk kolom: {', '.join(text_fields)}")
            except Exception as e:
                st.error(f"Gagal membuat index: {e}")

# --- Hapus Index ---
col_viewIndex , col_deleteIndex= st.columns(2)
with col_viewIndex:
    if mongo_collection is not None:
        indexes = mongo_collection.index_information()
    else:
        indexes = {}
    
    # --- Tampilkan Semua Index ---
    st.markdown("---")
    st.header("📜 Index yang Sudah Ada di Koleksi Ini")

    indexes = mongo_collection.index_information()
    if indexes:
        for index_name, details in indexes.items():
            st.markdown(f"**🔹 {index_name}**")
            st.json(details)
    else:
        st.info("Belum ada index di koleksi ini.")

with col_deleteIndex:
    st.markdown("---")
    st.header("🗑️ Hapus Index dari Koleksi")

    index_names = [name for name in indexes if name != '_id_']
    if index_names:
        selected_index_to_drop = st.selectbox("Pilih index yang ingin dihapus", index_names)
        if st.button("❌ Hapus Index"):
            try:
                mongo_collection.drop_index(selected_index_to_drop)
                st.success(f"Index `{selected_index_to_drop}` berhasil dihapus.")
            except Exception as e:
                st.error(f"Gagal menghapus index: {e}")
    else:
        st.info("Tidak ada index lain selain `_id_` untuk dihapus.")

#---- footer ----

# --- Close connections on app exit (Streamlit handles this somewhat, but explicit is good) ---
# This part is generally handled by Streamlit's caching and resource management.
# No explicit client.close() here as @st.cache_resource manages it.
