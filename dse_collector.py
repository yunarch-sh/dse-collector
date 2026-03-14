import requests
import time
import os
from pymongo import MongoClient
from datetime import datetime
import pytz

# --- 1. CONFIGURATION & DATABASE ---
# os.environ.get looks for a secret key named 'MONGO_URI' in Koyeb settings
MONGO_URI = os.environ.get("MONGO_URI")
API_URL = "https://dse-scraper.onrender.com/all_stocks"

# Safety check: if the secret is missing, stop the script and explain why
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in Environment Variables!")
    exit(1)

client = MongoClient(MONGO_URI)
db = client['DSE_Market_Data']
collection = db['price_logs']

def fetch_and_save():
    try:
        # 1. Fetch data from your Render scraper
        response = requests.get(API_URL, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # 2. Fix the Timezone
            # Servers usually use UTC; we force Dhaka time for accurate charts
            dhaka_tz = pytz.timezone('Asia/Dhaka')
            timestamp = datetime.now(dhaka_tz)
            
            # 3. Data Cleaning
            for record in data:
                record['captured_at'] = timestamp
                # Convert "31.90" (text) to 31.9 (number) so we can do math/charts
                try:
                    record['LTP*'] = float(record['LTP*'].replace(',', ''))
                except:
                    pass 

            # 4. Database Injection
            if data:
                collection.insert_many(data)
                print(f"[{timestamp.strftime('%H:%M:%S')}] Saved {len(data)} stocks.")
        else:
            print(f"⚠️ Scraper Error: HTTP {response.status_code}")

    except Exception as e:
        print(f"⚠️ Connection failed: {e}. Retrying in 60s...")

# --- 2. THE INFINITE LOOP ---
if __name__ == "__main__":
    print("🚀 DSE Collector is active...")
    
    while True:
        fetch_and_save()
        # Sleep for 60 seconds. This creates our "1-minute resolution"
        time.sleep(60)
