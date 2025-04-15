import requests
import sqlite3
import time
import pandas as pd

#######################################################
# 1. Database Setup – Create Tables if They Don't Exist
#######################################################

conn = sqlite3.connect("food_rent.db")
cur = conn.cursor()

# create City table: stores city name, state, and zip
cur.execute("""
CREATE TABLE IF NOT EXISTS City (
    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name TEXT UNIQUE,
    state TEXT,
    zip TEXT
)
""")

# create Price table: stores unique price levels with fixed IDs.
cur.execute("""
CREATE TABLE IF NOT EXISTS Price (
    price_id INTEGER PRIMARY KEY,
    price_level TEXT NOT NULL UNIQUE
)
""")
conn.commit()

prepopulated_prices = {1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
for pid, level in prepopulated_prices.items():
    cur.execute("INSERT OR IGNORE INTO Price (price_id, price_level) VALUES (?, ?)", (pid, level))
conn.commit()


cur.execute("""
CREATE TABLE IF NOT EXISTS Category (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_type TEXT NOT NULL UNIQUE
)
""")
conn.commit()

# create Restaurants table: no zip column here; uses foreign key to City
cur.execute("""
CREATE TABLE IF NOT EXISTS Restaurants (
    id TEXT PRIMARY KEY,
    name TEXT,
    city_id INTEGER,
    rating REAL,
    review_count INTEGER,
    price_id INTEGER,
    category_id INTEGER,
    FOREIGN KEY(city_id) REFERENCES City(city_id),
    FOREIGN KEY(price_id) REFERENCES Price(price_id),
    FOREIGN KEY(category_id) REFERENCES Category(category_id)
)
""")
conn.commit()

# create Property table: lookup for property types.
cur.execute("""
CREATE TABLE IF NOT EXISTS Property (
    property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_type TEXT NOT NULL UNIQUE
)
""")
conn.commit()

# create Rent table: stores rental listings with formatted_address, 
# bedrooms, bathrooms, price, and property_id.
cur.execute("""
CREATE TABLE IF NOT EXISTS Rent (
    listing_id TEXT PRIMARY KEY,
    city_id INTEGER,
    formatted_address TEXT,
    bedrooms INTEGER,
    bathrooms REAL,
    price REAL,
    property_id INTEGER,
    FOREIGN KEY(city_id) REFERENCES City(city_id),
    FOREIGN KEY(property_id) REFERENCES Property(property_id)
)
""")
conn.commit()


#######################################################################
# 2. Insert/Update Target Cities (Only Ann Arbor, Chicago, and Detroit)
#######################################################################

CITIES = {
    "Ann Arbor": {"state": "MI"},
    "Chicago":   {"state": "IL"},
    "Detroit":   {"state": "MI"}
}

city_mapping = {}
for city, info in CITIES.items():
    state = info["state"]
    cur.execute("INSERT OR IGNORE INTO City (city_name, state, zip) VALUES (?, ?, ?)", (city, state, ""))
    conn.commit()
    cur.execute("SELECT city_id FROM City WHERE city_name = ?", (city,))
    city_mapping[city] = cur.fetchone()[0]


#######################################
# 3. API Keys, Endpoints, and Headers
#######################################

# Yelp API – Replace with your actual Yelp API key.
YELP_API_KEY = "HIDE"
YELP_URL = "https://api.yelp.com/v3/businesses/search"
YELP_HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}

# RentCast API (using the long-term rental listings endpoint)
RENCAST_API_KEY = "HIDE"
RENCAST_URL = "https://api.rentcast.io/v1/listings/rental/long-term"
RENCAST_HEADERS = {"X-Api-Key": RENCAST_API_KEY}


#############################################
# 4. Lookup Helper Functions
#############################################

def get_price_id(price_level):
    if not price_level:
        return None
    cur.execute("SELECT price_id FROM Price WHERE price_level = ?", (price_level,))
    res = cur.fetchone()
    if res:
        return res[0]
    else:
        cur.execute("INSERT INTO Price (price_level) VALUES (?)", (price_level,))
        conn.commit()
        return cur.lastrowid

def get_category_id(category_type):
    if not category_type:
        return None
    cur.execute("SELECT category_id FROM Category WHERE category_type = ?", (category_type,))
    res = cur.fetchone()
    if res:
        return res[0]
    else:
        cur.execute("INSERT INTO Category (category_type) VALUES (?)", (category_type,))
        conn.commit()
        return cur.lastrowid

def get_property_id(property_type):
    if not property_type:
        return None
    cur.execute("SELECT property_id FROM Property WHERE property_type = ?", (property_type,))
    res = cur.fetchone()
    if res:
        return res[0]
    else:
        cur.execute("INSERT INTO Property (property_type) VALUES (?)", (property_type,))
        conn.commit()
        return cur.lastrowid

#################################################################
# 5. Data Collection Functions (8 items per run using Offset)
#################################################################

# (A) Yelp Data Collection – Add 8 new restaurants for the given city.
def fetch_yelp_data_for_city(city_name):
    """
    Retrieve 8 new Yelp restaurant records for a given city 
    based on the current number of rows in Restaurants (using city_id).
    """
    city_id = city_mapping[city_name]
    state = CITIES[city_name]["state"]
    cur.execute("SELECT COUNT(*) FROM Restaurants WHERE city_id = ?", (city_id,))
    current_count = cur.fetchone()[0]
    offset = current_count  # each run adds the next 8 records
    print(f"[Yelp] {city_name}: Current count = {current_count}. Fetching next 8 records (offset = {offset}).")
    
    params = {
        "term": "restaurants",
        "location": f"{city_name}, {state}",
        "limit": 8,
        "offset": offset,
        "radius": 5000
    }
    response = requests.get(YELP_URL, headers=YELP_HEADERS, params=params)
    if response.status_code != 200:
        print(f"Yelp API error ({response.status_code}): {response.text}")
        return
    data = response.json()

    for biz in data.get("businesses", []):
        try:
            rid = biz["id"]
            name = biz["name"]
            location = biz.get("location", {})
            biz_city = location.get("city", "").strip()
            if biz_city.lower() != city_name.lower():
                continue
            zip_code = location.get("zip_code", "")

            cur.execute("SELECT zip FROM City WHERE city_id = ?", (city_id,))
            existing_zip = cur.fetchone()[0]
            if (not existing_zip) and zip_code:
                cur.execute("UPDATE City SET zip = ? WHERE city_id = ?", (zip_code, city_id))
                conn.commit()
            rating = biz.get("rating", 0.0)
            review_count = biz.get("review_count", 0)
            price_level = biz.get("price", None)
            category_type = None

            if biz.get("categories"):
                category_type = biz["categories"][0].get("title", None)
            price_id = get_price_id(price_level)
            category_id = get_category_id(category_type)

            cur.execute("""
                INSERT OR IGNORE INTO Restaurants 
                (id, name, city_id, rating, review_count, price_id, category_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (rid, name, city_id, rating, review_count, price_id, category_id))

        except Exception as e:
            print("Error processing Yelp business:", e)

    conn.commit()
    time.sleep(1)

# (B) RentCast Data Collection – Add 8 new rental listings for a given city.
def fetch_rentcast_data_for_city(city_name):
    """
    Retrieve 8 new rental listings from RentCast for the given city based on
    the current count in the Rent table (using city_id).
    """
    city_id = city_mapping[city_name]
    state = CITIES[city_name]["state"]
    cur.execute("SELECT COUNT(*) FROM Rent WHERE city_id = ?", (city_id,))
    current_count = cur.fetchone()[0]
    offset = current_count
    print(f"[RentCast] {city_name}: Current Rent count = {current_count}. Fetching next 8 records (offset = {offset}).")
    
    params = {
        "city": city_name,
        "state": state,
        "limit": 8,
        "offset": offset
    }

    response = requests.get(RENCAST_URL, headers=RENCAST_HEADERS, params=params)
    if response.status_code != 200:
        print(f"RentCast API error ({response.status_code}): {response.text}")
        return
    try:
        data = response.json()
    except Exception as e:
        print("Error parsing RentCast JSON:", e)
        return
    
    listings = data if isinstance(data, list) else data.get("listings", [])
    for listing in listings:
        try:
            listing_id = listing.get("id")
            formatted_address = listing.get("formattedAddress", "")
            bedrooms = listing.get("bedrooms", None)
            bathrooms = listing.get("bathrooms", None)
            price_value = listing.get("price", None)
            property_type = listing.get("propertyType", "")
            property_id = get_property_id(property_type)

            cur.execute("""
                INSERT OR IGNORE INTO Rent 
                (listing_id, city_id, formatted_address, bedrooms, bathrooms, price, property_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (listing_id, city_id, formatted_address, bedrooms, bathrooms, price_value, property_id))

        except Exception as e:
            print("Error processing RentCast listing:", e)
    conn.commit()
    time.sleep(1)


#################################################################
# 6. Main Routine – Add 8 New Data Points per Run for Each City
#################################################################

def run_all():
    
    for city in CITIES.keys():
        print(f"\nProcessing city: {city}")
        fetch_yelp_data_for_city(city)
        fetch_rentcast_data_for_city(city)

    print("Data gathering complete.")

############################################################
# 7. Run Everything and Close Connection (No Table Dropping)
############################################################
if __name__ == "__main__":
    run_all()
    conn.close()
    print("Done!")