import requests
import sqlite3
import time

YELP_API_KEY = "HIDE"
RENCAST_API_KEY = "HIDE"

YELP_URL = "https://api.yelp.com/v3/businesses/search"
YELP_HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}

RENCAST_URL = "https://api.rentcast.io/v1/listings/rental/long-term"
RENCAST_HEADERS = {"X-Api-Key": RENCAST_API_KEY}


CITIES = {
    "Ann Arbor": {"state": "MI"},
    "Chicago": {"state": "IL"},
    "Boston": {"state": "MA"},
    "New York": {"state": "NY"},
    "Seattle": {"state": "WA"},
    "Orlando": {"state": "FL"},
    "Miami": {"state": "FL"},
    "Nashville": {"state": "TN"},
    "Detroit": {"state": "MI"}
}

VALID_CITIES = list(CITIES.keys())

# === SQLite Database Setup ===
conn = sqlite3.connect("food_rent.db")
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS Restaurants (
    id TEXT PRIMARY KEY,
    name TEXT,
    city TEXT,
    zip TEXT,
    rating REAL,
    review_count INTEGER,
    price TEXT,
    category TEXT
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Rent (
    city TEXT PRIMARY KEY,
    state TEXT,
    average_rent REAL
)
''')
conn.commit()


# 1. Yelp API: Fetch Restaurant Data and Store in SQLite

def fetch_yelp_data(city, max_calls=1):
    for offset in range(0, 25 * max_calls, 25):
        print(f"\n[+] Fetching Yelp data for {city}, offset {offset}")
        params = {
            "term": "restaurants",
            "location": city,
            "limit": 25,
            "offset": offset
        }
        resp = requests.get(YELP_URL, headers=YELP_HEADERS, params=params)

        if resp.status_code != 200:
            print(f" Yelp API error {resp.status_code}: {resp.text}")
            continue
        data = resp.json()

        for biz in data.get("businesses", []):
            try:
                rid = biz["id"]
                name = biz["name"]
                location = biz["location"]
                biz_city = location.get("city", "").strip()
                if biz_city not in VALID_CITIES:
                    continue

                zip_code = location.get("zip_code", "")
                rating = biz.get("rating", 0.0)
                review_count = biz.get("review_count", 0)
                price = biz.get("price", None)
                category = None
                if biz.get("categories"):
                    category = biz["categories"][0].get("title", None)

                cur.execute('''
                    INSERT OR IGNORE INTO Restaurants
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (rid, name, biz_city, zip_code, rating, review_count, price, category))
                conn.commit()

            except Exception as e:
                print(f" Error processing Yelp business: {e}")
        # pause to respect rate limits
        time.sleep(1)


# 2. RentCast API: Fetch Long-Term Rental Listings and Calculate Avg Rent

def fetch_rentcast_data(city, state):
    print(f"\n[+] Fetching RentCast data for {city}, {state}")
    params = {
        "city": city,
        "state": state,
        "limit": "20"
    }
    resp = requests.get(RENCAST_URL, headers=RENCAST_HEADERS, params=params)

    # print(f" RentCast response status: {resp.status_code}")
    # print(f" RentCast response text: {resp.text}")

    if resp.status_code != 200:
        print(f" RentCast API error {resp.status_code}")
        return
    
    try:
        data = resp.json()
    except Exception as e:
        print(" Error parsing JSON from RentCast:", e)
        return

    listings = data if isinstance(data, list) else []
    prices = []

    for listing in listings:
        price = listing.get("price")
        if price and isinstance(price, (int, float)):
            prices.append(price)

    if prices:
        avg_rent = round(sum(prices) / len(prices), 2)
        print(f" Average rent for {city}, {state}: ${avg_rent}")
        cur.execute('''
            INSERT OR REPLACE INTO Rent (city, state, average_rent)
            VALUES (?, ?, ?)
        ''', (city, state, avg_rent))
        conn.commit()

    else:
        print(f" No rent listings found for {city}, {state}")


def run_all():
    for city_name, info in CITIES.items():
        fetch_yelp_data(city_name, max_calls=1)
        fetch_rentcast_data(city_name, info["state"])

if __name__ == "__main__":
    run_all()
    print(" Done! Check food_rent.db for your stored data.")
