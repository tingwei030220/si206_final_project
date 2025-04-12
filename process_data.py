import sqlite3
import pandas as pd

def export_average_rent():
    """
    Connects to the food_rent.db database, calculates the average rent per city
    by joining the Rent table with the City table, and exports the results to a CSV file.
    """
    
    conn = sqlite3.connect("food_rent.db")
    

    query = """
    SELECT c.city_id, c.city_name, AVG(r.price) AS average_rent
    FROM Rent r
    JOIN City c ON r.city_id = c.city_id
    GROUP BY c.city_id, c.city_name
    """
    
    df = pd.read_sql_query(query, conn)
    
    csv_filename = "average_rent.csv"
    
    df.to_csv(csv_filename, index=False)
    print(f"Average rent per city exported to {csv_filename}")
    
    conn.close()

if __name__ == "__main__":
    export_average_rent()
