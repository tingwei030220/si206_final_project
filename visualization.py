import sqlite3
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

mpl.rcParams['axes.formatter.use_mathtext'] = False
sns.set(style="whitegrid")

conn = sqlite3.connect("food_rent.db")

# ------------------------------------------
# Data for Scatter Plot & Twin Bar/Line Chart:
# (Average restaurant rating, restaurant count, and average rent per city)
# ------------------------------------------
restaurants_query = """
SELECT city, AVG(rating) AS avg_rating, COUNT(*) AS restaurant_count
FROM Restaurants
GROUP BY city
"""
restaurants_df = pd.read_sql_query(restaurants_query, conn)

rent_query = """
SELECT city, average_rent
FROM Rent
"""
rent_df = pd.read_sql_query(rent_query, conn)

merged_df = pd.merge(restaurants_df, rent_df, on="city", how="inner")
print("Merged DataFrame:")
print(merged_df)

# ------------------------------------------
# Visualization 1: Scatter Plot
# ------------------------------------------
plt.figure(figsize=(8, 6))
sns.scatterplot(data=merged_df, x="avg_rating", y="average_rent", hue="city", s=150)
plt.title("Average Restaurant Rating vs. Average Rent by City")
plt.xlabel("Average Restaurant Rating")
plt.ylabel("Average Rent ($)")
plt.tight_layout()
plt.savefig("scatter_plot.png")
plt.show()

# ------------------------------------------
# Visualization 2: Bar Chart with Twin Y-Axis
# (Restaurant count as bars, Average rent as line)
# ------------------------------------------
fig, ax1 = plt.subplots(figsize=(8, 6))
color1 = 'tab:blue'
ax1.set_xlabel("City")
ax1.set_ylabel("Number of Restaurants", color=color1)
bars = ax1.bar(merged_df["city"], merged_df["restaurant_count"], color=color1, alpha=0.7)
ax1.tick_params(axis='y', labelcolor=color1)

ax2 = ax1.twinx()
color2 = 'tab:red'
ax2.set_ylabel("Average Rent ($)", color=color2)
ax2.plot(merged_df["city"], merged_df["average_rent"], color=color2, marker="o", linewidth=2)
ax2.tick_params(axis='y', labelcolor=color2)

plt.title("Restaurant Count and Average Rent by City")
fig.tight_layout()
plt.savefig("bar_chart.png")
plt.show()

# ------------------------------------------
# Visualization 3: Bar Charts for Top Cuisines in Each City
# (Top 5 cuisines per city)
# ------------------------------------------
cuisine_query = """
SELECT city, category, COUNT(*) AS count
FROM Restaurants
WHERE category IS NOT NULL
GROUP BY city, category
ORDER BY city, count DESC
"""
cuisine_df = pd.read_sql_query(cuisine_query, conn)
print("Cuisine Data:")
print(cuisine_df)

# For each valid city, plot the top 5 cuisines.
cities = cuisine_df['city'].unique()
for city in cities:
    city_df = cuisine_df[cuisine_df['city'] == city].sort_values(by="count", ascending=False).head(5)
    # cuisine_df['city'].replace({'Nashvilli': 'Nashville'}, inplace=True)
    
    plt.figure(figsize=(8, 6))
    sns.barplot(x="category", y="count", data=city_df, palette="viridis")
    plt.title(f"Top 5 Cuisines in {city}")
    plt.xlabel("Cuisine Type")
    plt.ylabel("Number of Restaurants")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    filename = f"top_cuisines_{city.replace(' ', '_')}.png"
    plt.savefig(filename)
    plt.show()

# ------------------------------------------
# Visualization 4: Pie Chart for Distribution of Restaurant Price Levels
# ------------------------------------------
price_query = """
SELECT price, COUNT(*) AS count
FROM Restaurants
WHERE price IS NOT NULL
GROUP BY price
ORDER BY count DESC
"""
price_df = pd.read_sql_query(price_query, conn)
print("Price Level Distribution:")
print(price_df)

price_labels = [label.replace("$", r"\$") for label in price_df["price"]]

plt.figure(figsize=(8, 8))
plt.pie(price_df["count"], labels=price_labels, autopct="%1.1f%%", startangle=140, 
        colors=sns.color_palette("pastel"), textprops={'fontsize': 12})
plt.title("Distribution of Restaurant Price Levels")
plt.tight_layout()
plt.savefig("price_distribution_pie.png")
plt.show()

conn.close()
