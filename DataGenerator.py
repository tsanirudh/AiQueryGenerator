import pandas as pd
import numpy as np
from faker import Faker
import random
import time
import mysql.connector

fake = Faker()

num_transactions = 100000
num_products = 50
regions = ['North America', 'Europe', 'Asia', 'South America', 'Africa']
states = {
    'New York': (40.7128, -74.0060),
    'California': (36.7783, -119.4179),
    'Texas': (31.9686, -99.9018),
    'Florida': (27.994402, -81.760254),
    'Ohio': (40.4173, -82.9071),
    'North Carolina': (35.7596, -79.0193),
    'Michigan': (44.3148, -85.6024),
    'Washington': (47.7511, -120.7401),
    'Arizona': (34.0489, -111.0937),
    'Georgia': (32.1656, -82.9001),
    'Tennessee': (35.5175, -86.5804),
    'Indiana': (40.2672, -86.1349),
    'Massachusetts': (42.4072, -71.3824),
    'Missouri': (37.9643, -91.8318),
    'Maryland': (39.0458, -76.6413),
    'Wisconsin': (44.5000, -89.5000),
    'Colorado': (39.5501, -105.7821),
    'Minnesota': (46.7296, -94.6859),
    'South Carolina': (33.8361, -81.1637),
    'Alabama': (32.3182, -86.9023),
    'Louisiana': (30.9843, -91.9623),
    'Kentucky': (37.8393, -84.2700),
    'Oregon': (43.8041, -120.5542),
    'Oklahoma': (35.4676, -97.5164),
    'Connecticut': (41.6032, -73.0877),
    'Iowa': (41.8780, -93.0977),
    'Mississippi': (32.3547, -89.3985),
    'Arkansas': (34.7465, -92.2896),
    'Utah': (39.3200, -111.0937),
    'Nevada': (38.8026, -116.4194),
    'Kansas': (39.0119, -98.4842),
    'New Mexico': (34.5199, -105.8701),
    'Nebraska': (41.4925, -99.9018),
    'West Virginia': (38.5976, -80.4549),
    'Idaho': (44.0682, -114.7420),
    'Maine': (45.2538, -69.4455),
    'New Hampshire': (43.1939, -71.5724),
    'Montana': (46.8797, -110.3626),
    'Rhode Island': (41.5801, -71.4774),
    'Delaware': (38.9108, -75.5277),
    'South Dakota': (43.9695, -99.9018),
    'North Dakota': (47.5515, -101.0020),
    'Vermont': (44.5588, -72.5778),
    'Wyoming': (43.0759, -107.2903)
}

date_range = pd.date_range(start='1/1/2015', end='12/31/2024', freq='D')
start_date = date_range[0]
last_used_date = start_date
product_ids = [f'P{str(i).zfill(4)}' for i in range(1, num_products + 1)]
products = [{'Product ID': pid, 'Product Name': fake.word(), 'Category': fake.word()}
            for pid in product_ids]

transactions = []
id = 0
for _ in range(num_transactions):
    id += 1
    transaction_id = fake.uuid4()
    timestamp = last_used_date + \
        pd.DateOffset(seconds=random.randint(0, 86400))
    last_used_date = timestamp
    product = random.choice(products)
    product_id = product['Product ID']

    # Simulate changing quantity based on time period
    if timestamp < start_date + pd.DateOffset(days=30):
        quantity = random.randint(1, 5)
    else:
        quantity = random.randint(5, 10)

    unit_price = round(random.uniform(5.0, 100.0), 2)
    total_amount = round(quantity * unit_price, 2)
    state = random.choice(list(states.keys()))
    lat, lon = states[state]

    transactions.append({
        'ID': id,
        'Transaction ID': transaction_id,
        'Timestamp': timestamp,
        'Product ID': product_id,
        'Quantity': quantity,
        'Unit Price': unit_price,
        'Total Amount': total_amount,
        'State': state,
        'Latitude': lat,
        'Longitude': lon
    })

# Convert to DataFrame
transactions_df = pd.DataFrame(transactions)
products_df = pd.DataFrame(products)

# Calculate Revenue Data
transactions_df['Date'] = transactions_df['Timestamp'].dt.date
revenue_df = transactions_df.groupby('Date').agg(
    {'Total Amount': 'sum'}).cumsum().reset_index()
revenue_df.columns = ['Timestamp', 'Total Revenue']

# Connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="user",
    password="userpassword",
    database="my_database"
)

mycursor = mydb.cursor()
tableName = "transactions"

# Check if table exists
mycursor.execute(f"SHOW TABLES LIKE '{tableName}'")
result = mycursor.fetchone()  # ✅ Fetch the result to avoid unread results error

if result:
    print(f"Table {tableName} exists")

    # ✅ Check if the Latitude and Longitude columns exist
    mycursor.execute("SHOW COLUMNS FROM transactions LIKE 'Latitude'")
    lat_exists = mycursor.fetchone()

    mycursor.execute("SHOW COLUMNS FROM transactions LIKE 'Longitude'")
    lon_exists = mycursor.fetchone()

    if not lat_exists or not lon_exists:
        mycursor.execute(
            "ALTER TABLE transactions ADD COLUMN Latitude FLOAT, ADD COLUMN Longitude FLOAT")
        mydb.commit()
        print("Added missing Latitude and Longitude columns.")

else:
    # Ensure a clean start
    mycursor.execute("DROP TABLE IF EXISTS transactions")
    mycursor.execute(
        "CREATE TABLE transactions ("
        "ID INT AUTO_INCREMENT PRIMARY KEY, "
        "Timestamp DATETIME, "
        "ProductID VARCHAR(255), "
        "Quantity INT, "
        "UnitPrice FLOAT, "
        "TotalAmount FLOAT, "
        "State VARCHAR(255), "
        "Latitude FLOAT, "
        "Longitude FLOAT)"
    )
    mydb.commit()
    print(f"Table {tableName} created.")

# Insert data into the table batch size
batch_size = 2
batch_count = len(transactions_df) // batch_size

for i in range(batch_count):
    start_index = i * batch_size
    end_index = (i + 1) * batch_size
    batch_df = transactions_df.iloc[start_index:end_index]

    sql = f"INSERT INTO {tableName} (Timestamp, ProductID, Quantity, UnitPrice, TotalAmount, State, Latitude, Longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    values = []
    for _, row in batch_df.iterrows():
        timestamp = row['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        val = (timestamp, row['Product ID'], row['Quantity'],
               row['Unit Price'], row['Total Amount'], row['State'], row['Latitude'], row['Longitude'])
        values.append(val)

    mycursor.executemany(sql, values)
    mydb.commit()
    print(mycursor.rowcount, "records inserted.")
    time.sleep(2)

print("Data insertion complete.")
