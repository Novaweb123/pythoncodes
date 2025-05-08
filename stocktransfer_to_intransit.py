import pandas as pd
import csv
import mysql.connector
import numpy as np

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
    host="183.82.62.219",
    user="dbuser",
    password="A@123456",
    database="stockandmanagement"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Read the CSV file into a pandas DataFrame with the correct encoding
df = pd.read_csv("C:/Users/hp/Desktop/Code files/tbl_intransit_stock.csv", encoding='ISO-8859-1')

# Replace NaN values with None (for SQL compatibility)
df = df.replace({np.nan: None})

# Create a list to collect valid rows and error rows
values_to_insert = []
error_rows = []

# Define the insert SQL statement
insert_sql = """
    INSERT INTO tbl_stock_intransit (
        shippeddate, trackingnumber, inventorytype, orderitemid, msku, sku,
        item_name, quantity, shippedfrom, shippedto, qtyreceived, partiallyreceived,
        damaged, physicalweight, `length`, `width`, `height`, `propweight`, `invoicenumber`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Loop through each row of the DataFrame and prepare values
for index, row in df.iterrows():
    try:
        values = (
            row['shippeddate'], row['trackingnumber'], row['inventorytype'],
            row['orderitemid'], row['msku'], row['sku'], row['item_name'],
            row['quantity'], row['shippedfrom'], row['shippedto'],
            row['qtyreceived'], row['partiallyreceived'], row['damaged'],
            row['physicalweight'], row['length'], row['width'], row['height'],
            row['propweight'], row['invoicenumber']
        )
        values_to_insert.append(values)
    except Exception as e:
        error_rows.append((row, str(e)))

# Bulk insert using executemany
inserted_rows = 0
try:
    cursor.executemany(insert_sql, values_to_insert)
    inserted_rows = len(values_to_insert)
    mydb.commit()
except Exception as e:
    print("Bulk insert failed:", e)
    # Optionally, fallback to row-by-row to find exact failures

# Close the database connection
cursor.close()
mydb.close()

# Save the error rows into a CSV
output_file = "C:/Users/hp/Desktop/Code files/Errors/Stock_transfer_error.csv"
with open(output_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(list(df.columns) + ["Error Reason"])  # Header
    for row, error in error_rows:
        writer.writerow(list(row.values) + [error])

# Print summary
print(f"Number of rows inserted: {inserted_rows}")
print(f"Number of errors: {len(error_rows)}")
