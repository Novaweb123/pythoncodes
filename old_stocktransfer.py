import pandas as pd
import csv
import mysql.connector

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
df = pd.read_csv("C:/Users/hp/Desktop/Code files/Stock transfer.csv", encoding='ISO-8859-1')

# Create a list to keep track of the rows with errors
error_rows = []

# Initialize a counter for inserted rows
inserted_rows = 0

# Loop through each row of the DataFrame and insert items into the MySQL database
for index, row in df.iterrows():
    try:
        cursor.execute("""
            INSERT INTO Stock_transfer (Date_shipped, Tracking_number, Inventory, Order_item, MSKU, SKU, Item_name, Quantity, Channel, Shipped_From, Shipped_To, Date_received, Received, Partially_received, Damaged,
            `Unit_weight`, `Length`, `Width`, `Height`, `Prop_weight`, `Invoice_number`)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (row['Date_shipped'], row['Tracking_number'], row['Inventory'], row['Order_item'], row['MSKU'], row['SKU'],  row['Item_name'], row['Quantity'], row['Channel'], row['Shipped_From'], row['Shipped_To'], row['Date_received'], row['Received'], row['Partially_received'], row['Damaged'],
     row['Unit_weight'], row['Length'], row['Width'], row['Height'], row['Prop_weight'], row['Invoice_number']))

        # Only call the stored procedure if Inventory equals 'stock'
        if row['Inventory'] == 'stock':
            cursor.callproc("sp_UpdateInventoryOnStockTransfer", (row['Tracking_number'], row['MSKU'], row['Order_item']))

        mydb.commit()
        inserted_rows += 1
    except Exception as e:
        # If an error occurs, add the row and error reason to the error_rows list
        error_rows.append((row, str(e)))
    
# Close the database connection
mydb.close()

# Create a new CSV file with the rows that had errors
output_file = "C:/Users/hp/Desktop/Code files/Errors/Stock_transfer_error.csv"
with open(output_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(df.columns + ["Error Reason"])  # Write the header with the additional column
    writer.writerows([(row.values.tolist() + [error]) for row, error in error_rows])

# Print the number of inserted rows
print(f"Number of rows inserted: {inserted_rows}")

# Print the number of errors
print(f"Number of errors: {len(error_rows)}")
