import pandas as pd
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
df = pd.read_csv("C:/Users/hp/Desktop/Code files/Shipped orders upload.csv", encoding='ISO-8859-1')

# Create a list to keep track of rows with errors
error_rows = []

# Initialize counters
inserted_count = 0
error_count = 0

# Loop through each row of the DataFrame and insert non-duplicate items into the MySQL database
for index, row in df.iterrows():
    try:
        # Check for duplicate entries in the database
        cursor.execute("""
            SELECT COUNT(*) FROM shipped_orders 
            WHERE orderNumber = %s AND lineItemKey = %s AND SKU = %s
        """, (row['orderNumber'], row['lineItemKey'], row['SKU']))
        duplicate_count = cursor.fetchone()[0]

        if duplicate_count > 0:
            # If a duplicate entry is found, add the row to the error_rows list
            row['Error_Message'] = "Duplicate"
            error_rows.append(row)
            error_count += 1
            continue

        # Insert the data into the database
        cursor.execute("""
            INSERT INTO shipped_orders (orderDate, orderId, orderNumber, lineItemKey, shipped_from, orderStatus, SKU, name, MSKU, Quantity, Package_size, Total_quantity, state, shipDate, Channel, Unit_price, taxAmount, shippingAmount, Brand, country, shipping_address, phone, customer_name, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['orderDate'], row['orderId'], row['orderNumber'], row['lineItemKey'], row['shipped_from'], row['orderStatus'], row['SKU'], row['name'], row['MSKU'], row['Quantity'], row['Package_size'], row['Total_quantity'], row['state'], row['shipDate'], row['Channel'], row['Unit_price'], row['taxAmount'], row['shippingAmount'], row['Brand'], row['country'], row['shipping_address'], row['phone'], row['customer_name'], row['email']))
        
        mydb.commit()
        inserted_count += 1  # Increment inserted count

        # Call the stored procedure `sp_fba_inventory`
        cursor.callproc('sp_fba_inventory', (row['orderId'], row['orderNumber'], row['lineItemKey']))
        mydb.commit()

    except mysql.connector.DataError as e:
        # Handle data errors and log the row with the error message
        row['Error_Message'] = str(e)
        error_rows.append(row)
        error_count += 1

    except mysql.connector.Error as err:
        # Handle other MySQL errors and log the row with the error message
        row['Error_Message'] = str(err)
        error_rows.append(row)
        error_count += 1

# Close the database connection
mydb.close()

# Create a new DataFrame with error rows
if error_rows:
    df_errors = pd.DataFrame(error_rows)
    # Write the error rows to a new CSV file
    df_errors.to_csv("C:/Users/hp/Desktop/Code files/Errors/ShippedErrors.csv", index=False)

# Print the counts
print(f"Inserted rows: {inserted_count}")
print(f"Error rows: {error_count}")
