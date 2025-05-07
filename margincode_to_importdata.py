import mysql.connector
import csv

# MySQL database configuration
DB_CONFIG = {
    'host': '183.82.62.219',
    'user': 'dbuser',
    'password': 'A@123456',
    'database': 'stockandmanagement'
}

# CSV file path for logging errors
ERRORS_CSV_PATH = r"C:\\Users\\hp\\Desktop\\Code files\\Errors\\ShippedErrors.csv"

# Connect to the database
try:
    mydb = mysql.connector.connect(**DB_CONFIG)
    mycursor = mydb.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit()

# Define error reasons list for logging
error_reasons = []

# SQL query to get data from shipped_orders
shipped_query = """
    SELECT orderDate, orderId, orderNumber, lineItemKey, shipped_from, orderStatus, SKU, MSKU, name, Brand,
           Quantity, Package_size, Total_quantity, Channel, Unit_price, taxAmount, shippingAmount, shipDate, delivery_region, state, country, customer_name
    FROM shipped_orders
    WHERE orderDate >= '2025-01-01'
      AND orderNumber NOT IN (SELECT orderNumber FROM tbl_ordermanagement) and orderstatus != 'Cancelled'
"""

# SQL query to get data from unshipped_orders
unshipped_query = """
    SELECT orderDate, orderId, orderNumber, lineItemKey, orderStatus, SKU, MSKU, name, Brand,
           Quantity, Package_size, Total_quantity, Channel, Unit_price, taxAmount, shippingAmount, delivery_region, state, country, customer_name
    FROM Unshipped_orders
    WHERE orderDate >= '2025-01-01'
      AND orderNumber NOT IN (SELECT orderNumber FROM tbl_ordermanagement)
"""

# Fetch data from both tables
try:
    mycursor.execute(shipped_query)
    shipped_orders = mycursor.fetchall()

    mycursor.execute(unshipped_query)
    unshipped_orders = mycursor.fetchall()

    # Add missing fields to unshipped_orders with default values
    for order in unshipped_orders:
        order['shipped_from'] = None
        order['shipDate'] = None

    # Combine both datasets
    all_orders_data = shipped_orders + unshipped_orders
except mysql.connector.Error as err:
    print(f"Error fetching data: {err}")
    mydb.close()
    exit()

# Insert into tbl_ordermanagement and process orders
rows_inserted = 0
for order_data in all_orders_data:
    try:
        # Insert the data into tbl_ordermanagement
        insert_query = """
            INSERT IGNORE INTO tbl_ordermanagement (orderdate, orderid, ordernumber, lineitemkey, shipped_from, orderstatus,
                                                     sku, msku, name, brand, category, quantity, pack_size, channel,
                                                     unit_price, taxamount, shippingamount, shipdate, state, country, customer_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            order_data.get('orderDate', None), order_data.get('orderId', None), order_data.get('orderNumber', None),
            order_data.get('lineItemKey', None), order_data.get('shipped_from', None), order_data.get('orderStatus', None),
            order_data.get('SKU', None), order_data.get('MSKU', None), order_data.get('name', None),
            order_data.get('Brand', None), order_data.get('category', None), order_data.get('Quantity', None),
            order_data.get('Package_size', None), order_data.get('Channel', None), order_data.get('Unit_price', None),
            order_data.get('taxAmount', None), order_data.get('shippingAmount', None),
            order_data.get('shipDate', None), order_data.get('state', None), order_data.get('country', None),
            order_data.get('customer_name', None)
        )
        mycursor.execute(insert_query, values)
        mydb.commit()

        if mycursor.rowcount > 0:
            rows_inserted += 1
            mycursor.callproc('p_updatesshipcost', (order_data['orderNumber'], order_data['lineItemKey'], order_data['SKU']))
            mydb.commit()

    except mysql.connector.Error as err:
        error_reasons.append({
            'orderNumber': order_data.get('orderNumber', 'Unknown'),
            'reason': str(err)
        })

# Write errors to a CSV file
if error_reasons:
    with open(ERRORS_CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['orderNumber', 'reason']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for error in error_reasons:
            writer.writerow(error)

# Close the database connection
mydb.close()

# Print summary
print(f"Rows inserted: {rows_inserted}")
if error_reasons:
    print(f"Errors encountered and logged to {ERRORS_CSV_PATH}")
