import requests
import base64
import mysql.connector

# Retrieve data from ShipStation API
api_key = 'a19c6d42b7c4441195facabb7e302842'
api_secret = '5466ba9b96db4622a7037da636c5c29b'
base_url = 'https://ssapi.shipstation.com/orders'
page_size = 500  # Number of orders to retrieve per page

api_key_secret = f"{api_key}:{api_secret}"
headers = {'Authorization': f'Basic {base64.b64encode(api_key_secret.encode()).decode()}'}

orders_data = []
page = 1
total_pages = 1

while page <= total_pages:
    url = f'{base_url}?orderStatus=awaiting_shipment&page={page}&pageSize={page_size}'
    response = requests.get(url, headers=headers)
    data = response.json()

    if 'orders' in data:
        orders = data['orders']
        for order in orders:
            for item in order.get('items', []):
                order_data = {
                    'orderId': order.get('orderId'),
                    'orderNumber': order.get('orderNumber')[:50],
                    'lineItemKey': item.get('lineItemKey'),
                    'orderDate': order.get('orderDate'),
                    'orderStatus': order.get('orderStatus'),
                    'Quantity': item.get('quantity'),
                    'Unit_price': item.get('unitPrice'),
                    'taxAmount': order.get('taxAmount'),
                    'shippingAmount': order.get('shippingAmount'),
                    'name': item.get('name'),
                    'SKU': item.get('sku'),
                    'state': order.get('shipTo', {}).get('state'),
                    'country': order.get('shipTo', {}).get('country'),
                    'Channel': order.get('advancedOptions', {}).get('storeId'),
                    'customer_name': order.get('billTo', {}).get('name')
                }
                orders_data.append(order_data)

    total_pages = data.get('pages', 1)
    page += 1

if not orders_data:
    print("No orders found.")
    exit()

# Connect to MySQL database with autocommit mode
try:
    mydb = mysql.connector.connect(
        host="183.82.62.219",
        user="dbuser",
        password="A@123456",
        database="stockandmanagement",
        autocommit=True  # Enable autocommit to avoid long transactions
    )
    print("Connected to MySQL database")
except mysql.connector.Error as err:
    print(f"MySQL Connection Error: {err}")
    exit(1)  # Exit the program if there's a connection error

mycursor = mydb.cursor()

# Delete all records from Unshipped_orders table before inserting new data
try:
    mycursor.execute("DELETE FROM lcwh_unshipped_orders WHERE 1=1")
    print("Existing records deleted from lcwh_unshipped_orders table")
except mysql.connector.Error as err:
    print(f"MySQL Deletion Error: {err}")

# SQL query to select records with matching orderId, orderNumber, and lineItemKey
select_sql = "SELECT 1 FROM lcwh_unshipped_orders WHERE orderId = %s AND orderNumber = %s AND lineItemKey = %s LIMIT 1"

# SQL query to call the Sr_UpdateMSKUForUnshippedOrders stored procedure
call_procedure_sql = "CALL Sr_lcwh_updateMSKU_ForUnshippedOrders(%s, %s, %s)"

# SQL query to insert data into the Unshipped_orders table
sql = """
INSERT INTO lcwh_unshipped_orders (orderId, orderNumber, lineItemKey, orderDate, orderStatus, Quantity, Unit_price, taxAmount, shippingAmount, name, SKU, state, country, Channel, customer_name) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for order in orders_data:
    # Execute the select query to check for existing records
    mycursor.execute(select_sql, (order['orderId'], order['orderNumber'], order['SKU']))
    result = mycursor.fetchone()

    if result:
        pass  # Skipping duplicate record
    else:
        try:
            # Insert the data into the Unshipped_orders table
            mycursor.execute(sql, (
                order['orderId'], order['orderNumber'], order['lineItemKey'], order['orderDate'], 
                order['orderStatus'], order['Quantity'], order['Unit_price'], order['taxAmount'], 
                order['shippingAmount'], order['name'], order['SKU'], order['state'], order['country'], 
                order['Channel'], order['customer_name']
            ))

            # Call the Sr_UpdateMSKUForUnshippedOrders procedure with orderId, orderNumber and lineItemKey
            mycursor.execute(call_procedure_sql, (order['orderId'], order['orderNumber'], order['SKU']))
        except mysql.connector.Error as err:
            print(f"MySQL Insert Error: {err}")

# Commit the changes
mydb.commit()

# Close the database connection
mycursor.close()
mydb.close()
