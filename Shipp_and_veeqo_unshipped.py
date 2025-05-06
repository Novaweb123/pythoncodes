import requests
import base64
import mysql.connector

# Function to convert datetime string from Veeqo format to MySQL format
def convert_datetime(veeqo_datetime):
    # Simply remove 'Z' at the end
    return veeqo_datetime[:-1]

# Retrieve data from ShipStation API
shipstation_api_key = 'fea4ddec8cba492db7a7914230eb697d'
shipstation_api_secret = 'dc7e476c71e04a3585431227204374a0'
shipstation_base_url = 'https://ssapi.shipstation.com/orders'
page_size = 500  # Number of orders to retrieve per page

shipstation_api_key_secret = f"{shipstation_api_key}:{shipstation_api_secret}"
headers = {'Authorization': f'Basic {base64.b64encode(shipstation_api_key_secret.encode()).decode()}'}

shipstation_orders_data = []
page = 1
total_pages = 1

while page <= total_pages:
    url = f'{shipstation_base_url}?orderStatus=awaiting_shipment&page={page}&pageSize={page_size}'
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
                shipstation_orders_data.append(order_data)

    total_pages = data.get('pages', 1)
    page += 1

# Retrieve data from Veeqo API
veeqo_api_keys = ['Vqt/1d2cb2113d124f7dae997eead2a1b229', 'Vqt/0a6bbd57d30e7631d59dbf27912b8495']
veeqo_base_url = 'https://api.veeqo.com/orders'
veeqo_orders_data = []

for api_key in veeqo_api_keys:
    headers = {'x-api-key': api_key}
    params = {
        'status': 'awaiting_fulfillment',  # Status for unshipped orders
        'page': 1,
        'per_page': 100  # Number of orders to retrieve per page
    }

    while True:
        response = requests.get(veeqo_base_url, headers=headers, params=params)
        data = response.json()

        if not data:  # If no more orders, break the loop
            break

        for order in data:
            for item in order.get('line_items', []):
                order_data = {
                    'orderId': order.get('id'),
                    'orderNumber': order.get('number')[:50],
                    'lineItemKey': item.get('id'),
                    'orderDate': convert_datetime(order.get('created_at')),
                    'orderStatus': order.get('status'),
                    'Quantity': item.get('quantity'),
                    'Unit_price': item.get('price_per_unit'),
                    'taxAmount': item.get('taxAmount', 0),
                    'shippingAmount': item.get('shippingAmount', 0),
                    'name': item.get('sellable', {}).get('product_title'),
                    'SKU': item.get('sellable', {}).get('sku_code'),
                    'state': order.get('deliver_to', {}).get('state'),
                    'country': order.get('deliver_to', {}).get('country'),
                    'Channel': order.get('channel', {}).get('name'),
                    'customer_name': order.get('customer', {}).get('full_name')
                }
                veeqo_orders_data.append(order_data)

        params['page'] += 1  # Go to the next page

# Combine ShipStation and Veeqo orders data
orders_data = shipstation_orders_data + veeqo_orders_data

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
    mycursor.execute("DELETE FROM Unshipped_orders WHERE 1=1")
    print("Existing records deleted from Unshipped_orders table")
except mysql.connector.Error as err:
    print(f"MySQL Deletion Error: {err}")

# SQL query to select records with matching orderId, orderNumber, and lineItemKey
select_sql = "SELECT 1 FROM Unshipped_orders WHERE orderId = %s AND orderNumber = %s AND lineItemKey = %s LIMIT 1"

# SQL query to call the Sr_UpdateMSKUForUnshippedOrders stored procedure
call_procedure_sql = "CALL Sr_UpdateMSKUForUnshippedOrders2(%s, %s, %s)"

# SQL query to insert data into the Unshipped_orders table
sql = """
INSERT INTO Unshipped_orders (orderId, orderNumber, lineItemKey, orderDate, orderStatus, Quantity, Unit_price, taxAmount, shippingAmount, name, SKU, state, country, Channel, customer_name) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for order in orders_data:
    # Execute the select query to check for existing records
    mycursor.execute(select_sql, (order['orderId'], order['orderNumber'], order['lineItemKey']))
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
            mycursor.execute(call_procedure_sql, (order['orderId'], order['orderNumber'], order['lineItemKey']))
        except mysql.connector.Error as err:
            print(f"MySQL Insert Error: {err}")

# Commit the changes
mydb.commit()

# Close the database connection
mycursor.close()
mydb.close()
