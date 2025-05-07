import requests
import base64
import mysql.connector
from datetime import datetime, timedelta

# MySQL database configuration
DB_CONFIG = {
    'host': '183.82.62.219',
    'user': 'dbuser',
    'password': 'A@123456',
    'database': 'stockandmanagement'
}

# ShipStation API configuration
SHIPSTATION_CONFIG = {
    'api_key': 'a19c6d42b7c4441195facabb7e302842',
    'api_secret': '5466ba9b96db4622a7037da636c5c29b'
}

# Establish database connection
mydb = mysql.connector.connect(**DB_CONFIG)
mycursor = mydb.cursor()

# Define the date range
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

# Set the desired page size
page_size = 500

# Construct the URL for ShipStation API
url = f'https://ssapi.shipstation.com/orders?orderDateStart={start_date.strftime("%Y-%m-%dT00:00:00")}&orderDateEnd={end_date.strftime("%Y-%m-%dT23:59:59")}&orderStatus=shipped&pageSize={page_size}'

# Encode the API key and secret for authorization
api_key_secret = f"{SHIPSTATION_CONFIG['api_key']}:{SHIPSTATION_CONFIG['api_secret']}"
headers = {'Authorization': f'Basic {base64.b64encode(api_key_secret.encode()).decode()}'}

# Initialize variables
all_orders_data = []
next_page = 1

# Define error reasons dictionary
error_reasons = {}

# Fetch orders from ShipStation API
while True:
    response = requests.get(url + f'&page={next_page}', headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching data from ShipStation API: {response.status_code}")
        break
    
    data = response.json()
    
    orders_data = []
    for order in data.get('orders', []):
        for item in order.get('items', []):
            order_data = {
                'orderDate': order.get('orderDate'),
                'orderId': order.get('orderId'),
                'orderNumber': order.get('orderNumber')[:50],
                'lineItemKey': '1',  # Default value is '1'
                'orderStatus': order.get('orderStatus'),
                'name': item.get('name'),
                'sku': item.get('sku'),
                'Quantity': item.get('quantity') if item.get('quantity') is not None else 0,  # Set to 0 if Quantity is None
                'state': order.get('shipTo', {}).get('state'),
                'shipDate': order.get('shipDate'),
                'Channel': order.get('advancedOptions', {}).get('storeId'),
                'shipped_from': order.get('advancedOptions', {}).get('warehouseId'),
                'Unit_price': item.get('unitPrice'),
                'taxAmount': order.get('taxAmount'),
                'shippingAmount': order.get('shippingAmount'),
                'country': order.get('shipTo', {}).get('country'),
                'customer_name': order.get('billTo', {}).get('name'),
                'email': order.get('customerEmail'),
                'phone': order.get('billTo', {}).get('phone'),
                'Package_size': 1,  # Default value is '1'
                'MSKU': item.get('sku'),  # Same as SKU
                'Total_quantity': item.get('quantity') if item.get('quantity') is not None else 0,  # Set to 0 if Quantity is None
                'delivery_region': None  # To be updated from database
            }

            # Skip rows with empty 'name' or 'sku'
            if not order_data['name'].strip() or not order_data['sku'].strip():
                continue

            orders_data.append(order_data)

    all_orders_data.extend(orders_data)

    if not data.get('pages') or next_page >= data.get('pages'):
        break

    next_page += 1

# Update delivery_region field
for order_data in all_orders_data:
    state = order_data['state']
    mycursor.execute("SELECT Zone FROM tbl_zone_store_mapping WHERE State_code = %s", (state,))
    result = mycursor.fetchone()
    if result:
        order_data['delivery_region'] = result[0]

# Insert orders into the database
if all_orders_data:
    rows_inserted = 0
    for order_data in all_orders_data:
        try:
            sql = ("INSERT IGNORE INTO lcwh_shipped_orders "
                   "(orderDate, orderId, orderNumber, lineItemKey, orderStatus, name, sku, Quantity, state, shipDate, Channel, shipped_from, Unit_price, taxAmount, shippingAmount, country, customer_name, email, phone, Package_size, MSKU, Total_quantity, delivery_region) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            val = tuple(order_data.values())
            mycursor.execute(sql, val)
            mydb.commit()

            # Check if the row was actually inserted
            if mycursor.rowcount > 0:
                rows_inserted += 1

        except mysql.connector.Error as err:
            # Record the error reason
            error_reasons[order_data['orderNumber']] = str(err)
            mydb.rollback()

# Close the database connection
mydb.close()

# Output the results
print(f"Inserted {rows_inserted} rows into the database.")
if error_reasons:
    print("Errors encountered during processing:")
    for key, reason in error_reasons.items():
        print(f"Order {key}: {reason}")
