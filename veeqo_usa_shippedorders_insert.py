import requests
import mysql.connector
from datetime import datetime, timedelta

# MySQL database configuration
DB_CONFIG = {
    'host': '183.82.62.219',
    'user': 'dbuser',
    'password': 'A@123456',
    'database': 'stockandmanagement'
}

mydb = mysql.connector.connect(**DB_CONFIG)
mycursor = mydb.cursor()

end_date = datetime.now()
start_date = end_date - timedelta(days=60)

# Veeqo API key
api_key = 'Vqt/1d2cb2113d124f7dae997eead2a1b229'
headers = {'x-api-key': api_key}

# Base URL for Veeqo API
base_url = 'https://api.veeqo.com/orders'
params = {
    'status': 'shipped',  # Status for shipped orders
    'order_date_min': start_date.strftime('%Y-%m-%d'),
    'order_date_max': end_date.strftime('%Y-%m-%d'),
    'page': 1,
    'per_page': 100,  # Number of orders to retrieve per page
    'channel': ['Amazon', 'Amazon Sweden']  # List of channels to include
}

all_orders_data = []
error_reasons = {}

while True:
    response = requests.get(base_url, headers=headers, params=params)

    # Check if the response status is OK
    if response.status_code == 401:
        print("Error: Received status code 401 - Unauthorized. Check your API key.")
        break
    elif response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        break

    try:
        data = response.json()
    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        break

    if not data:  # If no more orders, break the loop
        break

    orders_data = []

    for order in data:
        line_items = order.get('line_items', [])  # Retrieve line items
        if line_items:  # Check if line_items is not empty
            for item in line_items:
                shipped_from = None
                allocations = order.get('allocations', [])
                if allocations:  # Check if allocations is not empty
                    for allocation in allocations:
                        name = allocation.get('warehouse', {}).get('name')
                        if name:  # Check if name is not empty
                            shipped_from = name
                            break  # Stop looping if name is found
                
                # Extract shipping address information
                ship_to = order.get('deliver_to', {})
                shipping_address = f"{ship_to.get('address1', '')} {ship_to.get('address2', '')} {ship_to.get('address3', '')}, {ship_to.get('city', '')}, {ship_to.get('state', '')} {ship_to.get('postal_code', '')}, {ship_to.get('country', '')}"

                order_data = {
                    'orderDate': datetime.strptime(order.get('created_at'), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S'),
                    'orderId': order.get('id'),
                    'orderNumber': order.get('number'),
                    'lineItemKey': item.get('id'),
                    'orderStatus': order.get('status'),
                    'name': item.get('sellable', {}).get('product_title'),
                    'sku': item.get('sellable', {}).get('sku_code'),
                    'Quantity': item.get('quantity'),
                    'state': order.get('deliver_to', {}).get('state'),
                    'shipDate': datetime.strptime(order.get('shipped_at'), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S') if order.get('shipped_at') else None,
                    'Channel': order.get('channel', {}).get('name'),
                    'shipped_from': shipped_from,
                    'Unit_price': item.get('price_per_unit'),
                    'taxAmount': order.get('tax_amount', 0),  # Set default to 0 if null
                    'shippingAmount': order.get('shipping_amount', 0),  # Set default to 0 if null
                    'shippingCost': order.get('shipping_cost', 0),  # Set default to 0 if null
                    'customer_name': order.get('customer', {}).get('full_name'),
                    'email': order.get('customer', {}).get('email'),
                    'phone': order.get('customer', {}).get('phone_number'),
                    'country': order.get('deliver_to', {}).get('country'),
                    'shipping_address': shipping_address  # Add shipping address to order_data
                }

                # Check if the order number already exists in the previously entered data table
                existing_order_sql = "SELECT COUNT(*) FROM shipped_orders WHERE orderNumber = %s AND lineItemKey = %s AND sku = %s"
                mycursor.execute(existing_order_sql, (order_data['orderNumber'], order_data['lineItemKey'], order_data['sku']))
                result = mycursor.fetchone()
                if result[0] > 0:
                    # Record the error reason
                    error_reasons[(order_data['orderNumber'], order_data['lineItemKey'], order_data['sku'])] = 'Duplicate row'
                    continue  # Skip inserting the row if the order number exists in the table

                orders_data.append(order_data)

    all_orders_data.extend(orders_data)

    if not data or len(data) < params['per_page']:
        break

    params['page'] += 1

# Batch insertion
if all_orders_data:
    try:
        keys = all_orders_data[0].keys()  # Get the keys of the first dictionary in all_orders_data
        sql = f"INSERT IGNORE INTO shipped_orders ({', '.join(keys)}) VALUES ({', '.join(['%s']*len(keys))})"
        val = [tuple(order_data.values()) for order_data in all_orders_data]
        mycursor.executemany(sql, val)
        mydb.commit()

        # Print the total number of rows inserted
        print(f"Total rows inserted: {len(all_orders_data)}")

        for order_data in all_orders_data:
            # Call the stored procedures after insertion
            mycursor.callproc('sp_shippedmskuupdate', (order_data['orderId'], order_data['orderNumber'], order_data['lineItemKey']))
            mydb.commit()

            mycursor.callproc('sp_UpdateInventoryOnShipment1', (order_data['orderId'], order_data['orderNumber'], order_data['lineItemKey']))
            mydb.commit()

    except mysql.connector.Error as err:
        # Print out the error message
        print(f"Error inserting orders: {err}")

# Close the database connection
mydb.close()
