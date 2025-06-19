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
    'api_key': '33b06747ac8b4939b8f879c59a914c1b',
    'api_secret': '5bb743e4ab4d4c92b955beb1a1cf0b82'
}

mydb = mysql.connector.connect(**DB_CONFIG)
mycursor = mydb.cursor()

# Calculate the date range for the last 45 days
end_date = datetime.now()
start_date = end_date - timedelta(days=10)

# Set the desired page size
page_size = 500

url = f'https://ssapi.shipstation.com/shipments?shipDateStart={start_date.strftime("%Y-%m-%dT00:00:00")}&shipDateEnd={end_date.strftime("%Y-%m-%dT23:59:59")}&orderStatus=shipped&pageSize={page_size}'

api_key_secret = f"{SHIPSTATION_CONFIG['api_key']}:{SHIPSTATION_CONFIG['api_secret']}"
headers = {'Authorization': f'Basic {base64.b64encode(api_key_secret.encode()).decode()}'}

all_orders_data = []
next_page = 1

while True:
    response = requests.get(url + f'&page={next_page}', headers=headers)
    data = response.json()

    orders_data = []
    for shipment in data.get('shipments', []):
        ship_to = shipment.get('shipTo', {})

        # Extract full shipping address details
        shipping_address = f"{ship_to.get('street1', '')} {ship_to.get('street2', '')} {ship_to.get('street3', '')}, {ship_to.get('city', '')}, {ship_to.get('state', '')} {ship_to.get('postalCode', '')}, {ship_to.get('country', '')}"

        order_data = {
            'shipmentid': shipment.get('shipmentId'),
            'orderid': shipment.get('orderId'),
            'ordernumber': shipment.get('orderNumber')[:50],
            'shipdate': shipment.get('shipDate'),
            'shipmentcost': shipment.get('shipmentCost'),
            'trackingnumber': shipment.get('trackingNumber'),
            'servicecode': shipment.get('serviceCode'),
            'warehouseid': shipment.get('warehouseId'),
            'storeid': shipment.get('advancedOptions', {}).get('storeId'),
            'customername': ship_to.get('name'),
            'street1': ship_to.get('street1'),
            'city': ship_to.get('city'),
            'state': ship_to.get('state'),
            'postalcode': ship_to.get('postalCode'),
            'country': ship_to.get('country'),
            'phone': ship_to.get('phone'),
            'totalamount': shipment.get('amountPaid', 0),
            'taxamount': shipment.get('taxAmount', 0),
            'shippingamount': shipment.get('shippingAmount', 0),
            'deliverystatus': shipment.get('deliveryStatus'),
            'shipping_address': shipping_address  # Add the full shipping address
        }

        # Check if the order ID already exists in the table
        existing_order_sql = "SELECT COUNT(*) FROM shipped_orders_header WHERE orderid = %s AND ordernumber = %s AND trackingnumber = %s"
        mycursor.execute(existing_order_sql, (order_data['orderid'], order_data['ordernumber'], order_data['trackingnumber']))
        result = mycursor.fetchone()
        if result[0] > 0:
            continue

        orders_data.append(order_data)

    all_orders_data.extend(orders_data)

    # Check if there are more pages
    if not data.get('pages') or next_page >= data['pages']:
        break

    next_page += 1

if all_orders_data:
    placeholders = ', '.join(['%s'] * len(all_orders_data[0]))
    columns = ', '.join(all_orders_data[0].keys())
    query = f"INSERT INTO shipped_orders_header ({columns}) VALUES ({placeholders})"
    values = [tuple(order.values()) for order in all_orders_data]

    mycursor.executemany(query, values)
    mydb.commit()
    print(f"{mycursor.rowcount} rows inserted into the database.")
else:
    print("No new data to insert.")

mydb.close()
