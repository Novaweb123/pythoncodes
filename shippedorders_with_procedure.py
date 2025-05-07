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

end_date = datetime.now()
start_date = end_date - timedelta(days=30)
page_size = 500

url = f'https://ssapi.shipstation.com/orders?orderDateStart={start_date.strftime("%Y-%m-%dT00:00:00")}&orderDateEnd={end_date.strftime("%Y-%m-%dT23:59:59")}&orderStatus=shipped&pageSize={page_size}'

api_key_secret = f"{SHIPSTATION_CONFIG['api_key']}:{SHIPSTATION_CONFIG['api_secret']}"
headers = {'Authorization': f'Basic {base64.b64encode(api_key_secret.encode()).decode()}'}

all_orders_data = []
next_page = 1
error_reasons = {}

while True:
    response = requests.get(url + f'&page={next_page}', headers=headers)
    data = response.json()

    orders_data = []
    for order in data.get('orders', []):
        for item in order.get('items', []):
            ship_to = order.get('shipTo', {})
            shipping_address = f"{ship_to.get('street1', '')} {ship_to.get('street2', '')} {ship_to.get('street3', '')}, {ship_to.get('city', '')}, {ship_to.get('state', '')} {ship_to.get('postalCode', '')}, {ship_to.get('country', '')}"

            order_data = {
                'orderDate': order.get('orderDate'),
                'orderId': order.get('orderId'),
                'orderNumber': order.get('orderNumber')[:50],
                'lineItemKey': item.get('lineItemKey'),
                'orderStatus': order.get('orderStatus'),
                'name': item.get('name'),
                'sku': item.get('sku'),
                'Quantity': item.get('quantity') if item.get('quantity') is not None else 0,
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
                'shipping_address': shipping_address
            }

            if order_data['Channel'] == '493213' or order_data['Quantity'] == 0:
                continue

            existing_order_sql = "SELECT COUNT(*) FROM shipped_orders WHERE orderNumber = %s AND lineItemKey = %s AND sku = %s"
            mycursor.execute(existing_order_sql, (order_data['orderNumber'], order_data['lineItemKey'], order_data['sku']))
            result = mycursor.fetchone()
            if result[0] > 0:
                error_reasons[(order_data['orderNumber'], order_data['lineItemKey'], order_data['sku'])] = 'Duplicate row'
                continue

            orders_data.append(order_data)

    all_orders_data.extend(orders_data)

    if not data.get('pages') or next_page >= data['pages']:
        break

    next_page += 1

# Insert new data
inserted_count = 0

for order_data in all_orders_data:
    try:
        sql = """
        INSERT IGNORE INTO shipped_orders (
            orderDate, orderId, orderNumber, lineItemKey, orderStatus, name, sku, Quantity,
            state, shipDate, Channel, shipped_from, Unit_price, taxAmount, shippingAmount,
            country, customer_name, email, phone, shipping_address
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        val = tuple(order_data.values())
        mycursor.execute(sql, val)
        mydb.commit()

        if mycursor.rowcount > 0:
            inserted_count += 1

            mycursor.callproc('sp_shippedmskuupdate', (order_data['orderId'], order_data['orderNumber'], order_data['lineItemKey']))
            mydb.commit()

            mycursor.callproc('sp_UpdateInventoryOnShipment1', (order_data['orderId'], order_data['orderNumber'], order_data['lineItemKey']))
            mydb.commit()

    except mysql.connector.Error as err:
        error_reasons[order_data['orderNumber']] = str(err)

# Delete from Unshipped_orders if exists
for order_data in all_orders_data:
    try:
        unshipped_order_sql = "SELECT COUNT(*) FROM Unshipped_orders WHERE orderNumber = %s"
        mycursor.execute(unshipped_order_sql, (order_data['orderNumber'],))
        unshipped_result = mycursor.fetchone()
        if unshipped_result[0] > 0:
            delete_unshipped_sql = "DELETE FROM Unshipped_orders WHERE orderNumber = %s"
            mycursor.execute(delete_unshipped_sql, (order_data['orderNumber'],))
            mydb.commit()
    except mysql.connector.Error as err:
        error_reasons[order_data['orderNumber']] = str(err)

# Close DB
mydb.close()

# Print how many were inserted
print(f"Total records inserted: {inserted_count}")
