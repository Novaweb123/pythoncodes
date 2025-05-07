import requests
import base64
import mysql.connector
import json  # Import the json module for explicit error handling

# Function to retrieve data from ShipStation API
def fetch_shipstation_orders(api_key, api_secret):
    base_url = 'https://ssapi.shipstation.com/orders'
    page_size = 500
    api_key_secret = f"{api_key}:{api_secret}"
    headers = {'Authorization': f'Basic {base64.b64encode(api_key_secret.encode()).decode()}'}

    orders_data = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = f'{base_url}?orderStatus=awaiting_shipment&page={page}&pageSize={page_size}'
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            total_pages = data.get('pages', 1)
            for order in data.get('orders', []):
                for item in order.get('items', []):
                    if item.get('unitPrice', 0) < 0:
                        orders_data.append({
                            'orderId': order.get('orderId'),
                            'orderNumber': order.get('orderNumber', '')[:50],
                            'lineItemKey': item.get('lineItemKey'),
                            'orderDate': order.get('createDate'),
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
                        })
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"ShipStation API Request Error on page {page}: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"ShipStation API JSON Decode Error on page {page}: {e}")
            print(f"Response text: {response.text}")
            break

    return orders_data

# Function to connect to MySQL database
def connect_to_mysql():
    try:
        return mysql.connector.connect(
            host="183.82.62.219",
            user="dbuser",
            password="A@123456",
            database="stockandmanagement",
            autocommit=True
        )
    except mysql.connector.Error as err:
        print(f"MySQL Connection Error: {err}")
        exit(1)

# Function to insert new orders into MySQL
def update_orders_in_mysql(orders_data, cursor, connection):
    insert_sql = """
    INSERT INTO tbl_unshipped_orders_discounts 
    (orderId, orderNumber, lineItemKey, orderDate, orderStatus, Quantity, Unit_price, 
     taxAmount, shippingAmount, name, SKU, state, country, Channel, customer_name) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    select_sql = """
    SELECT 1 FROM tbl_unshipped_orders_discounts 
    WHERE orderId = %s AND orderNumber = %s AND lineItemKey = %s LIMIT 1
    """

    for order in orders_data:
        try:
            cursor.execute(select_sql, (order['orderId'], order['orderNumber'], order['lineItemKey']))
            if not cursor.fetchone():
                cursor.execute(insert_sql, (
                    order['orderId'], order['orderNumber'], order['lineItemKey'], order['orderDate'], 
                    order['orderStatus'], order['Quantity'], order['Unit_price'], order['taxAmount'], 
                    order['shippingAmount'], order['name'], order['SKU'], order['state'], 
                    order['country'], order['Channel'], order['customer_name']
                ))
                connection.commit()
        except mysql.connector.Error as err:
            print(f"MySQL Insert Error for order {order['orderNumber']}: {err}")
            continue

# Main logic
if __name__ == "__main__":
    # Fetch ShipStation orders
    shipstation_orders = fetch_shipstation_orders(
        '33b06747ac8b4939b8f879c59a914c1b',
        '5bb743e4ab4d4c92b955beb1a1cf0b82'
    )

    if shipstation_orders:
        db_connection = connect_to_mysql()
        db_cursor = db_connection.cursor()

        update_orders_in_mysql(shipstation_orders, db_cursor, db_connection)

        db_cursor.close()
        db_connection.close()
    else:
        print("No orders fetched.")
