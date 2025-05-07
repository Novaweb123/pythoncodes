import requests
import base64
import mysql.connector
import json  # Import the json module for explicit error handling

# Function to convert datetime from Veeqo format to MySQL format
def convert_datetime(veeqo_datetime):
    return veeqo_datetime[:-1]

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
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            data = response.json()

            if 'orders' in data:
                total_pages = data.get('pages', 1)
                for order in data['orders']:
                    for item in order.get('items', []):
                        orders_data.append({
                            'orderId': order.get('orderId'),
                            'orderNumber': order.get('orderNumber')[:50],
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
            break  # Stop fetching if there's a persistent request error
        except json.JSONDecodeError as e:
            print(f"ShipStation API JSON Decode Error on page {page}: {e}")
            print(f"Response text: {response.text}")  # Print the raw response for debugging
            break  # Stop fetching if the JSON is consistently invalid

    return orders_data

# Function to retrieve data from Veeqo API
def fetch_veeqo_orders(api_keys):
    base_url = 'https://api.veeqo.com/orders'
    orders_data = []

    for api_key in api_keys:
        headers = {'x-api-key': api_key}
        params = {'status': 'awaiting_fulfillment', 'page': 1, 'per_page': 100}

        while True:
            try:
                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                if not data:
                    break

                for order in data:
                    for item in order.get('line_items', []):
                        orders_data.append({
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
                        })
                params['page'] += 1

            except requests.exceptions.RequestException as e:
                print(f"Veeqo API Request Error with key '{api_key}' on page {params['page']}: {e}")
                break
            except json.JSONDecodeError as e:
                print(f"Veeqo API JSON Decode Error with key '{api_key}' on page {params['page']}: {e}")
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

# Function to insert and update orders data in MySQL
def update_orders_in_mysql(orders_data, cursor, connection):
    # Delete existing records in Unshipped_orders table
    delete_sql = "DELETE FROM Unshipped_orders WHERE 1=1"
    cursor.execute(delete_sql)
    print("Existing records deleted from Unshipped_orders table")

    # SQL to insert data into the Unshipped_orders table
    insert_sql = """
    INSERT INTO Unshipped_orders (orderId, orderNumber, lineItemKey, orderDate, orderStatus, Quantity, Unit_price, taxAmount, shippingAmount, name, SKU, state, country, Channel, customer_name) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # SQL to check if the order already exists in the table
    select_sql = "SELECT 1 FROM Unshipped_orders WHERE orderId = %s AND orderNumber = %s AND lineItemKey = %s LIMIT 1"

    # Stored procedures to update MSKU and shipping cost
    call_procedure_msku_sql = "CALL Sr_UpdateMSKUForUnshippedOrders2(%s, %s, %s)"
    call_procedure_shipping_sql = "CALL Sr_UpdateShippingCostForUnshippedOrders(%s)"

    for order in orders_data:
        # Check if the order is already in the database
        cursor.execute(select_sql, (order['orderId'], order['orderNumber'], order['lineItemKey']))
        if not cursor.fetchone():
            try:
                # Insert new record
                cursor.execute(insert_sql, (
                    order['orderId'], order['orderNumber'], order['lineItemKey'], order['orderDate'], 
                    order['orderStatus'], order['Quantity'], order['Unit_price'], order['taxAmount'], 
                    order['shippingAmount'], order['name'], order['SKU'], order['state'], order['country'], 
                    order['Channel'], order['customer_name']
                ))
                connection.commit()  # Explicit commit after insertion

                # Call stored procedures to update MSKU and shipping cost
                try:
                    cursor.execute(call_procedure_msku_sql, (order['orderId'], order['orderNumber'], order['lineItemKey']))
                    connection.commit()  # Explicit commit after calling procedure
                except mysql.connector.Error as err:
                    print(f"Error in calling Sr_UpdateMSKUForUnshippedOrders2: {err}")

                try:
                    cursor.execute(call_procedure_shipping_sql, (order['orderNumber'],))
                    connection.commit()  # Explicit commit after calling procedure
                except mysql.connector.Error as err:
                    print(f"Error in calling Sr_UpdateShippingCostForUnshippedOrders: {err}")

            except mysql.connector.Error as err:
                print(f"MySQL Insert Error: {err}")

# Main logic
if __name__ == "__main__":
    # Fetch orders from ShipStation and Veeqo
    shipstation_orders = fetch_shipstation_orders('33b06747ac8b4939b8f879c59a914c1b', '5bb743e4ab4d4c92b955beb1a1cf0b82')
    veeqo_orders = fetch_veeqo_orders(['Vqt/1d2cb2113d124f7dae997eead2a1b229', 'Vqt/0a6bbd57d30e7631d59dbf27912b8495'])

    # Combine ShipStation and Veeqo orders data
    orders_data = shipstation_orders + veeqo_orders

    # Connect to MySQL database
    db_connection = connect_to_mysql()
    db_cursor = db_connection.cursor()

    # Insert and update orders data in MySQL
    update_orders_in_mysql(orders_data, db_cursor, db_connection)

    # Commit and close database connection
    db_connection.commit()
    db_cursor.close()
    db_connection.close()
