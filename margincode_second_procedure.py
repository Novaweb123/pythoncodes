import mysql.connector
from mysql.connector import Error

def update_shipping_cost_for_all_orders():
    try:
        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host="183.82.62.219",
            user="dbuser",
            password="A@123456",
            database="stockandmanagement",
            autocommit=True
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Fetch all orderNumbers, lineItemKeys, and SKUs with missing shipment costs
            query = """
                SELECT ordernumber, lineitemkey, sku
                FROM tbl_ordermanagement
                WHERE orderstatus IN ("awaiting_shipment", "awaiting_fulfillment") 
                OR shipping_carrier LIKE "%Man"
            """
            cursor.execute(query)
            order_numbers = cursor.fetchall()

            # Call the stored procedure for each orderNumber
            for ordernumber, lineitemkey, sku in order_numbers:
                try:
                    cursor.callproc('p_update_awaitingshipment', [ordernumber, lineitemkey, sku])
                    connection.commit()
                except Error as proc_error:
                    print(f"Error calling procedure for ordernumber {ordernumber}: {proc_error}")

    except Error as e:
        print(f"Database connection error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Call the function
update_shipping_cost_for_all_orders()
