import mysql.connector
from mysql.connector import Error

def update_shipping_cost_for_all_unshipped_orders():
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

            # Fetch all orderNumbers from the Unshipped_orders table
            cursor.execute("SELECT DISTINCT orderNumber FROM Unshipped_orders")
            order_numbers = cursor.fetchall()

            # Call the stored procedure for each orderNumber
            for (order_number,) in order_numbers:
                cursor.callproc('Sr_UpdateShippingCostForUnshippedOrders', [order_number])

    except Error as e:
        # Handle any errors here
        pass

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Call the function
update_shipping_cost_for_all_unshipped_orders()
