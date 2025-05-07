import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import mysql.connector

# MySQL database connection details
db_config = {
    'host': '183.82.62.219',
    'user': 'dbuser',
    'password': 'A@123456',
    'database': 'stockandmanagement',
    'port': 3306  # Change the port if it's different from the default MySQL port
}

# Function to establish a MySQL connection
def get_mysql_connection():
    try:
        cnx = mysql.connector.connect(**db_config)
        return cnx
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Function to execute a query and return the result as a pandas DataFrame
def execute_query_to_dataframe(query):
    connection = get_mysql_connection()
    if connection is None:
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=columns)
        return df
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}")
        return None
    finally:
        if connection:
            connection.close()

# Current date and time
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    # Query for Belmont_stock sheet
    belmont_query = "SELECT * FROM tbl_inventory WHERE locationid = 19 order by idno asc"
    belmont_df = execute_query_to_dataframe(belmont_query)

    # Query for Richmond_stock sheet
    richmond_query = "SELECT * FROM tbl_inventory WHERE locationid = 20 order by idno asc"
    richmond_df = execute_query_to_dataframe(richmond_query)

    # Query for Haridwar_stock sheet
    haridwar_query = "SELECT * FROM tbl_inventory WHERE locationid = 17 order by idno asc"
    haridwar_df = execute_query_to_dataframe(haridwar_query)

    # Query for Hyderabad_stock sheet
    hyderabad_query = "SELECT * FROM tbl_inventory WHERE locationid = 1 order by idno asc"
    hyderabad_df = execute_query_to_dataframe(hyderabad_query)

    # Create Excel workbook (adjusted for macOS path)
    excel_file_path = os.path.join("/home", "pushmycart", "Desktop", "backup_mysql", "Inventory data", f"inventory_as_on_{current_datetime}.xlsx")

    with pd.ExcelWriter(excel_file_path) as writer:
        belmont_df.to_excel(writer, sheet_name=f"Belmont_stock_{current_datetime}", index=False)
        richmond_df.to_excel(writer, sheet_name=f"Richmond_stock_{current_datetime}", index=False)
        haridwar_df.to_excel(writer, sheet_name=f"Haridwar_stock_{current_datetime}", index=False)
        hyderabad_df.to_excel(writer, sheet_name=f"Hyderabad_stock_{current_datetime}", index=False)

    print(f"Excel workbook '{excel_file_path}' has been created.")
except Exception as e:
    print(f"Error: {e}")
