import os
import pandas as pd
from datetime import datetime
import mysql.connector

# MySQL database connection details
db_config = {
    "host": "183.82.62.219",
    "user": "dbuser",
    "password": "A@123456",
    "database": "stockandmanagement"
}

# Function to execute a query and return the result as a pandas DataFrame
def execute_query_to_dataframe(query):
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        cursor.close()
        connection.close()
        return df
    except mysql.connector.Error as err:
        print(f"MySQL Connection Error: {err}")
        return None  # Return None on error

def process_table(table_name):
    current_stock_query = f"SELECT msku, sum(quantity) as Quantity FROM tbl_inventory WHERE locationid IN (19,20) GROUP BY msku"
    current_stock_df = execute_query_to_dataframe(current_stock_query)

    table_query = f"SELECT * FROM {table_name}"
    table_df = execute_query_to_dataframe(table_query)

    if table_df is None or current_stock_df is None:
        return None  # Return None if there is a connection error or if dataframes are empty

    consolidated_rows = []

    for _, row in table_df.iterrows():
        sku = row["SKU"]
        msku = row.get("MSKU", "")
        pack_size = float(row.get("Pack_size", 0))  # Convert pack_size to float

        availability = float(row.get("availability", ""))
        if availability == 0:
            # Skip the logic and set SKU, Quantity as 0 and handling time as blank
            quantity = 0
            handling_time = ""
        else:
            if msku:
                current_stock_row = current_stock_df[current_stock_df["msku"] == msku]
                if not current_stock_row.empty:
                    quantity = int(current_stock_row.iloc[0]["Quantity"]) // int(pack_size)  # Convert to int before division
                    if quantity <= 0:
                        quantity = 0
                        handling_time = 10
                    else:
                        handling_time = 1
                else:
                    quantity = 0
                    handling_time = 10
            else:
                quantity = 0
                handling_time = 10

        consolidated_rows.append([sku, quantity, row.get("fulfillment-center-id", "")])

    header_row = ["SKU*", "New Quantity*", "Fulfillment Center ID"]

    consolidated_df = pd.DataFrame(consolidated_rows, columns=header_row)

    return consolidated_df

# Output directory
output_directory = "D:\Consolidated file"  # Use forward slashes for directory path

# Process and save the Walmart table
tables = ["Walmart_item_master"]
output_file_name_base = "consolidated_quantity"
file_extension = "xlsx"

for table_name in tables:
    output_file_name = f"{output_file_name_base}_{table_name}_{datetime.now().strftime('%Y-%m-%d%H-%M-%S')}.{file_extension}"
    output_file_path = os.path.join(output_directory, output_file_name)

    # Try to process the table, and handle connection errors
    consolidated_df = process_table(table_name)
    if consolidated_df is None:
        print(f"Skipping task for table {table_name} due to connection error.")
    else:
        consolidated_df["New Quantity*"] = consolidated_df["New Quantity*"].astype(int)
        consolidated_df.to_excel(output_file_path, index=False)

print("Data processing completed successfully!")
