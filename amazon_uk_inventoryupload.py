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

def process_table(table_name, headers):
    current_stock_query = f"SELECT MSKU, sum(Quantity) as Quantity FROM Inventory WHERE Location IN (1,17,19,20) GROUP BY MSKU"
    current_stock_df = execute_query_to_dataframe(current_stock_query)

    table_query = f"SELECT * FROM {table_name}"
    table_df = execute_query_to_dataframe(table_query)

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
                current_stock_row = current_stock_df[current_stock_df["MSKU"] == msku]
                if not current_stock_row.empty:
                    quantity = int(current_stock_row.iloc[0]["Quantity"]) // int(pack_size)  # Convert to int before division
                    if quantity <= 0:
                        quantity = 200
                        handling_time = 5
                    else:
                        handling_time = 1
                else:
                    quantity = 200
                    handling_time = 5
            else:
                quantity = 200
                handling_time = 5
        consolidated_rows.append([sku, row.get("price", ""), row.get("minimum-seller-allowed-price", ""),
                                  row.get("maximum-seller-allowed-price", ""), quantity, handling_time,
                                  row.get("fulfillment-channel", "")])

    header_row = ["SKU", "price", "minimum-seller-allowed-price",
                  "maximum-seller-allowed-price", "Quantity", "handling-time",
                  "fulfillment-channel"]

    consolidated_df = pd.DataFrame(consolidated_rows, columns=header_row)

    return consolidated_df

# Output directory
output_directory = "D:\Consolidated file"  # Use forward slashes for directory path

# Process and save each task
tasks = [
    ("Amazon_sg_item_master", "consolidated_amazon_sg_quantity"),
    ("Amazon_ca_item_master", "consolidated_CA_amazon_quantity"),
    ("Amazon_uae_item_master", "consolidated_UAE_amazon_quantity"),
    ("Amazon_uk_item_master", "consolidated_uk_amazon_quantity")
]

for table_name, output_file_name_base in tasks:
    headers = None
    file_extension = "csv"  # Default to CSV format

    headers = ["SKU", "price", "minimum-seller-allowed-price",
               "maximum-seller-allowed-price", "Quantity", "handling-time",
               "fulfillment-channel"]
    file_extension = "tsv"

    output_file_name = f"{output_file_name_base}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.{file_extension}"
    output_file_path = os.path.join(output_directory, output_file_name)

    # Try to process the table, and handle connection errors
    consolidated_df = process_table(table_name, headers)
    if consolidated_df is None:
        print(f"Skipping task for table {table_name} due to connection error.")
        continue

    consolidated_df["Quantity"] = consolidated_df["Quantity"].astype(int)
    consolidated_df.to_csv(output_file_path, sep="\t", index=False)

print("Data processing completed successfully!")
