import pandas as pd
import mysql.connector
import os
import time

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host="183.82.62.219",
  user="dbuser",
  password="A@123456",
  database="stockandmanagement"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define the file path for the CSV file
csv_file_path = "C:/Users/hp/Desktop/Code files/Item master upload new.csv"

# Check if the file exists
if not os.path.exists(csv_file_path):
    raise FileNotFoundError(f"The file {csv_file_path} does not exist.")

# Read the CSV file into a pandas DataFrame with the correct encoding
df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')

# Create a list to keep track of the inserted Seller_SKUs
inserted_skus = []

# Create a list to keep track of the rows with duplicate Seller_SKUs
duplicate_rows = []

# Create a list to keep track of the rows with errors
error_rows = []

# Loop through each row of the DataFrame and insert non-duplicate items into the MySQL database
for index, row in df.iterrows():
    if row['seller_sku'] not in inserted_skus:
        try:
            cursor.execute("""
                INSERT INTO tbl_item_master (seller_sku,msku, item_name, brandid, pack_size, mrp, discount, physicalweight, length, width, height, gstrate, hsncode, productcategoryid, categoryid, responsibility)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['seller_sku'],row['msku'], row['item_name'], row['brandid'], row['pack_size'], row['mrp'], row['discount'], row['physicalweight'], row['length'], row['width'], row['height'], row['gstrate'], row['hsncode'], row['productcategoryid'], row['categoryid'], row['responsibility']))
            mydb.commit()
            # Add the inserted Seller_SKU to the list
            inserted_skus.append(row['seller_sku'])
        except mysql.connector.Error as e:
            error_reason = str(e)
            # If an "Incorrect string value" error is encountered, add the row to the error_rows list
            if "Incorrect string value" in error_reason:
                error_rows.append((row, error_reason))
            else:
                # Handle other MySQL errors as needed
                pass
    else:
        # If the Seller_SKU has already been inserted, add the row to the duplicate_rows list
        duplicate_rows.append(row)

# Close the database connection
mydb.close()

# Create a new DataFrame with only the duplicate rows
df_duplicates = pd.DataFrame(duplicate_rows)

# Define the directory for saving error files
error_directory = "C:/Users/hp/Desktop/Code files/Errors/"
os.makedirs(error_directory, exist_ok=True)

# Write the duplicate rows to a new CSV file
df_duplicates.to_csv(os.path.join(error_directory, "Item_master_errors.csv"), index=False)

# If there are rows with errors, create a DataFrame and write them to a CSV file with error reasons
if error_rows:
    error_df = pd.DataFrame(error_rows, columns=["Row", "Error_Reason"])
    current_datetime = time.strftime("%Y%m%d%H%M%S")
    error_filename = os.path.join(error_directory, f"itemmastererrors_{current_datetime}.csv")
    error_df.to_csv(error_filename, index=False)
    
# Print the counts of inserted rows and errors
print(f"Inserted rows count: {len(inserted_skus)}")
print(f"Errors count: {len(error_rows)}")
