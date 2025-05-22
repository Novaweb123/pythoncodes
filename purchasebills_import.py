import pandas as pd
import csv
import mysql.connector

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
    host="183.82.62.219",
    user="dbuser",
    password="A@123456",
    database="stockandmanagement"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Read the CSV file into a pandas DataFrame with the correct encoding
df = pd.read_csv("C:/Users/hp/Desktop/Code files/tbl_purchase_invoices.csv", encoding='ISO-8859-1', low_memory=False)

# Create a list to keep track of the rows with errors
error_rows = []

# Initialize a counter for inserted rows
inserted_rows = 0

# Loop through each row of the DataFrame and insert items into the MySQL database
for index, row in df.iterrows():
    try:
        cursor.execute("""
            INSERT INTO tbl_purchase_invoices 
            (billdate, vendor, gstnumber, billnumber, branchid, sku, 
            item_name, quantity, purchasecost, invoicetotal, totalcost, account, 
            source_of_supply, destination_of_supply, tax_name, cgst, sgst, igst, item_type, createdby, brandid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['billdate'], row['vendor'], row['gstnumber'], row['billnumber'], row['branchid'], 
            row['sku'], row['item_name'], row['quantity'], row['purchasecost'], row['invoicetotal'], row['totalcost'], 
            row['account'], row['source_of_supply'], row['destination_of_supply'], row['tax_name'], row['cgst'], 
            row['sgst'], row['igst'], row['item_type'], row['createdby'], row['brandid']
        ))
        
        # Call the stored procedure to update inventory
        cursor.callproc("sp_updateonpurcahse", (row['billnumber'], row['sku'], row['purchasecost']))
        
        # Commit the transaction
        mydb.commit()
        
        inserted_rows += 1
    except Exception as e:
        # If an error occurs, add the row and error reason to the error_rows list
        error_rows.append((row, str(e)))
        mydb.rollback()  # Rollback in case of failure

# Close the cursor and database connection
cursor.close()
mydb.close()

# Create a new CSV file with the rows that had errors
output_file = "C:/Users/hp/Desktop/Code files/Errors/purchase_bill_error.csv"
with open(output_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(df.columns.tolist() + ["Error Reason"])  # Write the header with the additional column
    for row, error in error_rows:
        writer.writerow(row.tolist() + [error])

# Print the number of inserted rows and errors
print(f"Number of rows inserted: {inserted_rows}")
print(f"Number of errors: {len(error_rows)}")
