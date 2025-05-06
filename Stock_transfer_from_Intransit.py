import mysql.connector
import csv
import os
import datetime

# Database connection
mydb = mysql.connector.connect(
    host="183.82.62.219",
    user="dbuser",
    password="A@123456",
    database="stockandmanagement"
)
cursor = mydb.cursor(dictionary=True)

# Get tracking number input from user
tracking_number = input("Please enter your tracking number: ").strip()

# Fetch matching rows from tbl_stock_intransit
query = "SELECT * FROM tbl_stock_intransit WHERE trackingnumber = %s"
cursor.execute(query, (tracking_number,))
rows = cursor.fetchall()

# If no matching records
if not rows:
    print("❌ No tracking number is matching in the data.")
    cursor.close()
    mydb.close()
    exit()

# Initialize counter and error tracker
inserted_rows = 0
error_rows = []

# Today's date for receivedate
today = datetime.date.today()

# Process each row
for row in rows:
    try:
        cursor.execute("""
            INSERT INTO tbl_stocktransfer (
                shippeddate, trackingnumber, inventorytype, orderitemid, msku, sku, item_name, quantity,
                shippedfrom, shippedto, receiveddate, qtyreceived, partiallyreceived, damaged,
                physicalweight, length, width, height, propweight, invoicenumber
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['shippeddate'], row['trackingnumber'], row['inventorytype'], row['orderitemid'],
            row['msku'], row['sku'], row['item_name'], row['quantity'], row['shippedfrom'], row['shippedto'],
            today, row['qtyreceived'], row['partiallyreceived'], row['damaged'],
            row['physicalweight'], row['length'], row['width'], row['height'],
            row['propweight'], row['invoicenumber']
        ))

        # Call procedure only if inventorytype is 'stock'
        if row['inventorytype'].lower() == 'stock':
            cursor.callproc("sp_stocktransfer_inventory", (
                row['trackingnumber'], row['msku'], row['orderitemid']
            ))

        # Delete the row from tbl_stock_intransit after successful insert
        cursor.execute("""
            DELETE FROM tbl_stock_intransit
            WHERE trackingnumber = %s AND msku = %s AND orderitemid = %s
        """, (row['trackingnumber'], row['msku'], row['orderitemid']))

        mydb.commit()
        inserted_rows += 1

    except Exception as e:
        error_rows.append((row, str(e)))
        mydb.rollback()

# Close DB connection
cursor.close()
mydb.close()

# Write errors to CSV if any
if error_rows:
    output_file = "/home/pushmycart/Desktop/Code files/Errors/Stock_transfer_error.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(list(error_rows[0][0].keys()) + ["Error Reason"])
        for row, error_msg in error_rows:
            writer.writerow(list(row.values()) + [error_msg])

# Summary
print(f"✅ Number of rows inserted: {inserted_rows}")
print(f"❌ Number of errors: {len(error_rows)}")
if error_rows:
    print(f"⚠️ Error log saved to: {output_file}")
