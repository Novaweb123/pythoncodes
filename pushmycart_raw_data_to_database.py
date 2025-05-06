#importing pandas
import pandas as pd

# Read the CSV file with low_memory=False to suppress the warning
df = pd.read_csv("D:/New folder/combined.csv", low_memory=False)

#drop null vlaues in Variant SKU
df_cleans = df.dropna(subset=['Variant SKU'])

#check the values
df_cleans


#check the rows and columns count
df_cleans.shape


#check first 5 rows
df_cleans.head()

#check status to remove the inactive listings
distinct_statuses = df_cleans['Status'].unique()
print(distinct_statuses)


#now clean the status, need only active ones
df_cleans = df_cleans[~df_cleans['Status'].isin(['draft']) & df_cleans['Status'].notna() & (df_cleans['Status'] != '')]



#check the shape
df_cleans.shape


#check the column names
df_cleans.columns.tolist()




# List of columns to keep
columns_to_keep = [
    'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product Category', 'Type', 'Tags', 
    'Published', 'Option1 Value', 'Variant SKU', 'Variant Grams', 'Variant Price',
    'Image Src', 'SEO Description', 'Google Shopping / Google Product Category'
]


# Select only these columns
df_cleans_new = df_cleans[columns_to_keep]

#check the columns 
df_cleans_new.columns.tolist()



df_cleans_new = df_cleans_new.copy()



#add full title by adding the size
df_cleans_new['full title'] = df_cleans_new['Title'].fillna('') + ' ' + df_cleans_new['Option1 Value'].fillna('')

#Check shape again
df_cleans_new.shape


#save the cleaned file in following path
file_path = r'D:\New folder\cleanedfile_new.csv'
df_cleans_new.to_csv(file_path, index=False)



#---------Now Import the cleaned file into the database table



import pandas as pd
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
df = pd.read_csv("D:/New folder/cleanedfile_new.csv", encoding='ISO-8859-1', low_memory=False)

# Strip and normalize column names
df.columns = df.columns.str.strip()

# Print column names to debug
print("Columns in DataFrame:", df.columns)

# Check if 'Variant SKU' column exists
if 'Variant SKU' not in df.columns:
    print("Error: 'Variant SKU' column not found in the CSV file.")
else:
    # Create a list to keep track of the inserted Variant_SKUs
    inserted_skus = []

    # Create a list to keep track of the rows with duplicate Variant_SKUs
    duplicate_rows = []

    # Create a list to keep track of the rows with data errors
    data_error_rows = []

    # Loop through each row of the DataFrame and insert non-duplicate items into the MySQL database
    for index, row in df.iterrows():
        if row['Variant SKU'] not in inserted_skus:
            try:
                cursor.execute("""
                    INSERT INTO pmc_raw_date (
                        Handle, 
                        Title, 
                        Body_HTML,
                        Vendor, 
                        Product_category, 
                        Type, 
                        Tags, 
                        Published,
                        size, 
                        SKU, 
                        Physicalweight, 
                        Variant_Price,
                        SEO_Description,
                        Image_Src, 
                        full_title
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (row['Handle'], 
                      row['Title'], 
                      row['Body (HTML)'],
                      row['Vendor'], 
                      row['Product Category'], 
                      row['Type'], 
                      row['Tags'],
                      row['Published'],
                      row['Option1 Value'], 
                      row['Variant SKU'], 
                      row['Variant Grams'], 
                      row['Variant Price'], 
                      row['SEO Description'],
                      row['Image Src'], 
                      row['full title']))
                mydb.commit()
                # Add the inserted Variant_SKU to the list
                inserted_skus.append(row['Variant SKU'])
            except mysql.connector.IntegrityError:
                # If a duplicate entry is found, add the row to the duplicate_rows list
                duplicate_rows.append(row)
            except mysql.connector.DataError as e:
                # If a DataError occurs, add the row to the data_error_rows list with the error message
                data_error_rows.append((row, str(e)))
        else:
            # If the Variant_SKU has already been inserted, add the row to the duplicate_rows list
            duplicate_rows.append(row)

    # Close the database connection
    mydb.close()

    # Create a new DataFrame with only the duplicate rows
    df_duplicates = pd.DataFrame(duplicate_rows)

    # Create a new DataFrame with rows that had data errors and the corresponding error messages
    df_data_errors = pd.DataFrame(data_error_rows, columns=["Row", "Error_Message"])

    # Write the duplicate rows to a new CSV file
    df_duplicates.to_csv("D:/New folder/digerrors.csv", index=False)

    # Write the rows with data errors and error messages to a new CSV file
    df_data_errors.to_csv("D:/New folder/digDataErrors.csv", index=False)

    # Print the number of inserted rows, duplicate rows, and data error rows
    print(f"Inserted rows: {len(inserted_skus)}")
    print(f"Duplicate rows: {len(duplicate_rows)}")
    print(f"Data error rows: {len(data_error_rows)}")



