import psycopg2
import pandas as pd
import streamlit as st

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

def export_to_excel():
    conn = None
    try:
        # 1. Connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()

        print("Fetching data from PostgreSQL...")

        # 2. The SQL Query
        query = """
            SELECT c.customer_id AS "ID", 
                   c.name_2 AS "Name", 
                   c.address AS "Address",
                   c.latitude AS "Latitude", 
                   c.longitude AS "Longitude", 
                   ct.subtype_name AS "Subtype", 
                   ct.type_name AS "Type"
            FROM customers c
            JOIN customer_types ct ON c.type_id = ct.type_id;
        """

        query2 = """
            SELECT *
            FROM customers;
        """
        cur.execute(query2)
        
        # 3. Grab the data and the column names
        records = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        # 4. Use Pandas to create a data grid (DataFrame)
        print("Converting data to Excel format...")
        df = pd.DataFrame(records, columns=column_names)

        # 5. Save it as an Excel file
        filename = "Customers.xlsx"
        df.to_excel(filename, index=False)

        print(f"SUCCESS! Your data has been saved to '{filename}' in your project folder.")

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    export_to_excel()