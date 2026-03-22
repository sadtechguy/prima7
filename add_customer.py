import psycopg2
import streamlit as st

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

def insert_location(id, name1, name2, name3, address, address2, phone, contact, lat, lon, note, post_id, type_id):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()

        # The SQL INSERT command. Notice the %s placeholders. 
        # This protects your database from bad data or hacking attempts.
        insert_query = """
            INSERT INTO customers (
                customer_id, name_1, name_2, name_3, status, address, address2, phone, contact,
                area_1, area_2, latitude, longitude, note, post_id, type_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id;
        """

        # Execute the query, pairing the %s placeholders with our actual data
        cur.execute(insert_query, (id, name1, name2, name3, "active", address, address2, phone, contact, "", "", lat, lon, note, post_id, type_id))

        # Grab the newly generated ID
        new_customer_id = cur.fetchone()[0]

        # Save (commit) the changes to the database
        conn.commit()
        cur.close()

        print(f"SUCCESS! '{name2}' has been added to the database.")
        print(f"-> Assigned Location ID: {new_customer_id}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()

# --- RUNNING THE CODE ---
if __name__ == '__main__':
    print("Initializing AreaMapper Database Connection...")

    # We are adding a new Drop Point in Bekasi for JktExpress (which is User ID 1)