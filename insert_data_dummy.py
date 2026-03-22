import psycopg2
import streamlit as st

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

def insert_dummy_data():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()

        print("Inserting dummy data for AreaMapper...")
        # 1. Insert a Company (User) and get their new ID
        cur.execute("""
            INSERT INTO users (company_name, email) 
            VALUES (%s, %s) RETURNING user_id;
        """, ("JktExpress Logistics", "admin@jktexpress.co.id"))
        
        user_id = cur.fetchone()[0]
        print(f"-> Created User 'JktExpress Logistics' with ID: {user_id}")

        # 2. Customer Type
        subtype_data = [
            ("HOTE", "Hotel", "On-Trade"),
            ("RECA", "Resto/Cafe", "On-Trade"),
            ("LOUN", "Lounge/Bar", "On-Trade"),
            ("KTVS", "KTV/Spa", "On-Trade"),
            ("CLUB", "Club", "On-Trade"),
            ("SUPE", "Supermarket/CVS", "Off-Trade"),
            ("MODE", "Modern-Retail", "Off-Trade"),
            ("TRAD", "Traditional-Retail", "Off-Trade"),
            ("SUBD", "Sub-Distributor", "Off-Trade"),
            ("CORP", "Corporate", "Off-Trade"),
            ("RESE", "Reseller/Private", "Off-Trade"),
            ("IMPO", "Importir", "Internal"),
            ("DIST", "Distributor", "Internal"),
            ("OTHE", "Others", "Others")
        ]

        for item in subtype_data:
            cur.execute("""
                INSERT INTO customer_types (type_id, subtype_name, type_name)
                VALUES (%s, %s, %s);
            """, item)
            print(f"-> Assigned Delivery to driver: {item[1]}")

        # Save all changes to the database
        conn.commit()
        cur.close()
        print("\nSUCCESS: All data inserted perfectly!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    insert_dummy_data()
