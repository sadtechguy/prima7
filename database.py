import psycopg2
import pandas as pd
import streamlit as st
from datetime import date

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

today = date.today()

# The @st.cache_data makes the app load faster by remembering the data
@st.cache_data(ttl=3600)
def load_data_mentah():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    # Updated query with SUM() and GROUP BY
    query = """
        SELECT 
            c.name_2 AS "Name", 
            c.address AS "Address", 
            c.latitude, 
            c.longitude, 
            c.type_id, 
            i.salesman, 
            i.invoice_date,
            i.invoice_id,        -- ADDED INVOICE ID
            sm.display_name AS sku_name, -- ADDED SKU NAME
            b.bm_id,
            quantity,
            amount
        FROM invoice_items st
        JOIN invoices i USING (invoice_id)
        JOIN customers c USING (customer_id)
        JOIN mapping_sku ms USING (mapping_id)
        LEFT JOIN sku_master sm ON ms.sku_id = sm.id
        JOIN brands b USING (brand_id)
        WHERE bm_id IN ('WIN1','SPI1','SPI2','LOC1')
    """
    
    cur.execute(query)
    records = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    df = pd.DataFrame(records, columns=columns)

    cur.close()
    conn.close()
    return df

def get_latest_invoice_date():
    """Fetches the most recent invoice date directly from the database."""
    query = "SELECT MAX(invoice_date) FROM invoices;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0] # Grab the single date returned
                
                if result:
                    return result
                else:
                    # Safety net: If the table is completely empty, return today
                    return today
                    
    except Exception as e:
        # Safety net: If the database is offline, return today
        return today
    
def get_salesman_list():
    """Fetches the most recent invoice date directly from the database."""
    query = "SELECT DISTINCT salesman FROM invoices WHERE salesman IS NOT NULL ORDER BY salesman;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute(query)

                # fetchall() returns a list of tuples like: [('JOHN',), ('SARAH',)]
                # The list comprehension below flattens it into: ['JOHN', 'SARAH']
                salesmen = [row[0] for row in cur.fetchall()]
                
                return salesmen
                
    except Exception as e:
        # Safety net: return an empty list if the database connection fails
        return []
    
# --- IN database.py ---
import datetime

def get_date_boundaries():
    """Fetches the oldest and newest invoice dates from the database."""
    query = "SELECT MIN(invoice_date), MAX(invoice_date) FROM invoices;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                oldest, newest = cur.fetchone() 
                
                # Safety net: If database is completely empty
                if not oldest or not newest:
                    today = datetime.date.today()
                    return today, today
                    
                return oldest, newest
                
    except Exception as e:
        today = datetime.date.today()
        return today, today