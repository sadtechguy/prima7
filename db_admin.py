import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import streamlit as st
from geopy.geocoders import Nominatim

# Pastikan Anda mengimpor kredensial dari secrets atau file konfigurasi Anda
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

# ==========================================
# 1. UTILITY FUNCTIONS (API Eksternal)
# ==========================================
def fetch_gps_coordinates(address_text):
    """Mengambil koordinat latitude dan longitude menggunakan Geopy."""
    geolocator = Nominatim(user_agent="areamapper_app") # Ganti user_agent dengan nama app Anda
    try:
        location = geolocator.geocode(address_text + ", Indonesia", timeout=10)
        if location:
            return location.latitude, location.longitude
        return None, None
    except Exception as e:
        st.warning(f"Geocoding gagal untuk '{address_text}': {e}")
        return None, None

def get_mapped_sku_ids():
    """Mengambil daftar mapping_id yang sudah ada di database."""
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT mapping_id FROM mapping_sku;")
                return [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Gagal mengambil daftar SKU Mapped: {e}")
        return []


# ==========================================
# 2. SINGLE INSERT OPERATIONS
# ==========================================

def insert_single_customer(id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id):
    """Memasukkan satu pelanggan baru ke database secara aman."""
    query = """
        INSERT INTO customers (
            customer_id, name_1, name_2, name_3, status, address, address2, phone, contact,
            area_1, area_2, latitude, longitude, note, post_id, type_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id))
            conn.commit()
    except Exception as e:
        st.error(f"Gagal menyimpan pelanggan {name2}: {e}")

def link_new_sku_mapping(display_name, mapping_id, mapping_name):
    """Mencari ID dari SKU Master, lalu menautkannya ke tabel mapping_sku."""
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                # Cari SKU ID berdasarkan namanya
                cur.execute("SELECT id FROM sku_master WHERE display_name=%s;", (display_name,))
                result = cur.fetchone()
                
                if result:
                    sku_id = result[0]
                    insert_query = """
                        INSERT INTO mapping_sku (mapping_id, name, sku_id) 
                        VALUES (%s, %s, %s) 
                        ON CONFLICT (mapping_id) DO NOTHING;
                    """
                    cur.execute(insert_query, (mapping_id, mapping_name, sku_id))
                    conn.commit()
                    return sku_id
                else:
                    return None
    except Exception as e:
        st.error(f"Database error saat mapping SKU '{display_name}': {e}")
        return None
    

# ==========================================
# 3. BULK INSERT OPERATIONS (Fast Data Uploads)
# ==========================================

def bulk_insert_principals(df):
    data_tuples = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
    query = "INSERT INTO principals (principal_id, name) VALUES %s ON CONFLICT (principal_id) DO NOTHING;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data_tuples)
            conn.commit()
    except Exception as e:
        st.error(f"Error inserting principals: {e}")

def bulk_insert_brands(df):
    data_tuples = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
    query = "INSERT INTO brands (brand_id, name, bm_id, principal_id) VALUES %s ON CONFLICT (brand_id) DO NOTHING;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data_tuples)
            conn.commit()
    except Exception as e:
        st.error(f"Error inserting brands: {e}")

def bulk_insert_skus(df):
    data_tuples = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
    query = """
        INSERT INTO sku_master (
            display_name, brand_id, sub_brand_line, varietal_flavor, category, sub_category,
            sweetness_level, quality_tier, classification, country_origin, region, volume_ml,
            bottles_per_case, serving_suggestion, tags, search_slug
        ) 
        VALUES %s 
        ON CONFLICT (display_name) DO NOTHING;
    """
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data_tuples)
            conn.commit()
    except Exception as e:
        st.error(f"Error inserting SKU Master: {e}")

def bulk_insert_sku_mappings(df):
    data_tuples = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
    query = "INSERT INTO mapping_sku (mapping_id, name, sku_id) VALUES %s ON CONFLICT (mapping_id) DO NOTHING;"
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data_tuples)
            conn.commit()
    except Exception as e:
        st.error(f"Error inserting SKU mappings: {e}")

def bulk_insert_customers(df):
    """Mengunggah banyak pelanggan sekaligus (biasanya untuk yang tidak ada koordinat)."""
    data_tuples = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
    query = """
        INSERT INTO customers (
            customer_id, name_1, name_2, name_3, status, address, address2, phone, contact,
            area_1, area_2, latitude, longitude, note, post_id, type_id
        )
        VALUES %s 
        ON CONFLICT (customer_id) DO NOTHING;
    """
    
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, data_tuples)
            conn.commit()
    except Exception as e:
        st.error(f"Error bulk inserting customers: {e}")

def bulk_upload_invoices(company_id, start_date, end_date, header_df, item_df):
    """Mengamankan proses penghapusan dan pengunggahan invoice dalam satu transaksi."""
    header_tuples = [tuple(x) for x in header_df.replace({np.nan: None}).to_numpy()]
    item_tuples = [tuple(x) for x in item_df.replace({np.nan: None}).to_numpy()]
    
    delete_query = "DELETE FROM invoices WHERE invoice_date >= %s AND invoice_date <= %s AND company_id = %s;"
    insert_headers_query = "INSERT INTO invoices (invoice_id, invoice_date, description, customer_id, company_id, dept_id, salesman, is_internal) VALUES %s;"
    insert_items_query = "INSERT INTO invoice_items (invoice_id, mapping_id, type_id, quantity, amount) VALUES %s;"

    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                st.write(f"🧹 Membersihkan data lama antara {start_date} dan {end_date}...")
                cur.execute(delete_query, (start_date, end_date, company_id))
                
                st.write("📤 Mengunggah Header Invoice...")
                extras.execute_values(cur, insert_headers_query, header_tuples)
                
                st.write("📤 Mengunggah Item Invoice...")
                extras.execute_values(cur, insert_items_query, item_tuples)
                
            conn.commit()
            st.success("🎉 Unggah Selesai! Semua invoice telah diamankan di database.")
            
    except psycopg2.Error as e:
        st.error(f"❌ Database Error! Proses dibatalkan untuk melindungi data Anda. \n\nDetail: {e}")

