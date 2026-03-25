import psycopg2
import streamlit as st

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

def create_tables():
    commands = (
        # 1. Create USERS Table (The Businesses)
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            company_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        # 2. Create CUSTOMER_TYPES Table (The Businesses)
        """
        CREATE TABLE IF NOT EXISTS customer_types (
            type_id VARCHAR(4) PRIMARY KEY,
            subtype_name VARCHAR(30),
            type_name VARCHAR(30)
        )
        """,
        # 3. CUSTOMERS Table. We use DECIMAL(9,6) for Lat/Lon because it offers ~11cm precision.
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(100) PRIMARY KEY NOT NULL,
            name_1 VARCHAR(150) NOT NULL,
            name_2 VARCHAR(150),
            name_3 VARCHAR(150),
            status VARCHAR(10),
            address TEXT,
            address2 TEXT,
            phone VARCHAR(100),
            contact VARCHAR(100),
            area_1 VARCHAR(50),
            area_2 VARCHAR(50),
            latitude DECIMAL(9,6),  -- Example: -6.2088 (Jakarta)
            longitude DECIMAL(9,6), -- Example: 106.8456
            note VARCHAR(100),
            post_id BIGINT,
            type_id VARCHAR(4) REFERENCES customer_types (type_id)
        )
        """,
        # 4. Create TRANSACTION CATEGORY
        """
        CREATE TABLE IF NOT EXISTS transaction_type (
            type_id VARCHAR(2) NOT NULL PRIMARY KEY,
            name VARCHAR(14)
        )
        """,
        # 5. Create COMPANIES
        """
        CREATE TABLE IF NOT EXISTS companies (
            company_id VARCHAR(3) NOT NULL PRIMARY KEY,
            name TEXT,
            type TEXT,
            join_code VARCHAR(3)
        )
        """,
        # 6. Create DEPT
        """
        CREATE TABLE IF NOT EXISTS dept (
            dept_id VARCHAR(1) NOT NULL PRIMARY KEY,
            name TEXT
        )
        """,
        # 7. Create SALESMAN
        """
        CREATE TABLE IF NOT EXISTS salesman (
            salesman_id VARCHAR(30) NOT NULL PRIMARY KEY,
            name_1 TEXT,
            name_2 TEXT,
            status TEXT
        )
        """,
        # 8. Create INVOICES Table
        """
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id VARCHAR(50) NOT NULL PRIMARY KEY,
            invoice_date DATE NOT NULL,
            description TEXT, 
            customer_id VARCHAR(150) REFERENCES customers (customer_id),
            company_id VARCHAR(3) REFERENCES companies (company_id),
            dept_id VARCHAR(1) REFERENCES dept (dept_id),
            salesman TEXT,
            type_id VARCHAR(2) REFERENCES transaction_type (type_id)
        )
        """,
        # 9. Create MAPPING SKU
        """
        CREATE TABLE IF NOT EXISTS mapping_sku (
            mapping_id VARCHAR(50) NOT NULL PRIMARY KEY,
            name TEXT,
            sku_id VARCHAR(8)
        )
        """,
        # 10. Create SALES TYPES
        """
        CREATE TABLE IF NOT EXISTS sales_types (
            type_id VARCHAR(4) NOT NULL PRIMARY KEY,
            name TEXT
        )
        """,
        # 11. Create SALES TRANSACTION
        """
        CREATE TABLE IF NOT EXISTS sales_transactions (
            transaction_id TEXT NOT NULL PRIMARY KEY,
            invoice_id VARCHAR(50) REFERENCES invoices (invoice_id) ON DELETE CASCADE,
            mapping_id VARCHAR(50) REFERENCES mapping_sku (mapping_id),
            type_id VARCHAR(5) REFERENCES sales_types (type_id),
            quantity INTEGER,
            amount NUMERIC(12, 0)
        )
        """,
        # 12. Create BRAND MANAGER
        """
        CREATE TABLE IF NOT EXISTS brand_managers (
            bm_id VARCHAR(4) NOT NULL PRIMARY KEY,
            name TEXT
        )
        """,
        # 13. Create PRINCIPAL
        """
        CREATE TABLE IF NOT EXISTS principals (
            principal_id VARCHAR(3) NOT NULL PRIMARY KEY,
            name TEXT
        )
        """,
        # 14. Create BRAND
        """
        CREATE TABLE IF NOT EXISTS brands (
            brand_id VARCHAR(5) NOT NULL PRIMARY KEY,
            name TEXT,
            principal_id VARCHAR(3) REFERENCES principals (principal_id)
        )
        """,
        # 15.Create SKU
        """
        CREATE TABLE IF NOT EXISTS skus (
            sku_id VARCHAR(7) NOT NULL PRIMARY KEY,
            name TEXT,
            volume INTEGER,
            packaging INTEGER,
            category VARCHAR(5),
            subtype_id VARCHAR(6),
            brand_id VARCHAR(5) REFERENCES brands (brand_id),
            principal_id VARCHAR(3) REFERENCES principals (principal_id)
        )
        """
    )

    conn = None
    try:
        # Connect to the PostgreSQL server
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()

        # Run each command one by one
        for command in commands:
            cur.execute(command)
        
        # Save changes
        cur.close()
        conn.commit()
        print("SUCCESS: Prima7 database schema created!")
        print("- Table 'users' created.")
        print("- Table 'customers' created.")
        print("- Table 'customer types' created.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()