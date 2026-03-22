import psycopg2
import folium
import streamlit as st

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

def create_interactive_map():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()

        print("Fetching coordinates for AreaMapper...")

        # Fetch the locations and delivery status
        query = """
            SELECT  
                   c.name_2 AS "Name", 
                   c.latitude AS "Latitude", 
                   c.longitude AS "Longitude", 
                   c.address AS "Address",
                   ct.subtype_name AS "Subtype",
                   ct.type_id AS "type_id"
            FROM customers c
            JOIN customer_types ct ON c.type_id = ct.type_id;
        """
        cur.execute(query)
        records = cur.fetchall()

        # 1. Create the Base Map
        # Centered roughly between Jakarta and Tangerang
        areamapper_map = folium.Map(location=[-6.25, 106.75], zoom_start=11)

        # 2. Add the Markers
        for row in records:
            name, lat, lon, address, type, type_id = row

            # Skip if coordinates are missing
            if lat is None or lon is None:
                continue

            # Color coding: Warehouses are Blue, Pending deliveries are Red, Done are Green
            marker_color = "red"
            icon_type = "info-sign"

            if type_id == "IMPO":
                marker_color = "blue"
                icon_type ="home"

            # Create the popup text
            popup_text = f"<b>{name}</b><br>Type: {type}"

            # Add the marker to the map
            folium.Marker(
                location=[lat, lon],
                popup=popup_text,
                icon=folium.Icon(color=marker_color, icon=icon_type)
            ).add_to(areamapper_map)

        # 3. Save the Map as an HTML webpage
        filename = "areamapper_dashboard.html"
        areamapper_map.save(filename)

        print(f"SUCCESS! Interactive map saved as '{filename}'.")

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_interactive_map()