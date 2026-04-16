import streamlit as st
import streamlit_authenticator as stauth
import psycopg2
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

# The @st.cache_data makes the app load faster by remembering the data
@st.cache_data(ttl=300)
def load_data2():
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
            SUM(st.quantity) AS quantity,
            SUM(st.amount) AS amount
        FROM invoice_items st
        LEFT JOIN invoices i ON st.invoice_id = i.invoice_id
        LEFT JOIN customers c ON i.customer_id = c.customer_id
        GROUP BY 
            c.name_2, 
            c.address, 
            c.latitude, 
            c.longitude, 
            c.type_id, 
            i.salesman;
    """
    
    cur.execute(query)
    records = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    df = pd.DataFrame(records, columns=columns)

    cur.close()
    conn.close()
    return df

def load_data3():
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
            b.bm_id,
            SUM(st.quantity) AS quantity,
            SUM(st.amount) AS amount
        FROM invoice_items st
        JOIN invoices i USING (invoice_id)
        JOIN customers c USING (customer_id)
        JOIN mapping_sku ms USING (mapping_id)
        LEFT JOIN sku_master sm ON ms.sku_id = sm.id
        JOIN brands b USING (brand_id)
        GROUP BY 
            c.name_2, 
            c.address, 
            c.latitude, 
            c.longitude, 
            c.type_id, 
            i.salesman,
            b.bm_id
    """
    
    cur.execute(query)
    records = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    df = pd.DataFrame(records, columns=columns)

    cur.close()
    conn.close()
    return df

def insert_customer_to_db(id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id):
    """Connects to Postgres and safely inserts a new customer row."""
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()
    query = """
        INSERT INTO customers (
            customer_id, name_1, name_2, name_3, status, address, address2, phone, contact,
            area_1, area_2, latitude, longitude, note, post_id, type_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """
    cur.execute(query, (id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id))
    conn.commit()
    cur.close()
    conn.close()

def get_coordinates(address_text):
    geolocator = Nominatim(user_agent=address_text)
    try:
        location = geolocator.geocode(address_text + ", Indonesia")
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None
    

def get_existing_mapping_sku():
    try:
        # The 'with' statement guarantees the connection closes automatically
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT mapping_id FROM mapping_sku;")
                
                # We can return the list directly!
                return [row[0] for row in cur.fetchall()]
                
    except Exception as e:
        # If the database is down, show an error in Streamlit instead of crashing
        st.error(f"Database connection failed: {e}")
        return []  # Return an empty list so the rest of your code doesn't break


# --- AUTHENTICATION SETUP ---
# Fetch the credentials and cookie settings from secrets.toml
credentials = st.secrets["credentials"].to_dict()
cookie_name = st.secrets["cookie"]["name"]
cookie_key = st.secrets["cookie"]["key"]
cookie_expiry = st.secrets["cookie"]["expiry_days"]

# Initialize the authenticator
authenticator = stauth.Authenticate(
    credentials,
    cookie_name,
    cookie_key,
    cookie_expiry
)

# Display the login widget on the main screen
authenticator.login()

# --- THE SECURITY GATE ---
if st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    st.error('Please enter your username and password to access AreaMapper')
elif st.session_state["authentication_status"]:
    # 🟢 IF LOGIN IS SUCCESSFUL, SHOW THE APP!
    
    # Put a logout button in the sidebar
    authenticator.logout('Logout', 'sidebar')
    st.sidebar.write(f'Welcome, *{st.session_state["name"]}*')

    # --- Check if the logged-in user is the ADMIN or NOT ---
    is_admin = st.session_state["username"] == "admin"

    # ==========================================
    # 🚨 INDENT EVERYTHING ELSE BELOW THIS LINE! 🚨
    # ==========================================

    # Sidebar filter Salesman
    selected_salesman = st.sidebar.selectbox("Salesman",["All", "Agung ","Andreas ","Ardhi ","Didi ","Nugie ","Puji ","Rangga ","Reza ","Yugi ","Eko ","Kantor ","F.O.C "])
    selected_sku_types = st.sidebar.selectbox("SKU Types",["All","Each Types","Lokal", "Wine", "Spirit (All)","Spirit (Principal only)","Spirit (Independen only)"])

    # Make the webpage wide
    st.set_page_config(page_title="AreaMapper", layout="wide")
    st.title("📍 AreaMapper Customers Dashboard")
        
    # --- SIDEBAR: SMART DATA ENTRY FORM ---
    if is_admin:
        st.sidebar.header("➕ Add New Drop Point")

        # 1. MOVED OUTSIDE THE FORM: Now it triggers an instant UI update!
        manual_override = st.sidebar.checkbox("I have coordinates")

        # 2. THE FORM:
        with st.sidebar.form("add_location_form"):
            customer_id = st.text_input("Customer id", placeholder="e.g., SMG-SJD0")
            name1 = st.text_input("Name in Accurate")
            name2 = st.text_input("Real Name")
            name3 = st.text_input("Company Name")
            address = st.text_input("Address", placeholder="e.g., Unmapped Street 123")
            address_clue = st.text_input("Address / building name for searching lon lat")
            phone = st.text_input("Phone")
            contact = st.text_input("Contact Person")
            note = st.text_input("Note")
            post_id = st.number_input(
                label="Post ID",
                min_value=0,
                max_value=99999,
                value=25000,
                step=1
            )
            type_id = st.selectbox("Type Customer", ["HOTE", "RECA", "LOUN", "KTVS", "CLUB", "SUPE",
                                                    "MODE", "TRAD", "SUBD", "CORP", "RESE", "DIST", "OTHE"])
            st.markdown("---")

            # These will now correctly unlock when the box above is checked
            manual_lat = st.number_input("Latitude", value=-6.200000, format="%.6f", disabled=not manual_override)
            manual_lon = st.number_input("Langitude", value=106.816666, format="%.6f", disabled=not manual_override)
            submitted = st.form_submit_button("Save to Database")

            if submitted:
                if name1 and address:
                    lat, lon = None, None
                    if manual_override:
                        lat, lon = manual_lat, manual_lon
                        source_msg = "Saved with manual coordinates"
                    else:
                        with st.spinner('Calculating coordinates automatically...'):
                            if address_clue:
                                lat, lon = get_coordinates(address_clue)
                            else:
                                lat, lon = get_coordinates(address)
                            source_msg = "Address mapped automatically"

                    if lat and lon:
                        status = "active"
                        address2 = ""
                        area1 = ""
                        area2 =""

                        insert_customer_to_db(customer_id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id)
                        st.sidebar.success(f"{source_msg}! Added {name2} to map.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.sidebar.error("Could not find that address. Please check the 'exact coordinates' box and enter them manually.")
                else:
                    st.sidebar.error("Please fill in both the Name and Address.")


    # admin and non-admin can access
    # --- BUILD THE UI LAYOUT ---
    raw_df = load_data3()
    if not raw_df.empty and 'salesman' in raw_df.columns and selected_salesman != 'All':
        df = raw_df[raw_df['salesman'] == selected_salesman.upper()]
    else:
        df = raw_df

    # Filter sku type
    if not df.empty:
        if selected_sku_types == 'Wine':
            df = df[df['bm_id'] == 'WIN1']
        elif selected_sku_types == 'Spirit (All)':
            df = df[df['bm_id'].str.startswith('SPI', na=False)]
        elif selected_sku_types == 'Spirit (Principal only)':
            df = df[df['bm_id'] == 'SPI1']
        elif selected_sku_types == 'Spirit (Independen only)':
            df = df[df['bm_id'] == 'SPI2']
        elif selected_sku_types == 'Lokal':
            df = df[df['bm_id'] == 'LOC1']

    # Create two tabs for the main dashboard
    if is_admin:
        tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Live Map", "📤 Bulk Import Customer", "📤 Bulk Import New SKU", "📤 Bulk Import Invoice"])
    else:
        tab1 = st.container() # Driver just gets a normal screen for the map
        tab2 = None
        tab3 = None
        tab4 = None
    
    # --- TAB 1: THE OPERATIONS MAP ---
    with tab1:
        # Divide the screen into two columns
        col1, col2 = st.columns([1,2])

        with col1:
            st.subheader("Data Overview")
            # Show the database data as a clean, interactive table
            st.dataframe(df[["Name", "Address", "salesman", "quantity", "amount"]], use_container_width=True)

            # Add a button to refresh data
            # if st.button("🔄 Refresh Data"):
            #     st.cache_data.clear()
            #     st.rerun()

        with col2:
            st.subheader("Live Operations Map")
            
            # Create the map
            m = folium.Map(location=[-6.25, 106.75], zoom_start=11)

            # Add the markers from our database
            for index, row in df.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    # Choose color based on type and status
                    if row['type_id'] == 'IMPO':
                        color = "blue"
                    else:
                        color = "red"
                    
                    formatted_amount = f"Rp {row['amount']:,.0f}".replace(',', '.')
                    html_content = f"""
                    <div style="font-size: 16px; font-family: Arial, sans-serif;">
                        <b>{row['Name']}</b><br>
                        <span style="color: #555;">Quantity: {row['quantity']} bottles</span><br>
                        <span style="color: #555;">Amount: {formatted_amount}</span>
                    </div>
                    """
                    custom_popup = folium.Popup(html_content, max_width=300, min_width=200)

                    folium.Marker(
                        location=[row["latitude"], row["longitude"]],
                        popup=custom_popup,
                        icon=folium.Icon(color=color)
                    ).add_to(m)

            # Display the map in Streamlit
            st_folium(m, width=800, height=500)

    # --- TAB 2: BULK IMPORT ---
    if is_admin:
        with tab2:
            st.subheader("📤 Bulk Upload Customers")
            st.write("Upload an Excel (`.xlsx`) or `.csv` file. Your spreadsheet must have these exact column headers: **Name**, **Address**, and **Type**.")

            # 1. The File Uploader Widget
            uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="customer_uploader")

            if uploaded_file is not None:
                # 2. Read the file into a temporary Pandas DataFrame
                try:
                    if uploaded_file.name.endswith('.csv'):
                        import_df = pd.read_csv(uploaded_file)
                    else:
                        import_df = pd.read_excel(uploaded_file)

                    st.write("**Data Preview:**")
                    st.dataframe(import_df, use_container_width=True)

                    # 3. The Import Button
                    if st.button("Process and Import to Database", type="primary"):
                        success_count = 0
                        error_list = []

                        # Create a progress bar
                        progress_text = "Geocoding and saving to database..."
                        my_bar = st.progress(0, text=progress_text)
                        total_rows = len(import_df)

                        # Loop through the spreadsheet
                        for index, row in import_df.iterrows():
                            name1 = row.get('name_1')
                            address = row.get('address')
                            customer_type = row.get('type_id')
                            lat = row.get('latitude', None)
                            lon = row.get('longitude', None)


                            if pd.isna(name1):
                                error_list.append(f"Row {index+1}: Missing Name or Address")
                                continue

                            if pd.isna(lat) or pd.isna(lon):
                                lat, lon = get_coordinates(address)
                            
                            if lat and lon:
                                insert_customer_to_db(
                                    id=row.get('customer_id'),
                                    name1=name1,  
                                    name2=row.get('name_2'),
                                    name3=row.get('name_3'),
                                    status=row.get('status'),
                                    address=address,
                                    address2=row.get('address2'),
                                    phone=row.get('phone'),
                                    contact=row.get('contact'),
                                    area1=row.get('area1'),
                                    area2=row.get('area2'),
                                    lat=lat,
                                    lon=lon,
                                    note=row.get('note'),
                                    post_id=row.get('post_id'),
                                    type_id=row.get('type_id')
                                )
                                success_count += 1
                            else:
                                error_list.append(f"Row {index+1}: Could not find coordinates for '{address}'")
                            
                            # Update the progress bar
                            my_bar.progress((index+1)/total_rows, text=f"Processing {index+1}/{total_rows}")

                        # 4. Show the Results
                        st.success(f"✅ Successfully imported {success_count} customerss!")

                        if error_list:
                            st.error(f"⚠️ Some rows had issues:")
                            for error in error_list:
                                st.write(f"- {error}")
                        
                        st.cache_data.clear()

                except Exception as e:
                    st.error(f"Error reading file: {e}")

    if is_admin:
        with tab3:
            st.subheader("📤 Bulk Upload New SKU")
            st.write("Upload an Excel (`.xlsx`) or `.csv` file. Your spreadsheet must have these exact column headers: **SKU ID**, **SKU Name**")

            # 1. The File Uploader Widget
            uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="sku_uploader")

            if uploaded_file is not None:
                # 2. Read the file into a temporary Pandas DataFrame
                try:
                    if uploaded_file.name.endswith('.csv'):
                        import_df = pd.read_csv(uploaded_file)
                    else:
                        import_df = pd.read_excel(uploaded_file)

                    st.write("**Data Preview:**")
                    st.dataframe(import_df, use_container_width=True)

                    # 3. The Import Button
                    if st.button("Process and Import to Database", type="primary"):
                        success_count = 0
                        error_list = []

                        # Create a progress bar
                        progress_text = "saving to database..."
                        my_bar = st.progress(0, text=progress_text)
                        total_rows = len(import_df)

                        # Loop through the spreadsheet

                except Exception as e:
                        st.error(f"Error reading file: {e}")

    if is_admin:
        with tab4:
            st.subheader("📤 Bulk Upload Invoices")
            st.write("Upload an Excel (`.xlsx`) or `.csv` file. Your spreadsheet must have these exact column headers: **Name**, **Address**, and **Type**.")

            # 1. The File Uploader Widget
            uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="invoice_uploader")

            if uploaded_file is not None:
                # 2. Read the file into a temporary Pandas DataFrame
                try:
                    if uploaded_file.name.endswith('.csv'):
                        raw_df = pd.read_csv(uploaded_file, header=None)
                    else:
                        raw_df = pd.read_excel(uploaded_file, header=None)
                    
                    company_mapping = {
                        'Prima Aktif Nusantara': 'PAN',
                        'Prima Panca Gemilang': 'PPG',
                        'PT SINAR AKTIF NIRWANA': 'SAN',
                        'SBM': 'SBM',
                        'PT Sinar Mulia Gemilang': 'SMG'
                    }
                    
                    # Capture Company Name (Cell A1 -> row 0, col 0)
                    # .strip() is a lifesaver here—it removes invisible spaces from Excel!
                    raw_company_name = str(raw_df.iloc[0, 0]).strip()

                    # Look up the ID in the dictionary
                    # .get() returns None if the company isn't in the list
                    company_id = company_mapping.get(raw_company_name)

                    # 4. The Abort Switch
                    if company_id is None:
                        st.error(f"❌ Upload Aborted: Unrecognized company name '{raw_company_name}'. Please check the Excel file.")
                        st.stop()  # This instantly stops the rest of the script from running!
                    

                    # 3. Capture and parse the dates (Cell A3 -> row 2, col 0)
                    date_string = raw_df.iloc[2, 0]  # "From 01 Mar 2026 to 09 Mar 2026"

                    # Clean the string and split it into a list of two dates
                    date_parts = date_string.replace('From ', '').split(' to ')

                    # Convert to datetime and format to YYYY-MM-DD
                    start_date = pd.to_datetime(date_parts[0]).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(date_parts[1]).strftime('%Y-%m-%d')

                    # 4. Find exactly which row contains 'Invoice No.' in the first column
                    # This returns the index number of the header row (whether it's 3, 4, or 5)
                    header_row_idx = (raw_df[0] == 'Invoice No.').idxmax()

                    # --- NEW: Multi-Row Header Fix ---
                    # Grab the main header row and the one above it, replacing NaNs with empty strings
                    row_above = raw_df.iloc[header_row_idx - 1].fillna('')
                    main_row = raw_df.iloc[header_row_idx].fillna('')

                    # Fuse them together. If both have text, combine with a space.
                    combined_headers = []
                    for top, bottom in zip(row_above, main_row):
                        top_str = str(top).strip()
                        bot_str = str(bottom).strip()
                        
                        # Combine the strings, ignoring empty ones
                        full_header = f"{top_str} {bot_str}".strip()
                        combined_headers.append(full_header)

                    # 5. Rebuild the DataFrame
                    # Slice the data to start ONE row below the 'Invoice No.' row
                    import_df = raw_df.iloc[header_row_idx + 1:].copy()
                    
                    # Apply our fused headers
                    import_df.columns = combined_headers

                    # Drop columns that ended up with a completely blank header name
                    import_df = import_df.loc[:, import_df.columns != '']

                    # --- NEW: Right Boundary Crop ---
                    # Find the 'Customer Province' column and delete everything to the right of it
                    if 'Customer Province' in import_df.columns:
                        last_col_index = import_df.columns.get_loc('Customer Province')
                        import_df = import_df.iloc[:, :last_col_index + 1]

                    # Reset the index so it starts cleanly at 0
                    import_df = import_df.reset_index(drop=True)

                    # 6. Add the new company column
                    import_df['company_id'] = company_id

                    # Ensure phone numbers are treated strictly as text, and handle any empty (NaN) cells first
                    if 'Customer Phone' in import_df.columns:
                        import_df['Customer Phone'] = import_df['Customer Phone'].fillna('').astype(str)
                        
                        # Optional: If you want to strip out the spaces to make your database perfectly clean
                        # import_df['Customer Phone'] = import_df['Customer Phone'].str.replace(' ', '')

                    # --- 1. Create the composite Customer ID ---
                    # Combine the company_id (e.g., 'PAN') with the Customer No.
                    # We use .astype(str) to ensure no math happens and .str.strip() to clean hidden spaces
                    import_df['customer_id'] = f"{company_id}-" + import_df['Customer No.'].astype(str).str.strip()
                    
                    foc_customers = ['PAN-PAN', 'SBM-Z-999', 'SMG-CST0441', 'SMG-CS00767', 'SAP-CS0134']

                    # --- 2. SALES or FOC ---
                    # Create the three conditions
                    # .isin() checks the list, and .str.upper() ensures we catch 'foc', 'Foc', etc.
                    cond_customer = import_df['customer_id'].isin(foc_customers)
                    cond_salesman = import_df['Salesman Name'].astype(str).str.upper().isin(['FOC', 'F.O.C'])
                    cond_amount   = import_df['Amount'] == 0

                    # Apply the logic instantly across all rows
                    # np.where(condition, value_if_true, value_if_false)
                    import_df['type_id'] = np.where(cond_customer | cond_salesman | cond_amount, 'FOC', 'SALES')

                    # --- 3. DEPT ---
                    if company_id in ['PAN', 'PPG']:
                        import_df['dept_id'] = 'A'
                    elif company_id == 'SBM':
                        import_df['dept_id'] = 'B'
                    else:
                        conditions = [
                            import_df['Item Default Dept. Name'] == 'Frontdoor',
                            import_df['Item Default Dept. Name'] == 'Backdoor'
                        ]
                        # Define the outputs for those rules
                        choices = ['A', 'B']

                        # Apply the rules, and use default='C' for anything that doesn't match
                        import_df['dept_id'] = np.select(conditions, choices, default='C')

                    # --- 4. IS_INTERNAL ---
                    internal_id = [
                        'PAN-SMG', 'PAN-SAN', 'PAN-NIRWANA', 'PAN-SAP',
                        'SBM-S-0001', 'SBM-S-0003', 'SBM-S-0004', 'SBM-S-0005',
                        'SMG-OTH-CST-0002', 'SMG-07-S0007', 'SMG-CST0112', 'SMG-CS00712',
                        'SA1-CC-IDR-0939', 'SA1-CC-IDR-0312', 'SA1-CC-IDR-1231', 'SA1-CC-IDR-0235',
                        'SAN-CC-IDR-0312', 'SAN-CC-IDR-0235',
                        'SAP-CS0052', 'SAP-CS0192',
                        'PPG-S. 003', 'PPG-S. 005', 'PPG-S.005', 'PPG-S. 002'
                    ]
                    import_df['is_internal'] = np.where(import_df['customer_id'].isin(internal_id), 'Y', 'N')

                    st.write("**Data Preview:**")
                    # st.dataframe(import_df, use_container_width=True)

                    # 3. The Import Button
                    if st.button("Process and Import to Database", type="primary"):
                        success_count = 0
                        error_list = []

                        # Create a progress bar
                        progress_text = "Geocoding and saving to database..."
                        my_bar = st.progress(0, text=progress_text)
                        total_rows = len(import_df)


                        # --- 2. Build the Headers DataFrame (invoices table) ---
                        # Select only the header-level columns
                        header_columns = [
                            'Invoice No.', 'Invoice Date', 'Description', 'customer_id', 'company_id',
                            'dept_id', 'Salesman Name', 'is_internal'
                        ]

                        # Drop duplicates so we only have exactly 1 row per Invoice No.
                        headers_df = import_df[header_columns].drop_duplicates(subset=['Invoice No.']).copy()

                        # Rename the columns to perfectly match your PostgreSQL 'invoices' table
                        headers_df = headers_df.rename(columns={
                            'Invoice No.': 'invoice_id',
                            'Invoice Date': 'invoice_date',
                            'Description': 'description',
                            'Salesman Name': 'salesman'
                        })

                        # (Optional but recommended) Format the date for PostgreSQL
                        headers_df['invoice_date'] = pd.to_datetime(headers_df['invoice_date']).dt.strftime('%Y-%m-%d')

                        # --- 3. Build the Items DataFrame (invoice_items table) ---
                        import_df['mapping_id'] = f"{company_id}-" + import_df['Item No.'].astype(str).str.strip()

                        # Select only the detail-level columns
                        item_columns = [
                            'Invoice No.', 'mapping_id', 'type_id', 'Quantity', 'Amount', 'Item Description'
                        ]

                        # We do NOT drop duplicates here, because 1 invoice can have 5 items
                        items_df = import_df[item_columns].copy()

                        # Rename the columns to perfectly match your PostgreSQL 'invoice_items' table
                        items_df = items_df.rename(columns={
                            'Invoice No.': 'invoice_id',
                            'Quantity': 'quantity',
                            'Amount': 'amount',
                            'Item Description': 'item_description'
                        })

                        # (Optional but recommended) Ensure Quantity and Amount are strict numbers
                        # The errors='coerce' forces any accidental text in these columns to become NaN
                        items_df['quantity'] = pd.to_numeric(items_df['quantity'], errors='coerce').fillna(0).astype(int)
                        items_df['amount'] = pd.to_numeric(items_df['amount'], errors='coerce').fillna(0)

                        # --- 4. Cek New Mapping SKU ---
                        existing_skus = get_existing_mapping_sku()

                        # 2. Get the unique mapping_ids from your uploaded Excel data
                        uploaded_skus = items_df['mapping_id'].dropna().unique().tolist()

                        # 3. Use Python Sets to find any SKUs in the upload that aren't in the database
                        missing_skus = set(uploaded_skus) - set(existing_skus)

                        # 4. The Abort Switch
                        if missing_skus:
                            st.error("❌ Upload Aborted: New, unrecognized items found in the invoice!")
                            st.write("Please add these items to your SKU Mapping table before uploading this file:")
                            
                            # Display the missing SKUs nicely
                            missing_df = items_df[items_df['mapping_id'].isin(missing_skus)][['mapping_id', 'item_description']].drop_duplicates()
                            st.dataframe(missing_df)
                            
                            st.stop() # Stops the script so the database insert never happens

                        
                        st.dataframe(items_df, use_container_width=True)



                        # Loop through the spreadsheet



                except Exception as e:
                    st.error(f"Error reading file: {e}")