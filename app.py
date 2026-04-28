import streamlit as st
import pydeck as pdk
import streamlit_authenticator as stauth
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import folium
import datetime
import plotly.express as px
from geopy.geocoders import Nominatim

from auth import check_login
authenticator = check_login()

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

# 1. IMPORT FUNGSI DARI MODUL BARU ANDA
from database import load_data_mentah, get_salesman_list, get_latest_invoice_date
from data_processing import get_kpi_summary, prepare_map_data, calculate_rfm
from visuals import create_customer_location_map, create_heatmap, create_product_bar_chart
from streamlit_folium import st_folium
from db_admin import bulk_upload_invoices, fetch_gps_coordinates, insert_single_customer, get_mapped_sku_ids
from db_admin import bulk_insert_principals, bulk_insert_brands, bulk_insert_skus, bulk_insert_sku_mappings
from db_admin import link_new_sku_mapping, bulk_insert_customers, bulk_upload_invoices

    
# Put a logout button in the sidebar
st.sidebar.write(f'Welcome, *{st.session_state["name"]}*')
authenticator.logout('Logout', 'sidebar')

# --- Check if the logged-in user is the ADMIN or NOT ---
is_admin = st.session_state["username"] == "admin"

# ==========================================
# 🚨 INDENT EVERYTHING ELSE BELOW THIS LINE! 🚨
# ==========================================

last_date_in_df = get_latest_invoice_date()
day_before_last = last_date_in_df - datetime.timedelta(days=1)
current_year = datetime.date.today().year
min_date = datetime.date(current_year, 1, 1)

default_start = max(day_before_last, min_date)
default_end = max(last_date_in_df, default_start)

## FILTERS

# 1. SIDE-BY-SIDE COLUMNS (Saves space) Filter
col1, col2 = st.sidebar.columns(2)
channel_type = col1.selectbox("Channel", ["All", "On-Trade", "Retails", "Others"])


salesman_list = get_salesman_list()
hidden_salesmen = ['WELLY', 'YUGI']
salesman_list = [name for name in salesman_list if name.strip().upper() != 'WELLY']
# salesman_list = [name for name in salesman_list if name.strip().upper() not in hidden_salesmen]
salesman_options = ["All"] + salesman_list
selected_salesman = col2.selectbox("Salesman", salesman_options)

show_heatmap = st.sidebar.toggle("Show Heatmap", value=False)
selected_sku_types = st.sidebar.selectbox("SKU Types",["All","Each Types","Lokal", "Wine", "Spirit (All)","Spirit (Principal only)","Spirit (Independen only)"])

# 3. DATE RANGE INPUT (Combined into 1 widget!)
# Pass a tuple (start, end) as the default value
date_selection = st.sidebar.date_input(
    "Date Range",
    value=(default_start, default_end),
    min_value=min_date 
)

# Safely unpack the date range (Streamlit returns 1 date while the user is still clicking)
if len(date_selection) ==2:
    start_date,end_date = date_selection
else:
    start_date = date_selection[0]
    end_date = date_selection[0]

# 4. THE EXPANDER (For your 2 new planned filters!)
with st.sidebar.expander("⚙️ More Filters", expanded=False):
    # Put your 2 new planned filters in here!
    # st.info("Additional filters go here!")
    show_sales_summary = st.toggle("Show Summary", value=False)
    # new_filter_2 = st.selectbox("Customer Type", [...])

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
                            lat, lon = fetch_gps_coordinates(address_clue)
                        else:
                            lat, lon = fetch_gps_coordinates(address)
                        source_msg = "Address mapped automatically"

                if lat and lon:
                    status = "active"
                    address2 = ""
                    area1 = ""
                    area2 =""

                    insert_single_customer(customer_id, name1, name2, name3, status, address, address2, phone, contact, area1, area2, lat, lon, note, post_id, type_id)
                    st.sidebar.success(f"{source_msg}! Added {name2} to map.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.sidebar.error("Could not find that address. Please check the 'exact coordinates' box and enter them manually.")
            else:
                st.sidebar.error("Please fill in both the Name and Address.")


# admin and non-admin can access
# --- BUILD THE UI LAYOUT ---
raw_df = load_data_mentah()

if not raw_df.empty and 'salesman' in raw_df.columns and selected_salesman != 'All':
    df = raw_df[raw_df['salesman'] == selected_salesman.upper()].copy()
else:
    df = raw_df.copy()

# Score and group customers
# 1. Tentukan tanggal hari ini (atau tanggal invoice terakhir di data) sebagai titik hitung mundur
latest_date = raw_df['invoice_date'].max()

rfm = calculate_rfm(df)

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

# st.write("Available columns:", df.columns.tolist())
# st.dataframe(df[["Name", "Address", "salesman", "quantity", "amount"]], use_container_width=True)
# st.stop()

df['invoice_date'] = pd.to_datetime(df['invoice_date'])

start_pd = pd.to_datetime(start_date)
end_pd = pd.to_datetime(end_date)
df = df[df['invoice_date'].between(start_pd, end_pd)]

# Mapping kategori ke list channel_in
channel_map = {
    "On-Trade": ["HOTE", "RECA", "LOUN", "KTVS", "CLUB"],
    "Retails": ["MODE", "TRAD", "SUPE"],
    "Others": ["SUBD", "RESE", "CORP"]
}

# Filter langsung menggunakan dictionary
if channel_type in channel_map:
    df = df[df['type_id'].isin(channel_map[channel_type])]

# 1. Group ONLY by the Customer's specific details
customer_columns = ['Name', 'Address', 'latitude', 'longitude', 'type_id']

# FINALLY: Roll it up by Customer to get the final totals for the map/table
# final_df = df.groupby(['Name', 'Address', 'latitude', 'longitude', 'type_id', 'salesman', 'bm_id'], as_index=False)[['quantity', 'amount']].sum()

# 2. Tell Pandas how to handle the rest of the columns
final_df = df.groupby(customer_columns, as_index=False, dropna=False).agg({
    'quantity': 'sum', # Add the quantities together
    'amount': 'sum',   # Add the amounts together
    
    # Grab all unique salesmen for this customer and join them with a comma
    'salesman': lambda x: ', '.join(x.dropna().unique()), 
    
    # Grab all unique SKU types (bm_id) and join them with a comma
    'bm_id': lambda x: ', '.join(x.dropna().unique())
})

final_df_for_table = df.groupby(['Name', 'Address', 'type_id'], as_index=False).agg({
    'quantity': 'sum', # Add the quantities together
    'amount': 'sum',   # Add the amounts together
    
    # Grab all unique salesmen for this customer and join them with a comma
    'salesman': lambda x: ', '.join(x.dropna().unique()), 
    
    # Grab all unique SKU types (bm_id) and join them with a comma
    'bm_id': lambda x: ', '.join(x.dropna().unique())
})

# Create two tabs for the main dashboard
if is_admin:
    tab1, tab2, tab3, tab4,tab5 = st.tabs(["🗺️ Map", "🗺️ Data Overview", "📤 Bulk Import Customer", "📤 Bulk Import New SKU", "📤 Bulk Import Invoice"])
else:
    tab1, tab2 = st.tabs(["🗺️ Map", "🗺️ Data Overview"]) # Driver just gets a normal screen for the map
    tab3 = None
    tab4 = None
    tab5 = None

# --- TAB 1: THE OPERATIONS MAP ---
with tab1:
    if show_sales_summary:
        summary_container, map_container = st.columns([2,1])
    else:
        summary_container = None
        map_container = st.container()
    
    if summary_container is not None:
        with summary_container:
            st.header(f"👤 Salesman: {selected_salesman}")
            st.divider()

            total_amount, total_qty, aov, outlets, ros, breadth = get_kpi_summary(df)

            # 5. FOC Ratio (Assuming FOC means amount == 0, or type_id == 'FOC')
            # Change the condition below based on how your database tags FOC items!
            foc_qty = df[df['amount'] == 0]['quantity'].sum() 
            foc_ratio = (foc_qty / total_qty) * 100 if total_qty > 0 else 0

            # --- DRAW THE UI ---

            # Row 1: The Big Numbers
            col1, col2 = st.columns(2)
            col1.metric("💰 Grand Total Sales", f"Rp {total_amount:,.0f}".replace(',', '.'))
            col2.metric("📦 Total Quantity", f"{total_qty:,.0f} bottles")

            st.write("") # Spacer

            # Row 2: Performance KPIs
            # col3, col4, col5, col6 = st.columns(4)
            col3, col4, col5 = st.columns(3)
            col3.metric("🧾 Avg Order Value", f"Rp {aov:,.0f}".replace(',', '.'))
            col4.metric("🛒 Avg Value / Outlet", f"Rp {ros:,.0f}".replace(',', '.'))
            col5.metric("🏪 Active Outlets", f"{outlets}")
            # col6.metric("🏷️ Product Breadth", f"{product_breadth} Types")

            # Row 3: Growth & Promos
            col6, col7 = st.columns(2)
            col6.metric("🎁 FOC Ratio", f"{foc_ratio:.1f}%")
            
            # (See note below about NAO)
            col7.metric("🚀 New Accounts (NAO)", "Requires DB Update") 

            st.divider()

            # --- 6. STACKED BAR CHART ---
            st.subheader("📊 Sales by Product Type")
            
            # Menampilkan Grafik Batang
            fig = create_product_bar_chart(df)
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No product data available.")

            # ==========================================
            # 7. TOP 10 BEST SELLING SKUs
            # ==========================================
            st.subheader("🏆 Top 10 Best Selling Products")
            
            # 1. Group by your product name column (CHANGE 'sku_name' TO YOUR ACTUAL COLUMN NAME)
            if 'sku_name' in df.columns:
                top_10_df = df.groupby('sku_name', as_index=False)[['amount', 'quantity']].sum()
                
                # 2. Sort by amount (highest to lowest) and grab the top 10
                top_10_df = top_10_df.sort_values(by='amount', ascending=False).head(10)
                
                # 3. Display as a beautiful Streamlit data table
                st.dataframe(
                    top_10_df,
                    column_config={
                        "sku_name": "Product Name",
                        "quantity": st.column_config.NumberColumn("Bottles"),
                        # This automatically formats the amount as Rupiah with separators!
                        "amount": st.column_config.NumberColumn("Total Sales (Rp)", format="Rp %,d") 
                    },
                    hide_index=True, # Hides the messy row numbers
                    use_container_width=True
                )
            else:
                st.error("⚠️ Column 'sku_name' not found in data. Please update the column name in the code!")
            
            
            st.subheader("🎯 Segmentasi Pelanggan (RFM)")
            # Tampilkan tabel yang sudah rapi
            st.dataframe(
                rfm[['Name', 'Customer_Class', 'Recency', 'Frequency', 'Monetary']],
                column_config={
                    "Name": "Nama Pelanggan",
                    "Customer_Class": "Kategori",
                    "Recency": st.column_config.NumberColumn("Hari Sejak Beli", format="%d hari"),
                    "Frequency": st.column_config.NumberColumn("Total Order", format="%dx"),
                    "Monetary": st.column_config.NumberColumn("Total Belanja", format="Rp %,d")
                },
                hide_index=True,
                use_container_width=True
            )


    with map_container:    
        if show_heatmap == False:
            st.subheader("Customers Map")

            # Panggil fungsinya, simpan ke variabel, lalu tampilkan!
            folium_map = create_customer_location_map(final_df, rfm)
            st_folium(folium_map, width=800, height=500)
        
        else:
            pydeck_map = create_heatmap(final_df)
    
            if pydeck_map is not None:
                st.pydeck_chart(pydeck_map)
            else:
                st.info("Tidak ada data dengan titik koordinat pada periode ini.")


# --- TAB 2: BULK IMPORT ---
with tab2:
    st.subheader("Data Overview")
    # Show the database data as a clean, interactive table
    st.dataframe(final_df_for_table[["Name", "Address", "salesman", "quantity", "amount"]], use_container_width=True)

if is_admin:
    with tab3:
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

                # 1. Convert completely blank text strings (like "" or "   ") into np.nan globally
                import_df = import_df.replace(r'^\s*$', np.nan, regex=True)

                # 2. Force the problematic numeric column to be an 'object' 
                # This tells Pandas: "Stop treating this as strict math, let me put text/None in here!"
                if 'post_id' in import_df.columns:
                    import_df['post_id'] = import_df['post_id'].astype(object)

                # 3. Globally replace all Pandas missing values (NaN, NaT) with standard SQL-safe None
                import_df = import_df.replace({np.nan: None, pd.NaT: None})

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

                    bulk_insert_customers(import_df)

                    # # Loop through the spreadsheet
                    # for index, row in import_df.iterrows():
                    #     name1 = row.get('name_1')
                    #     address = row.get('address')
                    #     customer_type = row.get('type_id')
                    #     lat = row.get('latitude', None)
                    #     lon = row.get('longitude', None)


                    #     if pd.isna(name1):
                    #         error_list.append(f"Row {index+1}: Missing Name or Address")
                    #         continue

                    #     if pd.isna(lat) or pd.isna(lon):
                    #         lat, lon = get_coordinates(address)
                        
                    #     if lat and lon:
                    #         insert_customer_to_db(
                    #             id=row.get('customer_id'),
                    #             name1=name1,  
                    #             name2=row.get('name_2'),
                    #             name3=row.get('name_3'),
                    #             status=row.get('status'),
                    #             address=address,
                    #             address2=row.get('address2'),
                    #             phone=row.get('phone'),
                    #             contact=row.get('contact'),
                    #             area1=row.get('area1'),
                    #             area2=row.get('area2'),
                    #             lat=lat,
                    #             lon=lon,
                    #             note=row.get('note'),
                    #             post_id=row.get('post_id'),
                    #             type_id=row.get('type_id')
                    #         )
                    #         success_count += 1
                    #     else:
                    #         error_list.append(f"Row {index+1}: Could not find coordinates for '{address}'")
                        
                    #     # Update the progress bar
                    #     my_bar.progress((index+1)/total_rows, text=f"Processing {index+1}/{total_rows}")

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
    with tab4:
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

                # STRIP THE SPACES FIRST to ensure perfect matching later!
                import_df['display_name'] = import_df['display_name'].astype(str).str.strip()

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

                    principal_df = (
                        import_df[['principal_id','principal_name']]
                        .dropna(subset=['principal_id'])
                        .drop_duplicates(subset=['principal_id'])
                        .copy()
                    )
                    bulk_insert_principals(principal_df)

                    my_bar.progress(1/4, text="Processing Brands...")
                    brand_df = (
                        import_df[['brand_id','brand_name','bm_id','principal_id']]
                        .dropna(subset=['brand_id'])
                        .drop_duplicates(subset=['brand_id'])
                        .copy()
                    )
                    bulk_insert_brands(brand_df)

                    my_bar.progress(2/4, text="Processing Skus...")
                    sku_columns = ['display_name','brand_id','sub_brand_line','varietal_flavor',
                                    'category', 'sub_category','sweetness_level', 'quality_tier',
                                    'classification','country_origin','region','volume_ml',
                                    'bottles_per_case','serving_suggestion','tags','search_slug']
                    sku_df = import_df[sku_columns] \
                        .dropna(subset=['display_name']) \
                        .drop_duplicates(subset=['display_name']) \
                        .copy()
                    
                    bulk_insert_skus(sku_df)
                    
                    my_bar.progress(3/4, text="Processing Adjust SKUs...")
                    exist_mapping_sku_df = (
                        import_df[['mapping_id','mapping_name','sku_id']]
                        .dropna(subset=['mapping_id', 'sku_id']) # Ensure BOTH exist
                        .drop_duplicates(subset=['mapping_id'])
                        .copy()
                    )
                    bulk_insert_sku_mappings(exist_mapping_sku_df)

                    # 1. Create a condition: Fill nulls with blank text, strip spaces, and check if it's completely empty
                    is_missing_sku = import_df['sku_id'].fillna('').astype(str).str.strip() == ''

                    # 2. Apply the condition to filter the dataframe
                    new_mapping_sku = (
                        import_df[is_missing_sku][['mapping_id', 'mapping_name', 'display_name']]
                        .dropna(subset=['mapping_id'])
                        .drop_duplicates(subset=['mapping_id'])
                        .copy()
                    )
                    
                    for index, row in new_mapping_sku.iterrows():
                        display_name = row.get('display_name')
                        mapping_id = row.get('mapping_id')
                        mapping_name = row.get('mapping_name')

                        link_new_sku_mapping(display_name,mapping_id, mapping_name)
                    
                    my_bar.progress(4/4, text="Done")

                    st.success(f"✅ Successfully update all sku info!")

                    if error_list:
                        st.error(f"⚠️ Some rows had issues:")
                        for error in error_list:
                            st.write(f"- {error}")

                    
                    st.cache_data.clear()
                    

            except Exception as e:
                    st.error(f"Error reading file: {e}")

if is_admin:
    with tab5:
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
                st.dataframe(import_df, use_container_width=True)

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
                        'Invoice No.', 'mapping_id', 'type_id', 'Quantity', 'Amount'
                    ]

                    # We do NOT drop duplicates here, because 1 invoice can have 5 items
                    items_df = import_df[item_columns].copy()

                    # Rename the columns to perfectly match your PostgreSQL 'invoice_items' table
                    items_df = items_df.rename(columns={
                        'Invoice No.': 'invoice_id',
                        'Quantity': 'quantity',
                        'Amount': 'amount'
                    })

                    # (Optional but recommended) Ensure Quantity and Amount are strict numbers
                    # The errors='coerce' forces any accidental text in these columns to become NaN
                    items_df['quantity'] = pd.to_numeric(items_df['quantity'], errors='coerce').fillna(0).astype(int)
                    items_df['amount'] = pd.to_numeric(items_df['amount'], errors='coerce').fillna(0)

                    # --- 4. Cek New Mapping SKU ---
                    existing_skus = get_mapped_sku_ids()

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

                    bulk_upload_invoices(company_id, start_date, end_date, headers_df, items_df)
                    # st.dataframe(headers_df, use_container_width=True)




                    # Loop through the spreadsheet



            except Exception as e:
                st.error(f"Error reading file: {e}")