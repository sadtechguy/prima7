import streamlit as st
import pandas as pd
import numpy as np

from auth import check_login
authenticator = check_login()

# --- CONFIGURATION ---
DB_NAME = "prima7"
DB_USER = "postgres"
DB_PASS = st.secrets["DB_PASS"]  # <--- UPDATE THIS!
DB_HOST = "localhost"

# 1. IMPORT FUNGSI DARI MODUL BARU ANDA
from database import load_data_mentah, get_salesman_list, get_latest_invoice_date
from data_processing import get_kpi_summary, prepare_map_data, calculate_rfm, get_default_date_range
from data_processing import get_active_salesman, get_company_id, get_range_date_for_bulk_invoice
from visuals import create_customer_location_map, create_heatmap, create_product_bar_chart
from streamlit_folium import st_folium
from db_admin import bulk_upload_invoices, fetch_gps_coordinates, insert_single_customer, get_mapped_sku_ids
from db_admin import bulk_insert_principals, bulk_insert_brands, bulk_insert_skus, bulk_insert_sku_mappings
from db_admin import link_new_sku_mapping, bulk_insert_customers, bulk_upload_invoices

# --- SUNTIKAN CUSTOM CSS ---
st.markdown(
    """
    <style>
    /* 1. Mengecilkan ukuran font Judul Metric (misal: "Grand Total Sales") */
    [data-testid="stMetricLabel"] > div {
        font-size: 14px !important;
    }
    
    /* 2. Mengecilkan ukuran font Angka Metric (misal: "Rp 1.000.000") */
    [data-testid="stMetricValue"] > div {
        font-size: 22px !important; 
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ==========================================
# SIDE BAR 
# ==========================================
st.sidebar.write(f'Welcome, *{st.session_state["name"]}*')
authenticator.logout('Logout', 'sidebar') # Put a logout button in the sidebar

# --- prepare variable
is_admin = st.session_state["username"] == "admin"
default_start, default_end, oldest_date = get_default_date_range()

# ==========================================
# SIDE BAR: FILTERS
# ==========================================
###### Filter Channel
col1, col2 = st.sidebar.columns(2)
channel_type = col1.selectbox("Channel", ["All", "On-Trade", "Retails", "Others"])

###### Filter Salesman
salesman_list = get_active_salesman()
salesman_options = ["All"] + salesman_list
selected_salesman = col2.selectbox("Salesman", salesman_options)

###### Filter Heat Map and sku_types
show_heatmap = st.sidebar.toggle("Show Heatmap", value=False)

sku_mapping = {
    "All": "ALL",
    "Lokal": "LOC1",
    "Wine": "WIN1",
    "Spirit (All)": "SPIALL", 
    "Spirit (Principal only)": "SPI1",
    "Spirit (Independen only)": "SPI2"
}
selected_label = st.sidebar.selectbox(
    "🏷️ SKU Types",
    options=list(sku_mapping.keys())
)
selected_sku_type = sku_mapping[selected_label]

# Filter date range (Combined into 1 widget!)
# Pass a tuple (start, end) as the default value
date_selection = st.sidebar.date_input(
    "Date Range",
    value=(default_start, default_end),
    min_value=oldest_date 
)

# Safely unpack the date range (Streamlit returns 1 date while the user is still clicking)
if len(date_selection) ==2:
    start_date,end_date = date_selection
else:
    start_date = date_selection[0]
    end_date = date_selection[0]

# THE EXPANDER (For your our new planned filters!)
with st.sidebar.expander("⚙️ More Filters", expanded=False):
    # Put your 2 new planned filters in here!
    # st.info("Additional filters go here!")
    show_sales_summary = st.toggle("Show Summary", value=False)
    # new_filter_2 = st.selectbox("Customer Type", [...])

# Make the webpage wide
st.set_page_config(page_title="AreaMapper", layout="wide")
st.title("📍 AreaMapper Customers Dashboard")

# ==========================================
# SIDE BAR: SMART DATA ENRY FORM
# ==========================================
if is_admin:
    with st.sidebar.expander("➕ Add New Drop Point", expanded=False):
        # st.sidebar.header("➕ Add New Drop Point")

        manual_override = st.checkbox("I have coordinates")
        with st.form("add_location_form"):
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
# ==========================================
# BUILD THE UI LAYOUT
# ==========================================

# ==========================================
# UI LAYOUT: Prepare the data
# ==========================================
raw_df = load_data_mentah()

# st.write("Available columns:", df.columns.tolist())
# st.dataframe(df[["Name", "Address", "salesman", "quantity", "amount"]], use_container_width=True)
# st.stop()

if not raw_df.empty and 'salesman' in raw_df.columns and selected_salesman != 'All':
    df = raw_df[raw_df['salesman'] == selected_salesman.upper()].copy()
else:
    df = raw_df.copy()


rfm_df = calculate_rfm(df)

# Filter sku type
if not df.empty and selected_sku_type != 'ALL':
    if selected_sku_type =='SPIALL':
        df = df[df['bm_id'].str.startswith('SPI', na=False)]
    else:
        df = df[df['bm_id'] == selected_sku_type]
    

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

# 2. Tell Pandas how to handle the rest of the columns
map_df = df.groupby(customer_columns, as_index=False, dropna=False).agg({
    'quantity': 'sum', # Add the quantities together
    'amount': 'sum',   # Add the amounts together
    
    # Grab all unique salesmen for this customer and join them with a comma
    'salesman': lambda x: ', '.join(x.dropna().unique()), 
    
    # Grab all unique SKU types (bm_id) and join them with a comma
    'bm_id': lambda x: ', '.join(x.dropna().unique())
})

df_for_table_overview = df.groupby(['Name', 'Address', 'type_id'], as_index=False).agg({
    'quantity': 'sum', # Add the quantities together
    'amount': 'sum',   # Add the amounts together
    
    # Grab all unique salesmen for this customer and join them with a comma
    'salesman': lambda x: ', '.join(x.dropna().unique()), 
    
    # Grab all unique SKU types (bm_id) and join them with a comma
    'bm_id': lambda x: ', '.join(x.dropna().unique())
})

# So now we have df, rfm_df, map_df, df_for_table_overview

# ==========================================
# UI LAYOUT: THE DASHBOARD
# ==========================================
if is_admin:
    tab1, tab2, tab3, tab4,tab5 = st.tabs(["🗺️ Map", "🗺️ Data Overview", "📤 Bulk Import Customer", "📤 Bulk Import New SKU", "📤 Bulk Import Invoice"])
else:
    tab1, tab2 = st.tabs(["🗺️ Map", "🗺️ Data Overview"]) # Driver just gets a normal screen for the map
    tab3 = None
    tab4 = None
    tab5 = None

# ==========================================
# UI LAYOUT TAB 1: SUMMARY AND MAP
# ==========================================
with tab1:
    if show_sales_summary:
        summary_container, map_container = st.columns([2,1])
    else:
        summary_container = None
        map_container = st.container()
    
    # ==========================================
    # TAB1-COL1: SUMMARY
    # ==========================================
    if summary_container is not None:
        with summary_container:
            st.header(f"👤 Salesman: {selected_salesman}")
            st.divider()
            
            # ========= SUMMARY: PREPARING DATA # =========
            total_amount, total_qty, aov, outlets, ros, breadth = get_kpi_summary(df)

            # FOC Ratio (Assuming FOC means amount == 0, or type_id == 'FOC')
            foc_qty = df[df['salesman'] == 'F.O.C ']['quantity'].sum() 
            foc_ratio = (foc_qty / total_qty) * 100 if total_qty > 0 else 0

            # ========= SUMMARY: THE UI # =================
            # SUMMARY-UI: THE NUMBERS (TOTAL AND OTHERS) -----------
            col1, col2 = st.columns(2)
            col1.metric("💰 Grand Total Sales", f"Rp {total_amount:,.0f}".replace(',', '.'))
            col2.metric("📦 Total Quantity", f"{total_qty:,.0f} bottles")

            st.write("") # Spacer

            # Row 2: Performance KPIs
            col3, col4 = st.columns(2)
            col3.metric("🧾 Avg Value / Invoice", f"Rp {aov:,.0f}".replace(',', '.'))
            col4.metric("🛒 Avg Value / Outlet", f"Rp {ros:,.0f}".replace(',', '.'))
            # col6.metric("🏷️ Product Breadth", f"{product_breadth} Types")

            # Row 3: Growth & Promos
            col5, col6, col7 = st.columns(3)
            col5.metric("🎁 FOC Ratio", f"{foc_ratio:.1f}%")
            col6.metric("🏪 Active Outlets", f"{outlets}")
            
            # (See note below about NAO)
            col7.metric("🚀 New Accounts (NAO)", "Requires DB Update") 

            st.divider()


            # SUMMARY-UI: STACKED BAR CHART -----------
            st.subheader("📊 Sales by Product Type")
            
            # Menampilkan Grafik Batang
            fig = create_product_bar_chart(df)
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No product data available.")

            
            # SUMMARY-UI: TOP 10 BEST SELLING SKUs -----------
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
            
            # SUMMARY-UI: RFM CUSTOMER -----------
            st.subheader("🎯 Segmentasi Pelanggan (RFM)")
            # Tampilkan tabel yang sudah rapi
            st.dataframe(
                rfm_df[['Name', 'Customer_Class', 'Recency', 'Frequency', 'Monetary']],
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

    # ==========================================
    # TAB1-COL2: MAP
    # ==========================================
    with map_container:    
        if show_heatmap == False:
            st.subheader("Customers Map")

            # Panggil fungsinya, simpan ke variabel, lalu tampilkan!
            folium_map = create_customer_location_map(map_df, rfm_df)
            st_folium(folium_map, width=800, height=500)
        
        else:
            pydeck_map = create_heatmap(map_df)
    
            if pydeck_map is not None:
                st.pydeck_chart(pydeck_map)
            else:
                st.info("Tidak ada data dengan titik koordinat pada periode ini.")

# ==========================================
# UI LAYOUT TAB 2: DATA OVERVIEW
# ==========================================
with tab2:
    st.subheader("Data Overview")
    # Show the database data as a clean, interactive table
    st.dataframe(df_for_table_overview[["Name", "Address", "salesman", "quantity", "amount"]], use_container_width=True)

# ==========================================
# UI LAYOUT TAB 3: BULK IMPORT CUSTOMERS
# ==========================================
if is_admin:
    with tab3:
        st.subheader("📤 Bulk Upload Customers")
        st.write("Upload an Excel (`.xlsx`) or `.csv` file. Your spreadsheet must have these exact column headers: **customer id**, **name**, and **Type**.")

        # BULK-CUSTOMERS-1. THE FILE UPLOADER WIDGET ------------
        uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="customer_uploader")

        if uploaded_file is not None:
            # BULK-CUSTOMERS-2. READ THE FILE INTO A TEMPORARY PANDAS DATAFRAME ------------
            try:
                if uploaded_file.name.endswith('.csv'):
                    import_df = pd.read_csv(uploaded_file)
                else:
                    import_df = pd.read_excel(uploaded_file)

                # BULK-CUSTOMERS-3. CLEANING DF ------------
                # 1. Convert completely blank text strings (like "" or "   ") into np.nan globally
                import_df = import_df.replace(r'^\s*$', np.nan, regex=True)

                # 2. Force the problematic numeric column to be an 'object' 
                # This tells Pandas: "Stop treating this as strict math, let me put text/None in here!"
                if 'post_id' in import_df.columns:
                    import_df['post_id'] = import_df['post_id'].astype(object)

                # 3. Globally replace all Pandas missing values (NaN, NaT) with standard SQL-safe None
                import_df = import_df.replace({np.nan: None, pd.NaT: None})

                # BULK-CUSTOMERS-4. PREVIEW BEFORE UPLOAD ------------
                st.write("**Data Preview:**")
                st.dataframe(import_df, use_container_width=True)

                # BULK-CUSTOMERS-5. IMPORT BUTTON ------------
                if st.button("Process and Import to Database", type="primary"):
                    # Gunakan spinner alih-alih progress bar
                    with st.spinner("🚀 Mengunggah data ke database... Mohon tunggu sebentar."):
                        bulk_insert_customers(import_df)

                    # Tampilkan hasil DI LUAR blok spinner (hilangkan indentasi)
                    # Ini memastikan spinner hilang dulu, baru pesan sukses muncul
                    st.success(f"✅ Successfully imported customerss!")

                    # Bersihkan cache agar tabel/grafik langsung membaca data terbaru
                    st.cache_data.clear()

            except Exception as e:
                st.error(f"Error reading file: {e}")

# ==========================================
# UI LAYOUT TAB 4: BULK IMPORT SKUS
# ==========================================
if is_admin:
    with tab4:
        st.subheader("📤 Bulk Upload New SKU")
        st.write("Upload an Excel (`.xlsx`) or `.csv` file. Your spreadsheet must have these exact column headers: **SKU ID**, **SKU Name**")

        # BULK-SKUS-1. THE FILE UPLOADER WIDGET ------------
        uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="sku_uploader")

        if uploaded_file is not None:
            # BULK-SKUS-2. READ THE FILE INTO A TEMPORARY DF ------------
            try:
                if uploaded_file.name.endswith('.csv'):
                    import_df = pd.read_csv(uploaded_file)
                else:
                    import_df = pd.read_excel(uploaded_file)

                # BULK-SKUS-3. CLEANING DF ------------
                # Strip the space to ensure perfect matching later!
                import_df['display_name'] = import_df['display_name'].astype(str).str.strip()

                # BULK-SKUS-4. PREVIEW BEFORE UPLOAD ------------
                st.write("**Data Preview:**")
                st.dataframe(import_df, use_container_width=True)

                # BULK-SKUS-5. IMPORT BUTTON ------------
                if st.button("Process and Import to Database", type="primary"):
                    # 1. Handle Upload Principals
                    with st.spinner("🚀 Updating principals ke database... Mohon tunggu sebentar."):
                        principal_df = (
                            import_df[['principal_id','principal_name']]
                            .dropna(subset=['principal_id'])
                            .drop_duplicates(subset=['principal_id'])
                            .copy()
                        )
                        bulk_insert_principals(principal_df)
                    st.success(f"✅ Successfully update principals!")

                    # 2. Handle Upload Brands
                    with st.spinner("🚀 Updating brands ke database... Mohon tunggu sebentar."):
                        brand_df = (
                            import_df[['brand_id','brand_name','bm_id','principal_id']]
                            .dropna(subset=['brand_id'])
                            .drop_duplicates(subset=['brand_id'])
                            .copy()
                        )
                        bulk_insert_brands(brand_df)
                    st.success(f"✅ Successfully update brands!")

                    # 3. Handle Upload SKUs
                    with st.spinner("🚀 Updating SKUs ke database... Mohon tunggu sebentar."):
                        sku_columns = ['display_name','brand_id','sub_brand_line','varietal_flavor',
                                        'category', 'sub_category','sweetness_level', 'quality_tier',
                                        'classification','country_origin','region','volume_ml',
                                        'bottles_per_case','serving_suggestion','tags','search_slug']
                        sku_df = import_df[sku_columns] \
                            .dropna(subset=['display_name']) \
                            .drop_duplicates(subset=['display_name']) \
                            .copy()
                    
                        bulk_insert_skus(sku_df)
                    st.success(f"✅ Successfully update SKUs!")
                    
                    # 4. Handle Upload Mapping SKUs
                    with st.spinner("🚀 Updating mapping sku ke database... Mohon tunggu sebentar."):
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
                    st.success(f"✅ Successfully Matching SKUs!")
                    st.cache_data.clear()
                    

            except Exception as e:
                    st.error(f"Error reading file: {e}")

# ==========================================
# UI LAYOUT TAB 5: BULK IMPORT INVOICES
# ==========================================
if is_admin:
    with tab5:
        st.subheader("📤 Bulk Upload Invoices")
        st.write("Upload an Excel (`.xlsx`) or `.csv` file. Get Excel data from Accurate Report")

        # BULK-INVOICES-1. THE FILE UPLOADER WIDGET ------------
        uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'], key="invoice_uploader")

        if uploaded_file is not None:
            # BULK-INVOICES-2. READ THE FILE INTO A TEMPORARY DF ------------
            try:
                if uploaded_file.name.endswith('.csv'):
                    raw_df = pd.read_csv(uploaded_file, header=None)
                else:
                    raw_df = pd.read_excel(uploaded_file, header=None)
                
                # BULK-INVOICES-3. CLEANING DF ------------
                
                # 1. Capture Company Name (Cell A1 -> row 0, col 0)
                # .strip() is a lifesaver here—it removes invisible spaces from Excel!
                raw_company_name = str(raw_df.iloc[0, 0]).strip()
                company_id = get_company_id(raw_company_name)

                # The Abort Switch
                if company_id is None:
                    st.error(f"❌ Upload Aborted: Unrecognized company name '{raw_company_name}'. Please check the Excel file.")
                    st.stop()  # This instantly stops the rest of the script from running!
                
                # 2. Capture and Cleaning Dates
                # Capture and parse the dates (Cell A3 -> row 2, col 0)
                date_string = raw_df.iloc[2, 0]  # "From 01 Mar 2026 to 09 Mar 2026"
                start_date, end_date = get_range_date_for_bulk_invoice(date_string)

                # 3. Find exactly which row contains 'Invoice No.' in the first column
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

                # 4. Rebuild the DataFrame
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

                # 5. Add the new company column
                import_df['company_id'] = company_id

                # Ensure phone numbers are treated strictly as text, and handle any empty (NaN) cells first
                if 'Customer Phone' in import_df.columns:
                    import_df['Customer Phone'] = import_df['Customer Phone'].fillna('').astype(str)
                    
                    # Optional: If you want to strip out the spaces to make your database perfectly clean
                    # import_df['Customer Phone'] = import_df['Customer Phone'].str.replace(' ', '')

                # 6. Create the composite Customer ID ---
                # Combine the company_id (e.g., 'PAN') with the Customer No.
                # We use .astype(str) to ensure no math happens and .str.strip() to clean hidden spaces
                import_df['customer_id'] = f"{company_id}-" + import_df['Customer No.'].astype(str).str.strip()
                

                # 7. SALES or FOC ---
                foc_customers = ['PAN-PAN', 'SBM-Z-999', 'SMG-CST0441', 'SMG-CS00767', 'SAP-CS0134']

                # Create the three conditions
                # .isin() checks the list, and .str.upper() ensures we catch 'foc', 'Foc', etc.
                cond_customer = import_df['customer_id'].isin(foc_customers)
                cond_salesman = import_df['Salesman Name'].astype(str).str.upper().isin(['FOC', 'F.O.C'])
                cond_amount   = import_df['Amount'] == 0

                # Apply the logic instantly across all rows
                # np.where(condition, value_if_true, value_if_false)
                import_df['type_id'] = np.where(cond_customer | cond_salesman | cond_amount, 'FOC', 'SALES')

                # 8. DEPT ---
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

                # 9. IS_INTERNAL ---
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

                # BULK-INVOICES-4. PREVIEW BEFORE UPLOAD ------------
                st.write("**Data Preview:**")
                st.dataframe(import_df, use_container_width=True)

                # BULK-INVOICES-5. IMPORT BUTTON ------------
                if st.button("Process and Import to Database", type="primary"):
                    with st.spinner("🚀 Processing Invoices ke database... Mohon tunggu sebentar."):

                        # 1. Build the Headers DataFrame (for invoices table) ---
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

                        # 2. Build the Items DataFrame (for invoice_items table) ---
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

                        # 3. Cek New Mapping SKU ---
                        existing_skus = get_mapped_sku_ids()

                        # Get the unique mapping_ids from your uploaded Excel data
                        uploaded_skus = items_df['mapping_id'].dropna().unique().tolist()

                        # Use Python Sets to find any SKUs in the upload that aren't in the database
                        missing_skus = set(uploaded_skus) - set(existing_skus)

                        # The Abort Switch
                        if missing_skus:
                            st.error("❌ Upload Aborted: New, unrecognized items found in the invoice!")
                            st.write("Please add these items to your SKU Mapping table before uploading this file:")
                            
                            # Display the missing SKUs nicely
                            missing_df = items_df[items_df['mapping_id'].isin(missing_skus)][['mapping_id', 'item_description']].drop_duplicates()
                            st.dataframe(missing_df)
                            
                            st.stop() # Stops the script so the database insert never happens

                        bulk_upload_invoices(company_id, start_date, end_date, headers_df, items_df)
                        # st.dataframe(headers_df, use_container_width=True)
                    st.success(f"✅ Successfully Uploading Invoices!")
                    st.cache_data.clear()

            except Exception as e:
                st.error(f"Error reading file: {e}")