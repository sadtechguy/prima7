import pandas as pd
import datetime as dt

from database import get_date_boundaries


def get_kpi_summary(df):
    """Menghitung metrik utama untuk Baris KPI"""
    total_amount = df['amount'].sum()
    total_qty = df['quantity'].sum()
    
    unique_invoices = df['invoice_id'].nunique()
    aov = total_amount / unique_invoices if unique_invoices > 0 else 0
    

    unique_outlets = df['Name'].nunique()
    ros = total_amount / unique_outlets if unique_outlets > 0 else 0

    product_breadth = df['bm_id'].nunique()
    
    return total_amount, total_qty, aov, unique_outlets, ros, product_breadth


def prepare_map_data(df):
    """Merangkum data mentah (Line Items) menjadi 1 baris per pelanggan untuk Peta"""
    customer_columns = ['Name', 'Address', 'latitude', 'longitude', 'type_id']
    
    final_df = df.groupby(customer_columns, as_index=False).agg({
        'quantity': 'sum', 
        'amount': 'sum',   
        'salesman': lambda x: ', '.join(x.dropna().unique()), 
        'bm_id': lambda x: ', '.join(x.dropna().unique())
    })
    return final_df


def calculate_rfm(df):
    """Menghitung skor RFM menggunakan metode Net Revenue (Cara Profesional)"""
    if df.empty:
        return pd.DataFrame() # Kembalikan dataframe kosong jika tidak ada data

    latest_date = df['invoice_date'].max()

    # 1. Hitung R dan F HANYA dari transaksi yang bukan retur
    df_real_orders = df[df['amount'] > 0]
    rf = df_real_orders.groupby('Name').agg({
        'invoice_date': lambda x: (latest_date - x.max()).days,
        'invoice_id': 'nunique'
    }).reset_index()

    # 2. Hitung M dari SELURUH transaksi
    m = df.groupby('Name').agg({'amount': 'sum'}).reset_index()

    # Gabungkan dan rapikan
    rfm = pd.merge(rf, m, on='Name', how='left')
    rfm.rename(columns={'invoice_date': 'Recency', 'invoice_id': 'Frequency', 'amount': 'Monetary'}, inplace=True)
    rfm = rfm[rfm['Monetary'] > 0].copy()

    # 3. Buat Segmentasi
    if len(rfm) >= 4: # Mencegah error qcut jika data pelanggan terlalu sedikit
        rfm['R_Score'] = pd.qcut(rfm['Recency'].rank(method='first'), 4, labels=[4, 3, 2, 1])
        rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 4, labels=[1, 2, 3, 4])
        rfm['M_Score'] = pd.qcut(rfm['Monetary'].rank(method='first'), 4, labels=[1, 2, 3, 4])
        rfm['RFM_Segment'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)
        
        # Fungsi label
        def classify_customer(code):
            r, f = int(code[0]), int(code[1])
            if r >= 3 and f >= 3: return "🌟 Champions"
            elif r >= 3 and f <= 2: return "👋 Pelanggan Baru"
            elif r <= 2 and f >= 3: return "⚠️ At Risk"
            elif r <= 2 and f <= 2: return "💤 Pasif"
            else: return "🤝 Reguler"
            
        rfm['Customer_Class'] = rfm['RFM_Segment'].apply(classify_customer)
    
    return rfm

# --- IN data_processing.py ---


def get_default_date_range():
    """
    Calculates:
    - min_date: The oldest invoice in the database
    - end_date: The latest invoice in the database
    - start_date: The Monday of the week of the latest invoice (Week-to-Date)
    """
    oldest_date, newest_date = get_date_boundaries()
    
    # Find Monday of that week
    # If newest_date is Wednesday (weekday = 2), we subtract 2 days to get Monday!
    days_since_monday = newest_date.weekday() 
    default_start = newest_date - dt.timedelta(days=days_since_monday)
    
    # Just in case the database is super new and Monday is BEFORE the oldest record
    default_start = max(default_start, oldest_date)

    return default_start, newest_date, oldest_date
