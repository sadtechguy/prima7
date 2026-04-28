import folium
import pydeck as pdk
import plotly.express as px
import pandas as pd

def create_customer_location_map(final_df, rfm_df):
    """Membuat peta sebaran lokasi pelanggan dengan warna berbasis RFM"""

    # Titik tengah awal (Jakarta)
    m = folium.Map(location=[-6.20, 106.77], zoom_start=11)

    # 1. Gabungkan data lokasi dengan status RFM
    # Jika rfm_df tersedia, tempelkan kolom 'Customer_Class' ke final_df
    if rfm_df is not None and not rfm_df.empty:
        map_data = pd.merge(final_df, rfm_df[['Name', 'Customer_Class']], on='Name', how='left')
    else:
        map_data = final_df.copy()
        map_data['Customer_Class'] = 'Belum Ada Data'

    # 2. Kamus Warna berdasarkan Status RFM
    color_map = {
        "🌟 Champions": "orange",      # Emas/Oranye untuk pelanggan terbaik
        "👋 Pelanggan Baru": "green",  # Hijau untuk area potensial
        "🤝 Reguler": "blue",          # Biru untuk pelanggan stabil
        "⚠️ At Risk": "red",           # Merah terang untuk yang mau lepas (Waspada!)
        "💤 Pasif": "darkred",         # Merah gelap untuk yang sudah lama tidak beli
        "Belum Ada Data": "gray"       # Abu-abu jika data tidak cukup
    }

    # 3. Gambar titik-titiknya
    for index, row in map_data.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            
            # Ambil status RFM pelanggan ini, lalu cari warnanya di kamus
            kategori = str(row.get('Customer_Class', 'Belum Ada Data'))
            pin_color = color_map.get(kategori, 'gray')

            # Format Pop-up (Kita tampilkan status RFM-nya di dalam pop-up!)
            formatted_amount = f"Rp {row['amount']:,.0f}".replace(',', '.')
            html_content = f"""
            <div style="font-size: 16px; font-family: Arial, sans-serif;">
                <b>{row['Name']}</b><br>
                <span style="color: {pin_color}; font-weight: bold;">{kategori}</span><br>
                <span style="color: #555;">Total Sales: {formatted_amount}</span>
            </div>
            """
            custom_popup = folium.Popup(html_content, max_width=300, min_width=200)

            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=custom_popup,
                icon=folium.Icon(color=pin_color, icon="info-sign")
            ).add_to(m)
            
    return m

def create_heatmap(map_df):
    """Membuat peta panas (PyDeck) berdasarkan nominal sales"""
    # Pastikan data numerik dan bersihkan baris kosong
    map_df = map_df.dropna(subset=['latitude', 'longitude']).copy()
    
    if map_df.empty:
        return None # Kembalikan None jika tidak ada koordinat

    map_df['latitude'] = map_df['latitude'].astype(float)
    map_df['longitude'] = map_df['longitude'].astype(float)
    map_df['amount'] = map_df['amount'].fillna(0).astype(float)
    
    # Skalakan angka agar grafik WebGL tidak crash (dibagi 1 Juta)
    map_df['heatmap_weight'] = map_df['amount'] / 1000000 

    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=map_df,
        get_position=["longitude", "latitude"],
        get_weight="heatmap_weight",  
        radiusPixels=60,
    )

    # Kunci kamera ke Jakarta agar tidak melayang ke laut
    view_state = pdk.ViewState(
        latitude=-6.20,
        longitude=106.77,
        zoom=10, # Zoom kecil agar seluruh Indonesia terlihat
        pitch=0,
    )

    deck = pdk.Deck(
        layers=[heatmap_layer],
        initial_view_state=view_state,
        map_style="dark", 
        tooltip={"text": "Area Penjualan"}
    )
    
    return deck

def create_product_bar_chart(df):
    """Membuat grafik batang horizontal (Plotly) untuk kontribusi produk"""
    chart_df = df.groupby('bm_id', as_index=False)[['quantity', 'amount']].sum()
    
    category_mapping = {
        'WIN1': 'Wine',
        'SPI1': 'Spirit (Principal)',
        'SPI2': 'Spirit (Independent)',
        'LOC1': 'Local'
    }
    chart_df['bm_id'] = chart_df['bm_id'].replace(category_mapping)
    
    if chart_df.empty:
        return None

    fig = px.bar(
        chart_df, 
        x='amount', 
        y='bm_id', 
        orientation='h',
        text_auto='.2s', 
        title="Revenue Contribution by Product",
        labels={'amount': 'Total Sales (Rp)', 'bm_id': 'Product Type'}
    )
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=300)
    
    return fig


