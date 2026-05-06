import folium
from branca.element import Template, MacroElement
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
        "🌟 VIP / Sultan": "orange",
        "🏆 Loyal (Low Spend)": "lightblue",
        "👋 Pelanggan Baru": "green",
        "🚨 Urgent Win-Back!": "red",
        "⚠️ At Risk": "lightred",
        "💤 Pasif": "gray",
        "🤝 Reguler": "blue",
        "Belum Ada Data": "pink"     # Abu-abu jika data tidak cukup
    }

    # 3. Gambar titik-titiknya
    for index, row in map_data.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            
            # Ambil status RFM pelanggan ini, lalu cari warnanya di kamus
            kategori = str(row.get('Customer_Class', 'Belum Ada Data'))
            pin_color = color_map.get(kategori, 'gray')

            # ==========================================
            # 1. PENGAMBILAN DATA YANG AMAN (ANTI-ERROR)
            # Gunakan .get() untuk mencegah KeyError. 
            # Jika kolom tidak ada, beri nilai default 0.
            # ==========================================
            wine = float(row.get('amount_WIN1', 0)) if pd.notna(row.get('amount_WIN1', 0)) else 0
            spi1 = float(row.get('amount_SPI1', 0)) if pd.notna(row.get('amount_SPI1', 0)) else 0
            spi2 = float(row.get('amount_SPI2', 0)) if pd.notna(row.get('amount_SPI2', 0)) else 0
            loc1 = float(row.get('amount_LOC1', 0)) if pd.notna(row.get('amount_LOC1', 0)) else 0
            total = float(row.get('amount', 0)) if pd.notna(row.get('amount', 0)) else 0

            # ==========================================
            # 2. BUAT RINCIAN HANYA JIKA ADA PENJUALAN (> 0)
            # ==========================================
            details_html = ""
            
            if wine > 0:
                details_html += f"<tr><td>Wine:</td><td style='text-align:right'>Rp {wine:,.0f}</td></tr>".replace(',', '.')
            if spi1 > 0:
                details_html += f"<tr><td>Spirit (Principal):</td><td style='text-align:right'>Rp {spi1:,.0f}</td></tr>".replace(',', '.')
            if spi2 > 0:
                details_html += f"<tr><td>Spirit (Independent):</td><td style='text-align:right'>Rp {spi2:,.0f}</td></tr>".replace(',', '.')
            if loc1 > 0:
                details_html += f"<tr><td>Lokal:</td><td style='text-align:right'>Rp {loc1:,.0f}</td></tr>".replace(',', '.')
                
            # Total keseluruhan
            total_str = f"Rp {total:,.0f}".replace(',', '.')

            # ==========================================
            # 3. SUSUN POP-UP DENGAN TABEL MINI AGAR COMPACT
            # ==========================================
            html_content = f"""
            <div style="font-size: 13px; font-family: Arial, sans-serif; min-width: 180px;">
                <b style="font-size: 15px;">{row['Name']}</b><br>
                <span style="color: {pin_color}; font-weight: bold;">{kategori}</span>
                <hr style="margin: 5px 0; border: 0; border-top: 1px solid #ccc;">
                
                <table style="width: 100%; color: #555; border-collapse: collapse;">
                    {details_html}
                    <tr><td colspan="2"><hr style="margin: 3px 0; border: 0; border-top: 1px dashed #ccc;"></td></tr>
                    <tr style="font-weight: bold; color: #000;">
                        <td>Total:</td>
                        <td style='text-align:right'>{total_str}</td>
                    </tr>
                </table>
            </div>
            """
            custom_popup = folium.Popup(html_content, max_width=300, min_width=200)

            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=custom_popup,
                icon=folium.Icon(color=pin_color, icon="info-sign")
            ).add_to(m)

            # ==========================================
            # 4. MEMBUAT LEGENDA MENGAMBANG (HTML/CSS)
            # ==========================================
            legend_html = '''
            {% macro html(this, kwargs) %}
            <div style="
                position: absolute;
                bottom: 30px; /* Jarak dari bawah */
                left: 30px;   /* Jarak dari kiri */
                z-index: 9999;
                background-color: rgba(255, 255, 255, 0.9); /* Putih dengan sedikit transparansi */
                color: #333333;
                padding: 15px;
                border: 2px solid grey;
                border-radius: 8px;
                font-family: Arial, sans-serif;
                font-size: 14px;
                box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
            ">
                <h4 style="margin-top: 0; margin-bottom: 10px;">Status Pelanggan</h4>
                <i style="background: orange; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> VIP / Sultan<br>
                <i style="background: lightblue; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Loyal (Low Spend)<br>
                <i style="background: green; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Pelanggan Baru<br>
                <i style="background: red; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Urgent Win-Back!<br>
                <i style="background: #FF8C8C; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> At Risk<br>
                <i style="background: gray; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Pasif<br>
                <i style="background: blue; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Reguler<br>
                <i style="background: pink; width: 12px; height: 12px; float: left; margin-right: 8px; margin-top: 3px; border-radius: 50%;"></i> Belum Ada Data
            </div>
            {% endmacro %}
            '''
            
            # Masukkan legenda ke dalam objek peta
            macro = MacroElement()
            macro._template = Template(legend_html)
            m.get_root().add_child(macro)
            
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


