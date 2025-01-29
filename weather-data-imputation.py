import pandas as pd
import numpy as np
from datetime import datetime

def load_weather_data(file_path):
    """
    Memuat data cuaca dari file CSV
    """
    df = pd.read_csv(file_path)
    
    # Konversi kolom tanggal ke format datetime
    if 'Tanggal' in df.columns:
        df['Tanggal'] = pd.to_datetime(df['Tanggal'])
        df.set_index('Tanggal', inplace=True)
    
    return df

def handle_missing_values(df):
    """
    Menangani data yang hilang dengan berbagai metode imputasi
    """
    # Salin dataframe asli untuk mempertahankan data original
    processed_df = df.copy()
    
    # Imputasi untuk Temperatur (interpolasi linear)
    if 'Temperature' in processed_df.columns:
        processed_df['Temperature'] = processed_df['Temperature'].interpolate(method='time')
    
    # Imputasi untuk Tekanan (forward fill)
    if 'Tekanan' in processed_df.columns:
        processed_df['Tekanan'] = processed_df['Tekanan'].fillna(method='ffill')
    
    # Imputasi untuk Curah Hujan (mean per bulan)
    if 'Curah_Hujan' in processed_df.columns:
        processed_df['Curah_Hujan'] = processed_df.groupby(processed_df.index.month)['Curah_Hujan']\
                                            .transform(lambda x: x.fillna(x.mean()))
    
    # Imputasi untuk Suhu (sama dengan Temperature, mungkin kolom duplikat)
    # Jika ini kolom berbeda, bisa menggunakan metode lain
    if 'Suhu' in processed_df.columns:
        processed_df['Suhu'] = processed_df['Suhu'].interpolate(method='linear')
    
    return processed_df

def save_processed_data(df, file_path):
    """
    Menyimpan data yang telah diproses ke file CSV baru
    """
    df.to_csv(file_path)
    print(f"Data berhasil disimpan di: {file_path}")

def main():
    # Input file path
    input_file = 'data_cuaca.csv'
    output_file = 'data_cuaca_processed.csv'
    
    try:
        # Memuat data
        weather_df = load_weather_data(input_file)
        
        # Menangani missing values
        processed_df = handle_missing_values(weather_df)
        
        # Menyimpan data yang telah diproses
        save_processed_data(processed_df, output_file)
        
        # Menampilkan perbandingan data sebelum dan sesudah
        print("\nContoh data sebelum pemrosesan:")
        print(weather_df.head(10))
        print("\nContoh data setelah pemrosesan:")
        print(processed_df.head(10))
        
    except FileNotFoundError:
        print(f"File {input_file} tidak ditemukan!")
    except Exception as e:
        print(f"Terjadi kesalahan: {str(e)}")

if __name__ == "__main__":
    main()
