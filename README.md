# Weather Data Imputation Tool 🔧☁️

Alat otomatisasi untuk mengisi data cuaca yang hilang (Temperature, Tekanan, Curah Hujan, Suhu) menggunakan Python

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Pandas](https://img.shields.io/badge/Pandas-1.0%2B-orange)
![License](https://img.shields.io/badge/License-MIT-green)

## Fitur Utama 🚀
- Imputasi data hilang untuk parameter cuaca:
  - **Temperature**: Interpolasi waktu (time-series)
  - **Tekanan**: Forward fill
  - **Curah Hujan**: Rata-rata bulanan
  - **Suhu**: Interpolasi linear
- Input/output format CSV
- Penanganan otomatis format tanggal
- Laporan hasil pemrosesan
- Error handling lengkap
