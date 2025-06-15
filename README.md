# Proyek ETL Sederhana untuk Data Produk Fashion

Repositori ini berisi sebuah proyek pipeline ETL (Extract, Transform, Load) sederhana yang dibangun dengan Python. Pipeline ini dirancang untuk mengekstrak data produk fashion dari sebuah situs web, membersihkan dan mentransformasikannya, lalu memuatnya ke berbagai tujuan data seperti CSV, Google Sheets, dan database PostgreSQL.

## Alur Proses ETL

Pipeline ini mengikuti tiga langkah utama:

### 1\. Extract (Ekstraksi)

Proses ekstraksi mengambil data mentah dari situs web demo fashion, `https://fashion-studio.dicoding.dev`.

  * **Sumber Data**: Data produk di-scrape dari total 50 halaman web.
  * **Teknologi**: Menggunakan library `requests` untuk mengambil konten HTML dan `BeautifulSoup` untuk parsing.
  * **Data yang Diekstrak**: Informasi yang diambil untuk setiap produk meliputi:
      * Judul (Title)
      * Harga (Price)
      * Peringkat (Rating)
      * Jumlah Warna (Colors)
      * Ukuran (Size)
      * Gender
      * Timestamp ekstraksi

### 2\. Transform (Transformasi)

Data mentah yang telah diekstrak kemudian dibersihkan dan diproses untuk memastikan kualitas dan konsistensi.

  * **Pembersihan Data**: Baris dengan data yang tidak valid atau kosong (seperti "N/A" atau "Unknown Product") akan dihapus.
  * **Transformasi Kolom**:
      * `price`: String harga (contoh: "$99.99") dibersihkan dari simbol non-numerik, diubah menjadi tipe data float, dan dikonversi ke Rupiah (IDR) dengan kurs $1 = Rp16.000.
      * `rating`: String peringkat (contoh: "4.5 / 5") diekstrak untuk mendapatkan nilai float-nya (contoh: 4.5).
      * `colors`: String (contoh: "3 Colors") diekstrak untuk mendapatkan jumlah warnanya dalam bentuk integer.
      * `size` dan `gender`: Awalan seperti "Size: " dan "Gender: " dihapus untuk mendapatkan nilai bersihnya.
  * **Penanganan Nilai Kosong**: Setelah transformasi, baris-baris yang masih memiliki nilai kosong pada kolom-kolom kunci akan dihapus untuk memastikan integritas data.

### 3\. Load (Pemuatan)

Data yang sudah bersih dan tertransformasi dimuat ke tiga tujuan berbeda secara bersamaan.

  * **CSV**: Disimpan sebagai file `products.csv` di direktori root.
  * **Google Sheets**: Diunggah ke Google Sheet. URL untuk melihat hasilnya dapat diakses di sini: [Google Sheets Result](https://docs.google.com/spreadsheets/d/173byRKN5zsxFwCp3-tL0W4A4t9fYqjIx0CYdVjEJirk/edit?usp=sharing).
  * **PostgreSQL**: Disimpan ke dalam sebuah tabel bernama `products` di database PostgreSQL.

## Struktur Proyek

```
.
├── .gitignore          # Mengabaikan file kredensial google-sheets-api.json
├── main.py             # Skrip utama untuk menjalankan pipeline ETL
├── requirements.txt    # Daftar dependensi Python yang dibutuhkan
├── submission.txt      # Berisi instruksi untuk menjalankan proyek
├── tests/              # Direktori berisi unit tests untuk setiap modul
│   ├── test_extract.py   # Test untuk modul ekstraksi
│   ├── test_transform.py # Test untuk modul transformasi
│   └── test_load.py      # Test untuk modul pemuatan
└── utils/              # Direktori berisi logika inti ETL
    ├── extract.py      # Modul untuk proses ekstraksi data
    ├── transform.py    # Modul untuk proses transformasi data
    └── load.py         # Modul untuk proses pemuatan data
```

## Prasyarat

  * Python 3.8+
  * `pip` (Python package installer)
  * Akses ke server PostgreSQL (jika ingin menggunakan destinasi PostgreSQL)

## Instalasi & Konfigurasi

1.  **Clone Repositori**

    ```bash
    git clone <URL_REPOSITORI_ANDA>
    cd <NAMA_DIREKTORI_REPOSITORI>
    ```

2.  **Buat Virtual Environment (Direkomendasikan)**

    ```bash
    python -m venv venv
    source venv/bin/activate  # Untuk Windows: venv\Scripts\activate
    ```

3.  **Instal Dependensi**
    Instal semua library yang dibutuhkan dari file `requirements.txt`.

    ```bash
    pip install -r requirements.txt
    ```

4.  **Konfigurasi Lingkungan**

      * **PostgreSQL**:
        Pastikan server PostgreSQL Anda berjalan. Skrip ini akan mencoba terhubung menggunakan kredensial berikut (dapat diubah di `main.py`):

          * **Host**: `localhost`
          * **Database**: `fashion_data`
          * **User**: `etl_user`
          * **Password**: `irfn321`
          * **Port**: `5432`

        Anda bisa membuat user dan database yang sesuai dengan perintah SQL berikut:

        ```sql
        CREATE USER etl_user WITH PASSWORD 'irfn321';
        CREATE DATABASE fashion_data OWNER etl_user;
        ```

      * **Google Sheets**:

        1.  Aktifkan Google Drive API dan Google Sheets API di Google Cloud Console.
        2.  Buat sebuah *Service Account* dan unduh file kredensial dalam format JSON.
        3.  Ganti nama file JSON tersebut menjadi `google-sheets-api.json` dan letakkan di direktori root proyek. File ini sudah ditambahkan ke `.gitignore` untuk keamanan.
        4.  Buka file `google-sheets-api.json`, salin alamat email `client_email`, dan bagikan Google Sheet tujuan Anda dengan email tersebut (berikan akses Editor).

## Cara Menjalankan

### Menjalankan Pipeline ETL Utama

Untuk menjalankan keseluruhan proses ETL dari ekstraksi hingga pemuatan.

```bash
python main.py
```

Log proses akan ditampilkan di konsol dan juga disimpan dalam file `etl_log_YYYYMMDD_HHMMSS.log`.

### Menjalankan Unit Tests

Untuk memastikan semua fungsi berjalan sesuai harapan, jalankan unit tests menggunakan `pytest`.

  * **Menjalankan semua test:**

    ```bash
    pytest tests/
    ```

  * **Menjalankan test dengan laporan cakupan (coverage report):**

    ```bash
    pytest --cov=utils tests/
    ```
