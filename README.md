# Instalasi dan Running Project Bot Auto Trading

Dokumen ini menjelaskan tahapan instalasi dari awal sampai selesai untuk menjalankan project ini di Windows.

## 1. Persyaratan awal

Pastikan perangkat Anda sudah memiliki:

- Python 3.10 atau lebih tinggi
- Git
- VS Code (disarankan)
- Akun Telegram yang akan dipakai sebagai userbot
- Kunci API Telegram: API ID dan API HASH
- Kunci API Gemini: GEMINI_API_KEY
- MetaTrader 5 terinstal dan sudah login ke akun trading
- Akses ke folder MetaTrader 5 Files untuk menyimpan output

> Catatan: project ini terdiri dari 2 komponen utama:
> - Telegram signal reader untuk membaca sinyal dari grup/channel Telegram
> - Dashboard untuk memantau akun/posisi MetaTrader 5 lewat browser

---

## 2. Buka project di VS Code

1. Buka folder project:
   - E:\brutalx\python\bot-auto-trading
2. Buka terminal PowerShell di folder tersebut.

---

## 3. Buat virtual environment

Jalankan perintah berikut:

```powershell
python -m venv venv
```

Aktifkan virtual environment:

```powershell
.
\venv\Scripts\Activate.ps1
```

Jika PowerShell memblokir aktivasi, jalankan:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Lalu aktifkan lagi:

```powershell
.
\venv\Scripts\Activate.ps1
```

---

## 4. Siapkan file .env

Buat file bernama `.env` di folder project. Isi dengan konfigurasi berikut:

```env
API_ID=isi_api_id_telegram_anda
API_HASH=isi_api_hash_telegram_anda
GEMINI_API_KEY=isi_kunci_gemini_anda

# Optional untuk dashboard MT5
MT5_LOGIN=
MT5_PASSWORD=
MT5_SERVER=
SYMBOLS=XAUUSD,EURUSD
REFRESH_RATE_SECONDS=1
HISTORY_DAYS=1
HOST=0.0.0.0
PORT=8000
```

### Penjelasan singkat

- `API_ID` dan `API_HASH`: didapat dari my.telegram.org
- `GEMINI_API_KEY`: didapat dari Google AI Studio
- `MT5_*`: isi bila Anda ingin dashboard login otomatis ke MT5
- Jika dikosongkan, dashboard akan memakai terminal MT5 yang sudah login manual

---

## 5. Pastikan folder output MT5 tersedia

Project ini menulis output ke folder MetaTrader 5 Files. Pastikan path berikut tersedia dan bisa ditulis:

```text
C:\Users\brutalx\AppData\Roaming\MetaQuotes\Terminal\D0E8209F77C8CF37AD8BF550E51FF075\MQL5\Files
```

Kalau folder berbeda, sesuaikan `OUTPUT_DIR` di file [telegram_signal_reader.py](telegram_signal_reader.py).

---

## 6. Jalankan Telegram signal reader

Jalankan perintah berikut:

```powershell
python telegram_signal_reader.py
```

### Yang akan terjadi

- Program akan mencoba menghubungkan akun Telegram Anda
- Anda akan diminta login lewat nomor Telegram dan kode verifikasi
- Setelah berhasil, bot akan mulai mendengarkan grup/channel

### Output yang dihasilkan

- File log: `messages_log.jsonl`
- File order JSON: `orders.json`

Keduanya akan tersimpan di folder MT5 Files.

---

## 7. Jalankan dashboard monitoring

Buka terminal baru dan jalankan:

```powershell
python super_dashboard.py
```

Setelah server berjalan, buka browser ke alamat:

```text
http://localhost:8000
```

Anda akan melihat dashboard monitoring akun, posisi, dan histori trading.

---

## 8. Uji alur kerja

### Test Telegram reader

1. Kirim pesan ke grup/channel yang dipantau
2. Pastikan pesan mengandung kata kunci seperti:
   - `buy`
   - `sell`
   - `sl`
   - `tp`
3. Bot akan memproses pesan dan menulis hasil ke file JSON

### Test dashboard

1. Pastikan MetaTrader 5 terhubung
2. Buka halaman dashboard di browser
3. Cek apakah data akun dan posisi muncul

---

## 9. Troubleshooting umum

### 1. Error `API_ID dan API_HASH wajib diisi`

Artinya file `.env` belum diisi dengan benar.

### 2. Error `GEMINI_API_KEY wajib diisi`

Tambahkan `GEMINI_API_KEY` ke file `.env`.

### 3. Telegram tidak bisa login

- Pastikan nomor Telegram aktif
- Pastikan kode verifikasi sudah masuk
- Pastikan API ID dan API HASH valid

### 4. `google-genai` tidak bisa diinstall pada Python 3.8.10

- `google-genai` versi terbaru membutuhkan Python 3.10+.
- Jika Anda perlu menggunakan Telegram signal reader di server ini, jalankan Python 3.10/3.11 di lingkungan terpisah.
- Alternatif lain: jalankan bot pada komputer lain atau container yang mendukung Python 3.10+.

### 5. Dashboard tidak terhubung ke MT5

- Pastikan MetaTrader 5 sudah terbuka dan login
- Pastikan `MT5_LOGIN`, `MT5_PASSWORD`, dan `MT5_SERVER` benar jika ingin login otomatis
- Cek apakah terminal MT5 sudah berjalan dengan akun yang valid

### 5. Tidak ada file orders.json muncul

- Pastikan pesan Telegram yang dikirim memang terdeteksi sebagai sinyal
- Pastikan kata kunci `buy/sell` dan `sl/tp` ada di teks

---

## 10. Selesai

Jika semua langkah di atas berjalan dengan baik, berarti instalasi sudah selesai dan sistem Anda siap digunakan.

Anda sudah bisa:

- menerima sinyal dari Telegram
- mengubahnya menjadi format order JSON
- memantau akun dan posisi melalui dashboard

Jika ingin, langkah berikutnya bisa ditambahkan berupa:

- membuat file `requirements.txt`
- menambahkan auto-start saat Windows boot
- membuat service agar Telegram bot dan dashboard jalan otomatis di background
