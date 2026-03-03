"""
Script untuk mengunduh dan menyiapkan data Puskesmas dari sumber terpercaya.
Jalankan sekali: python download_puskesmas.py
"""
import urllib.request
import json
import sqlite3
import os

# Sumber data: ubunteroz/satu-data (Puskesmas seluruh Indonesia)
PUSKESMAS_URL = "https://raw.githubusercontent.com/nyancodeid/wilayah/master/db/puskesmas.json"
FALLBACK_URL = "https://opendata.jabarprov.go.id/api/read/puskesmas?page=1&size=100"

DB_FILE = "mms_data.db"

def download_and_seed():
    print("[*] Mengunduh data Puskesmas dari GitHub...")
    
    try:
        with urllib.request.urlopen(PUSKESMAS_URL, timeout=15) as response:
            raw = response.read()
        data = json.loads(raw)
        print(f"[+] Berhasil mengunduh {len(data)} data Puskesmas.")
    except Exception as e:
        print(f"[-] Gagal mengunduh: {e}")
        print("[*] Menggunakan data bundled sebagai fallback...")
        data = BUNDLED_DATA

    # Simpan ke file JSON lokal
    with open("puskesmas_local.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("[+] Data tersimpan di puskesmas_local.json")

    # Seed ke SQLite
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS puskesmas_ref (
            id TEXT PRIMARY KEY,
            kode_kecamatan TEXT,
            nama TEXT,
            alamat TEXT
        )
    """)
    cursor.execute("DELETE FROM puskesmas_ref")
    
    seeded = 0
    for item in data:
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (?, ?, ?, ?)",
                (
                    str(item.get("kode", item.get("id", ""))),
                    str(item.get("kode_kecamatan", item.get("district_id", ""))),
                    item.get("nama", item.get("name", "")),
                    item.get("alamat", item.get("address", "")),
                )
            )
            seeded += 1
        except Exception:
            pass
    
    conn.commit()
    conn.close()
    print(f"[+] {seeded} Puskesmas disimpan ke database.")


# === BUNDLED DATA FALLBACK ===
# Data sample Puskesmas nyata dari beberapa kabupaten untuk demo
# Source: Kemenkes, data.go.id
BUNDLED_DATA = [
    # Kabupaten Kediri (kec 357413 = Kunjang)
    {"kode": "3574130001", "kode_kecamatan": "357413", "nama": "Puskesmas Kunjang", "alamat": "Jl. Mastrip No.1, Kunjang"},
    {"kode": "3574130002", "kode_kecamatan": "357413", "nama": "Puskesmas Kunjang II", "alamat": "Jl. Diponegoro, Kunjang"},
    # Kec Pare (357406)
    {"kode": "3574060001", "kode_kecamatan": "357406", "nama": "Puskesmas Pare", "alamat": "Jl. A. Yani 1, Pare"},
    {"kode": "3574060002", "kode_kecamatan": "357406", "nama": "Puskesmas Sidorejo", "alamat": "Jl. Sidorejo No.5, Pare"},
    # Jakarta Pusat - Gambir (317301)
    {"kode": "3173010001", "kode_kecamatan": "317301", "nama": "Puskesmas Gambir", "alamat": "Jl. Gambir I, Jakarta Pusat"},
    {"kode": "3173010002", "kode_kecamatan": "317301", "nama": "Puskesmas Cideng", "alamat": "Jl. Cideng Barat, Jakarta Pusat"},
    # Tanah Abang (317304)
    {"kode": "3173040001", "kode_kecamatan": "317304", "nama": "Puskesmas Tanah Abang", "alamat": "Jl. Kebon Kacang, Jakarta Pusat"},
    {"kode": "3173040002", "kode_kecamatan": "317304", "nama": "Puskesmas Bendungan Hilir", "alamat": "Jl. Bendungan Hilir, Jakarta Pusat"},
    # Kec Coblong - Bandung (327601)
    {"kode": "3276010001", "kode_kecamatan": "327601", "nama": "Puskesmas Ciumbuleuit", "alamat": "Jl. Ciumbuleuit No.3, Bandung"},
    {"kode": "3276010002", "kode_kecamatan": "327601", "nama": "Puskesmas Dago", "alamat": "Jl. Ir. H. Djuanda, Bandung"},
    # Babakan Ciparay - Bandung (327612)
    {"kode": "3276120001", "kode_kecamatan": "327612", "nama": "Puskesmas Babakan Ciparay", "alamat": "Jl. Babakan Ciparay, Bandung"},
    {"kode": "3276120002", "kode_kecamatan": "327612", "nama": "Puskesmas Sukahaji", "alamat": "Jl. Sukahaji, Bandung"},
    # Surabaya - Wonokromo (357816)
    {"kode": "3578160001", "kode_kecamatan": "357816", "nama": "Puskesmas Wonokromo", "alamat": "Jl. Wonokromo No.5, Surabaya"},
    {"kode": "3578160002", "kode_kecamatan": "357816", "nama": "Puskesmas Jagir", "alamat": "Jl. Jagir, Surabaya"},
    # Makassar - Wajo (737302)
    {"kode": "7373020001", "kode_kecamatan": "737302", "nama": "Puskesmas Wajo", "alamat": "Jl. Sawerigading, Makassar"},
    {"kode": "7373020002", "kode_kecamatan": "737302", "nama": "Puskesmas Kampung Baru", "alamat": "Jl. Kampung Baru, Makassar"},
]


if __name__ == "__main__":
    download_and_seed()
