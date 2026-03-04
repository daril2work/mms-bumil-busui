"""
Skrip migrasi data dari SQLite (lokal) ke PostgreSQL (Neon).
Jalankan SEKALI setelah DATABASE_URL diisi di file .env.

Cara penggunaan:
    python migrate_to_neon.py
"""
import sqlite3
import os
import sys

# ================================
# LOAD .env
# ================================
def load_env(path=".env"):
    env = {}
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()
    return env

_env = load_env()
DATABASE_URL = _env.get("DATABASE_URL", "").strip() or os.environ.get("DATABASE_URL", "").strip()
SQLITE_FILE  = "mms_data.db"

if not DATABASE_URL:
    print("❌ DATABASE_URL tidak ditemukan di .env")
    print("   Isi dulu DATABASE_URL dengan connection string dari Neon.")
    sys.exit(1)

if not os.path.exists(SQLITE_FILE):
    print(f"❌ File SQLite '{SQLITE_FILE}' tidak ditemukan.")
    print("   Jalankan dulu appnya sekali di mode SQLite agar file DB terbentuk.")
    sys.exit(1)

# ================================
# CONNECT
# ================================
import psycopg2

print(f"🔌 Menghubungkan ke Neon PostgreSQL...")
pg_conn = psycopg2.connect(DATABASE_URL, sslmode='require')
pg_cur  = pg_conn.cursor()

sq_conn = sqlite3.connect(SQLITE_FILE)
sq_cur  = sq_conn.cursor()

print("✅ Koneksi berhasil!\n")

# ================================
# CREATE TABLES (idempotent)
# ================================
print("📐 Membuat tabel di PostgreSQL (jika belum ada)...")
pg_cur.execute('''
    CREATE TABLE IF NOT EXISTS mms_records (
        id SERIAL PRIMARY KEY,
        reporter_name TEXT,
        kabupaten TEXT,
        kecamatan TEXT,
        puskesmas TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    )
''')
pg_cur.execute('''
    CREATE TABLE IF NOT EXISTS mms_batches (
        id SERIAL PRIMARY KEY,
        submission_id INTEGER REFERENCES mms_records(id) ON DELETE CASCADE,
        jumlah_botol INTEGER DEFAULT 0 CHECK (jumlah_botol >= 0),
        jumlah_tab INTEGER DEFAULT 0,
        tgl_kadaluarsa DATE
    )
''')
pg_cur.execute("CREATE INDEX IF NOT EXISTS idx_mms_records_kab ON mms_records(kabupaten)")
pg_cur.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_ed  ON mms_batches(tgl_kadaluarsa)")
pg_cur.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_sub ON mms_batches(submission_id)")
pg_conn.commit()
print("✅ Tabel siap.\n")

# ================================
# MIGRATE mms_records
# ================================
sq_cur.execute("SELECT id, reporter_name, kabupaten, kecamatan, puskesmas, created_at FROM mms_records ORDER BY id")
records = sq_cur.fetchall()
print(f"📦 Memigrasikan {len(records)} baris dari mms_records...")

id_map = {}  # old SQLite id → new PostgreSQL id
for row in records:
    old_id, reporter_name, kabupaten, kecamatan, puskesmas, created_at = row
    pg_cur.execute(
        "INSERT INTO mms_records (reporter_name, kabupaten, kecamatan, puskesmas, created_at) VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (reporter_name, kabupaten, kecamatan, puskesmas, created_at)
    )
    new_id = pg_cur.fetchone()[0]
    id_map[old_id] = new_id

pg_conn.commit()
print(f"✅ mms_records: {len(id_map)} baris berhasil dipindahkan.\n")

# ================================
# MIGRATE mms_batches
# ================================
sq_cur.execute("SELECT submission_id, jumlah_botol, jumlah_tab, tgl_kadaluarsa FROM mms_batches ORDER BY id")
batches = sq_cur.fetchall()
print(f"📦 Memigrasikan {len(batches)} baris dari mms_batches...")

migrated_batches = 0
skipped_batches  = 0
for row in batches:
    old_sub_id, jumlah_botol, jumlah_tab, tgl_kadaluarsa = row
    new_sub_id = id_map.get(old_sub_id)
    if new_sub_id is None:
        print(f"  ⚠️  submission_id {old_sub_id} tidak ditemukan di peta ID, dilewati.")
        skipped_batches += 1
        continue
    pg_cur.execute(
        "INSERT INTO mms_batches (submission_id, jumlah_botol, jumlah_tab, tgl_kadaluarsa) VALUES (%s, %s, %s, %s)",
        (new_sub_id, jumlah_botol or 0, jumlah_tab or 0, tgl_kadaluarsa)
    )
    migrated_batches += 1

pg_conn.commit()
print(f"✅ mms_batches: {migrated_batches} dipindahkan, {skipped_batches} dilewati.\n")

# ================================
# DONE
# ================================
sq_conn.close()
pg_conn.close()
print("🎉 Migrasi selesai! Database PostgreSQL di Neon siap digunakan.")
print("   Sekarang isi DATABASE_URL di .env, restart app, dan test endpoint.")
