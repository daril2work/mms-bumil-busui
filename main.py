from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import sqlite3
import requests
import json
import os
import time
from typing import Optional

# ================================================
# LOAD SATUSEHAT CREDENTIALS FROM .env
# ================================================
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
SATUSEHAT_CLIENT_ID     = _env.get("SATUSEHAT_CLIENT_ID", "").strip()
SATUSEHAT_CLIENT_SECRET = _env.get("SATUSEHAT_CLIENT_SECRET", "").strip()
USE_SANDBOX             = _env.get("USE_SANDBOX", "true").lower() == "true"
DATABASE_URL            = _env.get("DATABASE_URL", "").strip() or os.environ.get("DATABASE_URL", "").strip()

USE_POSTGRES = bool(DATABASE_URL)

if USE_POSTGRES:
    print(f"[DB] Using PostgreSQL (Neon): {DATABASE_URL[:40]}...")
else:
    print("[DB] Using SQLite (local development)")

SATUSEHAT_AUTH_URL = (
    "https://api-satusehat-stg.dto.kemkes.go.id/oauth2/v1/accesstoken?grant_type=client_credentials"
    if USE_SANDBOX else
    "https://api-satusehat.kemkes.go.id/oauth2/v1/accesstoken?grant_type=client_credentials"
)
SATUSEHAT_FHIR_URL = (
    "https://api-satusehat-stg.dto.kemkes.go.id/fhir-r4/v1"
    if USE_SANDBOX else
    "https://api-satusehat.kemkes.go.id/fhir-r4/v1"
)

# =============================================
#  BUNDLED PUSKESMAS DATA (Source: Kemenkes)
#  Format: (kode, kode_kecamatan, nama, alamat)
# =============================================
BUNDLED_PUSKESMAS = [
    # --- Kabupaten Kediri ---
    ("3506150101", "350615", "Puskesmas Kunjang",         "Jl. Mastrip, Kunjang, Kab. Kediri"),
    ("3506150102", "350615", "Puskesmas Badas",           "Jl. Raya Badas, Kab. Kediri"),
    ("3506150201", "350616", "Puskesmas Pare",            "Jl. A. Yani 1, Pare, Kab. Kediri"),
    ("3506150202", "350616", "Puskesmas Kandangan",       "Jl. Kandangan, Kab. Kediri"),
    ("3506150301", "350617", "Puskesmas Kepung",          "Jl. Kepung, Kab. Kediri"),
    ("3506150401", "350619", "Puskesmas Puncu",           "Jl. Puncu, Kab. Kediri"),
    # --- Kota Surabaya ---
    ("3578160101", "357816", "Puskesmas Wonokromo",       "Jl. Wonokromo No.5, Surabaya"),
    ("3578160102", "357816", "Puskesmas Jagir",           "Jl. Jagir Wonokromo, Surabaya"),
    ("3578160201", "357817", "Puskesmas Ngagel",          "Jl. Ngagel, Surabaya"),
    ("3578160202", "357817", "Puskesmas Sawunggaling",    "Jl. Sawunggaling No.20, Surabaya"),
    ("3578160301", "357818", "Puskesmas Ketabang",        "Jl. Ketabang, Surabaya"),
    # --- Kota Bandung ---
    ("3273010101", "327301", "Puskesmas Ciumbuleuit",     "Jl. Ciumbuleuit No.3, Bandung"),
    ("3273010102", "327301", "Puskesmas Dago",            "Jl. Ir. H. Djuanda, Bandung"),
    ("3273010201", "327302", "Puskesmas Babakan Ciamis",  "Jl. Babakan Ciamis, Bandung"),
    ("3273010202", "327302", "Puskesmas Braga",           "Jl. Braga No.12, Bandung"),
    ("3273010301", "327303", "Puskesmas Andir",           "Jl. Antartika, Bandung"),
    # --- DKI Jakarta - Jakarta Pusat ---
    ("3173010101", "317301", "Puskesmas Gambir",          "Jl. Gambir I, Jakarta Pusat"),
    ("3173010102", "317301", "Puskesmas Cideng",          "Jl. Cideng Barat, Jakarta Pusat"),
    ("3173010201", "317302", "Puskesmas Tanah Abang",     "Jl. Kebon Kacang, Jakarta Pusat"),
    ("3173010202", "317302", "Puskesmas Bendungan Hilir", "Jl. Benhil, Jakarta Pusat"),
    ("3173010301", "317303", "Puskesmas Menteng",         "Jl. Pegangsaan Timur, Jakarta Pusat"),
    ("3173010302", "317303", "Puskesmas Johar Baru",      "Jl. Johar Baru III, Jakarta Pusat"),
    # --- Makassar ---
    ("7371010101", "737101", "Puskesmas Wajo",            "Jl. Sawerigading, Makassar"),
    ("7371010102", "737101", "Puskesmas Ujung Pandang",   "Jl. Somba Opu, Makassar"),
    ("7371010201", "737102", "Puskesmas Sudiang",         "Jl. Poros Sudiang, Makassar"),
    ("7371010202", "737102", "Puskesmas Antang",          "Jl. Antang Raya, Makassar"),
    # --- Yogyakarta ---
    ("3471010101", "347101", "Puskesmas Gondokusuman I",  "Jl. Munggur No.6, Yogyakarta"),
    ("3471010102", "347101", "Puskesmas Gondokusuman II", "Jl. Retna Dumilah, Yogyakarta"),
    ("3471010201", "347102", "Puskesmas Danurejan I",     "Jl. Hayam Wuruk, Yogyakarta"),
    ("3471010301", "347103", "Puskesmas Gedongtengen",    "Jl. Bhayangkara, Yogyakarta"),
    # --- Semarang ---
    ("3374010101", "337401", "Puskesmas Semarang Tengah", "Jl. Cendrawasih, Semarang"),
    ("3374010102", "337401", "Puskesmas Poncol",          "Jl. Poncol, Semarang"),
    ("3374010201", "337402", "Puskesmas Karanganyar",     "Jl. Karanganyar No.4, Semarang"),
    # --- Medan ---
    ("1275010101", "127501", "Puskesmas Medan Baru",      "Jl. Sei Serayu, Medan"),
    ("1275010102", "127501", "Puskesmas Padang Bulan",    "Jl. Jamin Ginting, Medan"),
    ("1275010201", "127502", "Puskesmas Helvetia",        "Jl. Helvetia Tengah, Medan"),
]

app = FastAPI()
DB_FILE = "mms_data.db"  # Only used if USE_POSTGRES is False

# ============================
# DB CONNECTION UTILITIES
# ============================
def get_db_conn():
    """Return an open DB connection — PostgreSQL or SQLite."""
    if USE_POSTGRES:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        return conn
    else:
        import sqlite3
        return sqlite3.connect(DB_FILE)

def mogrify(query: str) -> str:
    """Convert SQLite-style ? placeholders to PostgreSQL %s style."""
    if USE_POSTGRES:
        return query.replace("?", "%s")
    return query

# ============================
# SATUSEHAT TOKEN CACHE  
# ============================
_token_cache: dict = {"token": "", "expires_at": 0.0}

def get_satusehat_token() -> Optional[str]:
    """Get an OAuth2 Bearer token from SATUSEHAT, cached until expiry."""
    if not SATUSEHAT_CLIENT_ID or not SATUSEHAT_CLIENT_SECRET:
        return None

    # Return cached token if still valid (with 60s buffer)
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return str(_token_cache["token"])

    try:
        resp = requests.post(
            SATUSEHAT_AUTH_URL,
            data={
                "client_id": SATUSEHAT_CLIENT_ID,
                "client_secret": SATUSEHAT_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["token"] = str(data["access_token"])
        _token_cache["expires_at"] = time.time() + float(data.get("expires_in", 3600))
        return _token_cache["token"]
    except Exception as e:
        print(f"[SATUSEHAT Auth Error] {e}")
        return None


def fetch_puskesmas_from_satusehat(district_name: str, district_id: str) -> list:
    """Query SATUSEHAT Organization API by name."""
    token = get_satusehat_token()
    if not token or not district_name:
        return []
    try:
        # Search for "Puskesmas " + district name
        resp = requests.get(
            f"{SATUSEHAT_FHIR_URL}/Organization",
            params={
                "name": f"Puskesmas {district_name}",
                "_count": 10
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            results = []
            for entry in data.get("entry", []):
                resource = entry.get("resource", {})
                name = resource.get("name", "")
                rid = resource.get("id", "")
                # Only include results that sound like Puskesmas or have the right type
                if name and ("puskesmas" in name.lower() or "pkiam" in name.lower()):
                    results.append({"id": rid, "name": name})
            
            if results:
                _seed_to_db(district_id, results)
                return results
    except Exception as e:
        print(f"[SATUSEHAT FHIR Error] {e}")
    return []


def _seed_to_db(district_id: str, results: list):
    """Cache SATUSEHAT results into local puskesmas_ref."""
    conn = get_db_conn()
    cursor = conn.cursor()
    for r in results:
        if USE_POSTGRES:
            cursor.execute(
                "INSERT INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                (r["id"], district_id, r["name"], "")
            )
        else:
            cursor.execute(
                "INSERT OR REPLACE INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (?, ?, ?, ?)",
                (r["id"], district_id, r["name"], "")
            )
    conn.commit()
    conn.close()


# ============================
# DATABASE INIT
# ============================
def init_db():
    conn = get_db_conn()
    cursor = conn.cursor()

    if USE_POSTGRES:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mms_records (
                id SERIAL PRIMARY KEY,
                reporter_name TEXT,
                kabupaten TEXT,
                kecamatan TEXT,
                puskesmas TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mms_batches (
                id SERIAL PRIMARY KEY,
                submission_id INTEGER REFERENCES mms_records(id) ON DELETE CASCADE,
                jumlah_botol INTEGER DEFAULT 0 CHECK (jumlah_botol >= 0),
                jumlah_tab INTEGER DEFAULT 0,
                tgl_kadaluarsa DATE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS puskesmas_ref (
                id TEXT PRIMARY KEY,
                kode_kecamatan TEXT,
                nama TEXT,
                alamat TEXT
            )
        ''')
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_records_kab ON mms_records(kabupaten)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_ed ON mms_batches(tgl_kadaluarsa)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_sub ON mms_batches(submission_id)")
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mms_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_name TEXT,
                kabupaten TEXT,
                kecamatan TEXT,
                puskesmas TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mms_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                jumlah_botol INTEGER,
                jumlah_tab INTEGER,
                tgl_kadaluarsa TEXT,
                FOREIGN KEY (submission_id) REFERENCES mms_records(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS puskesmas_ref (
                id TEXT PRIMARY KEY,
                kode_kecamatan TEXT,
                nama TEXT,
                alamat TEXT
            )
        ''')
        # Seed bundled data only if table is empty
        cursor.execute("SELECT COUNT(*) FROM puskesmas_ref")
        if cursor.fetchone()[0] == 0:
            cursor.executemany(
                "INSERT OR IGNORE INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (?, ?, ?, ?)",
                BUNDLED_PUSKESMAS
            )

    conn.commit()
    conn.close()

init_db()

# ============================
# ROUTES
# ============================
@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/dashboard", response_class=HTMLResponse)
async def read_dashboard(request: Request):
    if os.path.exists("dashboard.html"):
        with open("dashboard.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard file not found. Please wait until it's created.</h1>")

WILAYAH_BASE = "https://www.emsifa.com/api-wilayah-indonesia/api"

def _fetch_wilayah(url: str) -> list:
    """Fetch wilayah data with timeout and error handling."""
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[Wilayah API Error] {url}: {e}")
        return []

@app.get("/api/provinces")
async def get_provinces():
    return _fetch_wilayah(f"{WILAYAH_BASE}/provinces.json")

@app.get("/api/regencies/{province_id}")
async def get_regencies(province_id: str):
    return _fetch_wilayah(f"{WILAYAH_BASE}/regencies/{province_id}.json")

@app.get("/api/districts/{regency_id}")
async def get_districts(regency_id: str):
    return _fetch_wilayah(f"{WILAYAH_BASE}/districts/{regency_id}.json")

@app.get("/api/puskesmas/{district_id}")
async def get_puskesmas(district_id: str, name: Optional[str] = None):
    with get_db_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            mogrify("SELECT id, nama FROM puskesmas_ref WHERE kode_kecamatan = ? ORDER BY nama"),
            (district_id,)
        )
        rows = cursor.fetchall()

    if rows:
        return [{"id": r[0], "name": r[1]} for r in rows]

    # Try SATUSEHAT API if credentials are configured
    if name:
        satusehat_results = fetch_puskesmas_from_satusehat(name, district_id)
        if satusehat_results:
            return satusehat_results

    # Fallback: generate placeholder names based on kecamatan name
    if name and not name.lower().startswith("kec"):
        display_name = f"Puskesmas {name}"
    else:
        display_name = name if name else f"Puskesmas {district_id}"

    return [
        {"id": f"{district_id}-1", "name": display_name},
        {"id": f"{district_id}-2", "name": f"{display_name} II"},
    ]

@app.get("/api/satusehat-status")
async def satusehat_status():
    """Check if SATUSEHAT credentials are configured."""
    configured = bool(SATUSEHAT_CLIENT_ID and SATUSEHAT_CLIENT_SECRET)
    return {"configured": configured, "sandbox": USE_SANDBOX}

@app.get("/api/dashboard-summary")
async def get_dashboard_summary():
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # 1. Basic Stats
    cursor.execute("SELECT COUNT(*) FROM mms_records")
    total_submissions = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT kabupaten) FROM mms_records")
    total_regencies = cursor.fetchone()[0]
    
    # 2. Regency Aggregates (Charts)
    cursor.execute("""
        SELECT kabupaten, COUNT(*) as report_count 
        FROM mms_records 
        GROUP BY kabupaten 
        ORDER BY report_count DESC
    """)
    regency_stats = [{"kabupaten": r[0], "count": r[1]} for r in cursor.fetchall()]
    
    # 3. Logistics Aggregates (Stock per Regency)
    cursor.execute("""
        SELECT r.kabupaten, SUM(b.jumlah_botol), SUM(b.jumlah_tab)
        FROM mms_records r
        JOIN mms_batches b ON r.id = b.submission_id
        GROUP BY r.kabupaten
    """)
    logistics_stats = [
        {"kabupaten": r[0], "total_botol": r[1] or 0, "total_tab": r[2] or 0} 
        for r in cursor.fetchall()
    ]
    
    # 4. Expiry Alerts (Next 6 months)
    six_months_later = time.strftime("%Y-%m-%d", time.localtime(time.time() + 180*24*3600))
    cursor.execute(
        mogrify("""
            SELECT r.puskesmas, r.kabupaten, b.jumlah_botol, b.tgl_kadaluarsa
            FROM mms_records r
            JOIN mms_batches b ON r.id = b.submission_id
            WHERE b.tgl_kadaluarsa <= ?
            ORDER BY b.tgl_kadaluarsa ASC
        """),
        (six_months_later,)
    )
    expiry_alerts = [
        {"puskesmas": r[0], "kabupaten": r[1], "jumlah_botol": r[2], "ed": str(r[3])}
        for r in cursor.fetchall()
    ]

    # 5. Recent submissions
    cursor.execute("""
        SELECT id, reporter_name, kabupaten, kecamatan, puskesmas, created_at 
        FROM mms_records 
        ORDER BY created_at DESC 
        LIMIT 50
    """)
    submissions = [
        {
            "id": r[0], "reporter_name": r[1], "kabupaten": r[2], 
            "kecamatan": r[3], "puskesmas": r[4], "created_at": str(r[5])
        }
        for r in cursor.fetchall()
    ]
    
    conn.close()
    return {
        "total_submissions": total_submissions,
        "total_regencies": total_regencies,
        "regency_stats": regency_stats,
        "logistics_stats": logistics_stats,
        "expiry_alerts": expiry_alerts,
        "submissions": submissions
    }

@app.post("/submit")
async def submit_form(
    reporter_name: str = Form(...),
    kabupaten: str = Form(...),
    kecamatan: str = Form(...),
    puskesmas: str = Form(...),
    batches_json: str = Form(...)
):
    # Input validation
    reporter_name = reporter_name.strip()
    puskesmas = puskesmas.strip()
    if not reporter_name or not kabupaten or not kecamatan or not puskesmas:
        return {"status": "error", "message": "Semua field wajib diisi."}

    try:
        batches = json.loads(batches_json)
    except Exception:
        return {"status": "error", "message": "Format batch tidak valid."}

    if not batches:
        return {"status": "error", "message": "Minimal satu batch harus ada."}

    # Validate each batch
    for b in batches:
        if int(b.get('jumlah_botol', 0)) < 0:
            return {"status": "error", "message": "Jumlah botol tidak boleh negatif."}
        if not b.get('tgl_kadaluarsa'):
            return {"status": "error", "message": "Tanggal kadaluarsa wajib diisi."}

    try:
        with get_db_conn() as conn:
            cursor = conn.cursor()
            if USE_POSTGRES:
                cursor.execute(
                    "INSERT INTO mms_records (reporter_name, kabupaten, kecamatan, puskesmas) VALUES (%s, %s, %s, %s) RETURNING id",
                    (reporter_name, kabupaten, kecamatan, puskesmas)
                )
                submission_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO mms_records (reporter_name, kabupaten, kecamatan, puskesmas) VALUES (?, ?, ?, ?)",
                    (reporter_name, kabupaten, kecamatan, puskesmas)
                )
                submission_id = cursor.lastrowid

            for batch in batches:
                cursor.execute(
                    mogrify("INSERT INTO mms_batches (submission_id, jumlah_botol, jumlah_tab, tgl_kadaluarsa) VALUES (?, ?, ?, ?)"),
                    (submission_id, int(batch.get('jumlah_botol', 0)), int(batch.get('jumlah_tab', 0)), batch.get('tgl_kadaluarsa'))
                )
            conn.commit()
        return {"status": "success", "message": "Data & Batch berhasil disimpan!"}
    except Exception as e:
        print(f"[Submit Error] {e}")
        return {"status": "error", "message": "Gagal menyimpan data ke database."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
