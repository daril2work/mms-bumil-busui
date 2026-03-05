from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
import psycopg2
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
DATABASE_URL = _env.get("DATABASE_URL", "").strip() or os.environ.get("DATABASE_URL", "").strip()

if not DATABASE_URL:
    print("❌ CRITICAL ERROR: DATABASE_URL not found in .env or environment!")
    print("   Application requires Neon PostgreSQL as the Single Source of Truth.")
    import sys
    sys.exit(1)

print(f"[DB] Using Neon PostgreSQL (SSOT): {DATABASE_URL[:40]}...")

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
    """Return an open PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL, sslmode='require')

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
    """Cache SATUSEHAT results into puskesmas_ref."""
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                for r in results:
                    cursor.execute(
                        "INSERT INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                        (r["id"], district_id, r["name"], "")
                    )
            conn.commit()
    except Exception as e:
        print(f"[Seeding Error] {e}")


# ============================
# DATABASE INIT
# ============================
def init_db():
    """Initialize PostgreSQL schema for SSOT."""
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                # 1. Records Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS mms_records (
                        id SERIAL PRIMARY KEY,
                        tipe_pelapor TEXT DEFAULT 'puskesmas',
                        reporter_name TEXT,
                        kabupaten TEXT,
                        kecamatan TEXT,
                        puskesmas TEXT,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                ''')
                
                # Column check (for migrations)
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='mms_records' AND column_name='tipe_pelapor'")
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE mms_records ADD COLUMN tipe_pelapor TEXT DEFAULT 'puskesmas'")
                    
                # 2. Batches Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS mms_batches (
                        id SERIAL PRIMARY KEY,
                        submission_id INTEGER REFERENCES mms_records(id) ON DELETE CASCADE,
                        jumlah_botol INTEGER DEFAULT 0 CHECK (jumlah_botol >= 0),
                        jumlah_tab INTEGER DEFAULT 0,
                        tgl_kadaluarsa DATE
                    )
                ''')
                
                # 3. Reference Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS puskesmas_ref (
                        id TEXT PRIMARY KEY,
                        kode_kecamatan TEXT,
                        nama TEXT,
                        alamat TEXT
                    )
                ''')
                
                # Indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_records_kab ON mms_records(kabupaten)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_ed ON mms_batches(tgl_kadaluarsa)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_mms_batches_sub ON mms_batches(submission_id)")
                
                # Seed bundled data if empty
                cursor.execute("SELECT COUNT(*) FROM puskesmas_ref")
                if cursor.fetchone()[0] == 0:
                    for item in BUNDLED_PUSKESMAS:
                        cursor.execute(
                            "INSERT INTO puskesmas_ref (id, kode_kecamatan, nama, alamat) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                            item
                        )
            conn.commit()
    except Exception as e:
        print(f"[DB Init Error] {e}")

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
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, nama FROM puskesmas_ref WHERE kode_kecamatan = %s ORDER BY nama",
                    (district_id,)
                )
                rows = cursor.fetchall()
    except Exception as e:
        print(f"[Puskesmas API Error] {e}")
        rows = []

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
async def get_dashboard_summary(page: int = 1, page_size: int = 10, kabupaten: str = None):
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # 1. Basic Stats
    where_clause = ""
    params = []
    if kabupaten and kabupaten != "Semua Kabupaten":
        where_clause = " WHERE kabupaten = %s"
        params = [kabupaten]
        
    cursor.execute(f"SELECT COUNT(*) FROM mms_records{where_clause}", tuple(params))
    total_submissions = cursor.fetchone()[0]
    
    total_pages = (total_submissions + page_size - 1) // page_size if total_submissions > 0 else 1
    offset = (max(1, page) - 1) * page_size
    
    cursor.execute(f"SELECT COUNT(DISTINCT kabupaten) FROM mms_records{where_clause}", tuple(params))
    total_regencies = cursor.fetchone()[0]
    
    # 2. Regency Stats (Count per Regency)
    cursor.execute(f"""
        SELECT kabupaten, COUNT(*) 
        FROM mms_records 
        {where_clause}
        GROUP BY kabupaten 
        ORDER BY COUNT(*) DESC
    """, tuple(params))
    regency_stats = [{"kabupaten": r[0], "count": r[1]} for r in cursor.fetchall()]

    # 3. Logistics Stats (Sum botol per Regency)
    cursor.execute(f"""
        SELECT r.kabupaten, SUM(b.jumlah_botol), SUM(b.jumlah_tab)
        FROM mms_records r
        JOIN mms_batches b ON r.id = b.submission_id
        {where_clause}
        GROUP BY r.kabupaten
    """, tuple(params))
    logistics_stats = [
        {"kabupaten": r[0], "total_botol": r[1] or 0, "total_tab": r[2] or 0}
        for r in cursor.fetchall()
    ]

    # 4. Expiry Alerts (Next 6 months)
    # We combine where_clause with date filter
    expiry_where = " WHERE b.tgl_kadaluarsa <= %s"
    expiry_params = [time.strftime("%Y-%m-%d", time.localtime(time.time() + 180*24*3600))]
    if kabupaten and kabupaten != "Semua Kabupaten":
        expiry_where += " AND r.kabupaten = %s"
        expiry_params.append(kabupaten)

    cursor.execute(
        f"""
            SELECT r.puskesmas, r.kabupaten, b.jumlah_botol, b.tgl_kadaluarsa
            FROM mms_records r
            JOIN mms_batches b ON r.id = b.submission_id
            {expiry_where}
            ORDER BY b.tgl_kadaluarsa ASC
        """,
        tuple(expiry_params)
    )
    expiry_alerts = [
        {"puskesmas": r[0], "kabupaten": r[1], "jumlah_botol": r[2], "ed": str(r[3])}
        for r in cursor.fetchall()
    ]

    # 6. Expiry Aggregates (Cards)
    now_str = time.strftime("%Y-%m-%d")
    six_m_str = time.strftime("%Y-%m-%d", time.localtime(time.time() + 180*24*3600))
    twelve_m_str = time.strftime("%Y-%m-%d", time.localtime(time.time() + 365*24*3600))
    
    agg_where = ""
    agg_params = []
    if kabupaten and kabupaten != "Semua Kabupaten":
        agg_where = " JOIN mms_records r ON r.id = b.submission_id WHERE r.kabupaten = %s"
        agg_params = [kabupaten]

    # Critical
    crit_where = agg_where + (" AND " if agg_where else " WHERE ") + "b.tgl_kadaluarsa <= %s"
    cursor.execute(f"SELECT SUM(b.jumlah_botol) FROM mms_batches b {crit_where}", tuple(agg_params + [six_m_str]))
    stock_critical = cursor.fetchone()[0] or 0
    
    # Warning
    warn_where = agg_where + (" AND " if agg_where else " WHERE ") + "b.tgl_kadaluarsa > %s AND b.tgl_kadaluarsa <= %s"
    cursor.execute(f"SELECT SUM(b.jumlah_botol) FROM mms_batches b {warn_where}", tuple(agg_params + [six_m_str, twelve_m_str]))
    stock_warning = cursor.fetchone()[0] or 0
    
    # Safe
    safe_where = agg_where + (" AND " if agg_where else " WHERE ") + "b.tgl_kadaluarsa > %s"
    cursor.execute(f"SELECT SUM(b.jumlah_botol) FROM mms_batches b {safe_where}", tuple(agg_params + [twelve_m_str]))
    stock_safe = cursor.fetchone()[0] or 0

    # 5. Recent submissions (Paginated)
    sub_params = list(params) + [page_size, offset]
    cursor.execute(
        f"""
            SELECT id, reporter_name, kabupaten, kecamatan, puskesmas, created_at, tipe_pelapor 
            FROM mms_records 
            {where_clause}
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
        """,
        tuple(sub_params)
    )
    submissions = [
        {
            "id": r[0], "reporter_name": r[1], "kabupaten": r[2], 
            "kecamatan": r[3], "puskesmas": r[4], "created_at": str(r[5]),
            "tipe_pelapor": r[6]
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
        "expiry_summary": {
            "critical": stock_critical,
            "warning": stock_warning,
            "safe": stock_safe
        },
        "submissions": submissions,
        "pagination": {
            "total_records": total_submissions,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": page_size
        }
    }

@app.get("/api/export-excel")
async def export_excel(background_tasks: BackgroundTasks):
    """Export all records and batches to a flat Excel file using FileResponse."""
    import pandas as pd
    import os
    
    temp_file = f"export_{int(time.time())}.xlsx"
    print(f"[Export] Starting Excel generation to {temp_file}...")
    
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT 
                        r.id as id_laporan,
                        r.tipe_pelapor,
                        r.reporter_name,
                        r.kabupaten,
                        r.kecamatan,
                        r.puskesmas,
                        r.created_at,
                        b.jumlah_botol,
                        b.jumlah_tab,
                        b.tgl_kadaluarsa
                    FROM mms_records r
                    LEFT JOIN mms_batches b ON r.id = b.submission_id
                    ORDER BY r.created_at DESC
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=cols)
                print(f"[Export] Fetched {len(df)} rows.")
        
        # Format dates for Excel
        if not df.empty:
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
            if 'tgl_kadaluarsa' in df.columns:
                # Handle potential None or string dates efficiently
                df['tgl_kadaluarsa'] = pd.to_datetime(df['tgl_kadaluarsa'], errors='coerce').dt.strftime('%Y-%m-%d')
            
        # Create Excel using xlsxwriter for maximum reliability
        df.to_excel(temp_file, index=False, sheet_name='Backup MMS Data', engine='xlsxwriter')
        
        filename = f"backup_mms_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Add cleanup task
        def remove_temp_file(path: str):
            try:
                if os.path.exists(path):
                    os.remove(path)
                    print(f"[Export] Cleaned up {path}")
            except Exception as e:
                print(f"[Export Cleanup Error] {e}")

        background_tasks.add_task(remove_temp_file, temp_file)

        print(f"[Export] Excel generated locally. Serving via FileResponse.")
        return FileResponse(
            path=temp_file,
            filename=filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"[Export Error] {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Export failed: {str(e)}"})

@app.get("/api/tracing-data")
async def get_tracing_data(kabupaten: str = None):
    """Aggregate stock data per unit (Puskesmas/IFK) for tracing with ED flags."""
    try:
        now_str = time.strftime("%Y-%m-%d")
        six_m_str = time.strftime("%Y-%m-%d", time.localtime(time.time() + 180*24*3600))
        twelve_m_str = time.strftime("%Y-%m-%d", time.localtime(time.time() + 365*24*3600))
        
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                where_clause = ""
                params = []
                if kabupaten and kabupaten != "Semua Kabupaten":
                    where_clause = " WHERE r.kabupaten = %s"
                    params = [kabupaten]

                query = f"""
                    SELECT 
                        r.tipe_pelapor,
                        r.kabupaten,
                        r.kecamatan,
                        r.puskesmas,
                        SUM(b.jumlah_botol) as total_botol,
                        SUM(b.jumlah_tab) as total_tab,
                        MAX(r.created_at) as last_update,
                        BOOL_OR(b.tgl_kadaluarsa <= %s) as has_critical_ed,
                        BOOL_OR(b.tgl_kadaluarsa > %s AND b.tgl_kadaluarsa <= %s) as has_warning_ed
                    FROM mms_records r
                    JOIN mms_batches b ON r.id = b.submission_id
                    {where_clause}
                    GROUP BY r.tipe_pelapor, r.kabupaten, r.kecamatan, r.puskesmas
                    ORDER BY last_update DESC
                """
                cursor.execute(query, tuple([six_m_str, six_m_str, twelve_m_str] + params))
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                
                results = []
                for r in rows:
                    item = dict(zip(cols, r))
                    item['last_update'] = str(item['last_update'])
                    results.append(item)
                
                return results
    except Exception as e:
        print(f"[Tracing API Error] {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/api/unit-batches")
async def get_unit_batches(puskesmas: str, kabupaten: str):
    """Get all batches for a specific unit (Puskesmas/IFK) within a kabupaten."""
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT b.jumlah_botol, b.jumlah_tab, b.tgl_kadaluarsa
                    FROM mms_batches b
                    JOIN mms_records r ON b.submission_id = r.id
                    WHERE r.puskesmas = %s AND r.kabupaten = %s
                    ORDER BY b.tgl_kadaluarsa ASC
                """
                cursor.execute(query, (puskesmas, kabupaten))
                rows = cursor.fetchall()
                return [
                    {"jumlah_botol": r[0], "jumlah_tab": r[1], "tgl_kadaluarsa": str(r[2])}
                    for r in rows
                ]
    except Exception as e:
        print(f"[Unit Batches Error] {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.post("/submit")
async def submit_form(
    tipe_pelapor: str = Form("puskesmas"),
    reporter_name: str = Form(...),
    kabupaten: str = Form(...),
    kecamatan: str = Form(None),
    puskesmas: str = Form(None),
    batches_json: str = Form(...)
):
    # Input validation
    reporter_name = reporter_name.strip()
    puskesmas = (puskesmas or "").strip()
    kecamatan = (kecamatan or "").strip()
    
    # Validation logic based on type
    if tipe_pelapor == "puskesmas":
        if not reporter_name or not kabupaten or not kecamatan or not puskesmas:
            return {"status": "error", "message": "Semua field Puskesmas wajib diisi."}
    else: # IFK
        if not reporter_name or not kabupaten:
            return {"status": "error", "message": "Nama Pelapor dan Kabupaten wajib diisi."}
        # Set N/A for IFK
        kecamatan = "IFK/Kota"
        puskesmas = f"IFK {kabupaten}"

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
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO mms_records (tipe_pelapor, reporter_name, kabupaten, kecamatan, puskesmas) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (tipe_pelapor, reporter_name, kabupaten, kecamatan, puskesmas)
                )
                submission_id = cursor.fetchone()[0]

                for batch in batches:
                    cursor.execute(
                        "INSERT INTO mms_batches (submission_id, jumlah_botol, jumlah_tab, tgl_kadaluarsa) VALUES (%s, %s, %s, %s)",
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
