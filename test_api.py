"""Test SATUSEHAT API credentials and Puskesmas lookup."""
import requests

CLIENT_ID     = "pRCiTAb3U1mSOoNmMN6PBzqG7QaXsBPrzcW4p724tT2l28xR"
CLIENT_SECRET = "ca9UDpnheZU53OyIiAqnIz0eIsRtO7MBoPrGz5IApks9aNWAarcc5PFZQ4n5eXVh"

AUTH_URL = "https://api-satusehat-stg.dto.kemkes.go.id/oauth2/v1/accesstoken?grant_type=client_credentials"
FHIR_URL = "https://api-satusehat-stg.dto.kemkes.go.id/fhir-r4/v1"

print("=== Testing SATUSEHAT Credentials ===")
resp = requests.post(
    AUTH_URL,
    data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=15
)
print(f"Auth Status: {resp.status_code}")
if resp.status_code == 200:
    token_data = resp.json()
    token = token_data["access_token"]
    print(f"Token: {token[:30]}... (OK)")
    print(f"Expires in: {token_data.get('expires_in')}s")

    print("\n=== Querying Organization (Search by Name: Puskesmas Kunjang) ===")
    r2 = requests.get(
        f"{FHIR_URL}/Organization",
        params={"name": "Puskesmas Kunjang", "_count": 5},
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    print(f"Status: {r2.status_code}")
    if r2.status_code == 200:
        data = r2.json()
        entries = data.get("entry", [])
        print(f"Found {len(entries)} entries")
        for e in entries:
            res = e.get("resource", {})
            print(f" - {res.get('name')} (ID: {res.get('id')})")
    
    print("\n=== Listing first 10 Puskesmas in Sandbox ===")
    r4 = requests.get(
        f"{FHIR_URL}/Organization",
        params={"type": "dept", "_count": 10}, # 'dept' is often used for Fasyankes departments/clinics
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    print(f"Status: {r4.status_code}")
    if r4.status_code == 200:
        data = r4.json()
        for e in data.get("entry", []):
            res = e.get("resource", {})
            print(f" - {res.get('name')} (ID: {res.get('id')})")
else:
    print(f"Auth Error: {resp.text[:400]}")
