import requests
import os
import json

BASE_URL = "https://api.data.gov.in/resource/"
API_KEY = os.getenv("DATA_GOV_IN_API_KEY", "579b464db66ec23bdd000001d324dad9e6ae4f9e679106fdc61fa35b")

DATASETS = {
    "rainfall": "8e0bd482-4aba-4d99-9cb9-ff124f6f1c2f",
    "crop_production": "35be999b-0208-4354-b557-f6ca9a5355de"
}

def fetch_dataset(resource_id, name, limit=1000, max_pages=50):
    save_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{name}.json")

    if not API_KEY:
        print("❌ No DATA_GOV_IN_API_KEY set in environment. Aborting.")
        return

    offset = 0
    all_records = []
    page = 0

    while True:
        url = f"{BASE_URL}{resource_id}?api-key={API_KEY}&format=json&limit={limit}&offset={offset}"
        print(f"📡 Fetching {name} (offset={offset})")
        try:
            r = requests.get(url, timeout=30)
        except Exception as e:
            print(f"❌ Request failed: {e}")
            break

        if r.status_code != 200:
            print(f"❌ HTTP {r.status_code} fetching {name}: {r.text[:200]}")
            break

        data = r.json()
        # find list of records in common keys
        records = None
        for k in ("records", "data", "result", "results"):
            if isinstance(data, dict) and k in data and isinstance(data[k], list):
                records = data[k]
                break
        if records is None and isinstance(data, list):
            records = data

        if not records:
            print("ℹ️ No more records returned.")
            break

        all_records.extend(records)
        page += 1
        if len(records) < limit:
            break
        offset += limit
        if page >= max_pages:
            print("⚠️ Reached max_pages limit; stopping.")
            break

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(all_records)} records to {save_path}")


if __name__ == "__main__":
    for name, resource_id in DATASETS.items():
        fetch_dataset(resource_id, name)
