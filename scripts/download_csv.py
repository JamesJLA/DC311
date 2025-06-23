import requests
import pandas as pd

def download_arcgis_csv(out_file="dc_311_requests.csv"):
    try:
        print("⬇️ Downloading data as GeoJSON from ArcGIS...")
        url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/FeatureServer/18/query"
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()

        geojson = response.json()
        features = geojson.get("features", [])

        if not features:
            print("⚠️ No features found in GeoJSON.")
            return False

        # Extract properties from features into a list of dicts
        data = [feature["properties"] for feature in features]

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Save to CSV
        df.to_csv(out_file, index=False)
        print(f"✅ Downloaded and saved to {out_file}")
        return True

    except Exception as e:
        print(f"❌ Failed to download data: {type(e).__name__}: {e}")
        return False
