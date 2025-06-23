import pandas as pd
from sqlalchemy import create_engine

def load_dc311_data(csv_file="dc_311_requests.csv"):
    try:
        df = pd.read_csv(csv_file)

        # Rename relevant columns to simpler names
        df = df.rename(columns={
            "SERVICEREQUESTID": "sr_id",
            "SERVICECODEDESCRIPTION": "request_type",
            "STATUS_CODE": "status",
            "ADDDATE": "created_date",
            "RESOLUTIONDATE": "closed_date",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude"
        })

        # Trim to just needed columns
        df = df[["sr_id", "request_type", "status", "created_date", "closed_date", "latitude", "longitude"]]

        engine = create_engine("postgresql://postgres:password@localhost:5432/monitor_db")

        df.to_sql("dc_311_requests", engine, if_exists="replace", index=False)

        print("✅ Data loaded into dc_311_requests table.")
        return True
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return False

