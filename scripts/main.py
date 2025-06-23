from download_csv import download_arcgis_csv
from load_311_data import load_dc311_data
from monitor_freshness import check_data_freshness

if __name__ == "__main__":
    print("\n🚀 DC311 Monitor Pipeline Starting...\n")

    success = download_arcgis_csv()
    print(f"Download success? {success}")

    if not success:
        print("🚫 Download failed, exiting.")
    else:
        success = load_dc311_data()
        print(f"Load success? {success}")

        if not success:
            print("🚫 Load failed, stopping pipeline.")
        else:
            freshness_success = check_data_freshness()
            print(f"Freshness check success? {freshness_success}")

    print("\n✅ Pipeline complete.\n")
