
import requests
import pandas as pd
import time
import os

from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

RAW_DATA_DIR = Path("data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

COMMODITIES = {
    "DCOILBRENTEU": "Brent Crude Oil",
    "DCOILWTICO": "WTI Crude Oil",
    "DHHNGSP": "Henry Hub Natural Gas"
}

HEADERS = {
    "User-Agent": "CommodityDataQualityPipeline/1.0"
}

MAX_RETRIES = 3
REQUEST_TIMEOUT = (30, 180)
RECORD_LIMIT = 100000


# --------------------------------------------------
# 2. API REQUEST FUNCTION
# --------------------------------------------------

def fetch_observations(series_id):

    all_observations = []
    offset = 0

    while True:

        params = {
            "series_id": series_id,
            "api_key": API_KEY,
            "file_type": "json",
            "limit": RECORD_LIMIT,
            "offset": offset
        }

        response_data = None

        # Automatic retry mechanism

        for attempt in range(1, MAX_RETRIES + 1):

            try:

                print(
                    f"Fetching records | "
                    f"Offset: {offset} | "
                    f"Attempt: {attempt}/{MAX_RETRIES}"
                )

                response = requests.get(
                    BASE_URL,
                    params=params,
                    headers=HEADERS,
                    timeout=REQUEST_TIMEOUT
                )

                response.raise_for_status()

                response_data = response.json()

                # Check for FRED API errors

                if "error_code" in response_data:

                    raise ValueError(
                        response_data.get(
                            "error_message",
                            "Unknown FRED API error"
                        )
                    )

                break

            except requests.exceptions.RequestException as error:

                print(f"Request failed: {error}")

                if attempt == MAX_RETRIES:
                    raise

                time.sleep(5 * attempt)

        if response_data is None:

            raise RuntimeError(
                f"Unable to retrieve data for {series_id}"
            )

        observations = response_data.get(
            "observations", []
        )

        all_observations.extend(observations)

        print(
            f"Records received in this request: "
            f"{len(observations)}"
        )

        # Stop when all observations have been retrieved

        if len(observations) < RECORD_LIMIT:
            break

        offset += RECORD_LIMIT

    return all_observations


# --------------------------------------------------
# 3. DOWNLOAD AND SAVE COMMODITY DATA
# --------------------------------------------------

def download_commodity_data(series_id, commodity_name):

    print("\n" + "=" * 55)
    print(f"Downloading: {commodity_name}")
    print("=" * 55)

    try:

        # Fetch historical observations

        observations = fetch_observations(series_id)

        if not observations:

            raise ValueError(
                f"No observations received for {series_id}"
            )

        # Convert JSON response into DataFrame

        df = pd.DataFrame(observations)

        # Select and standardize columns

        df = df[["date", "value"]]

        df = df.rename(
            columns={
                "date": "observation_date",
                "value": series_id
            }
        )

        # Preserve raw values, including "." markers

        df["observation_date"] = (
            df["observation_date"].astype(str)
        )

        df[series_id] = (
            df[series_id].astype(str)
        )

        # Save CSV file

        file_path = RAW_DATA_DIR / f"{series_id}.csv"

        df.to_csv(
            file_path,
            index=False
        )

        # --------------------------------------------------
        # DOWNLOAD SUMMARY
        # --------------------------------------------------

        print("\nDownload successful!")

        print(f"Commodity: {commodity_name}")
        print(f"Series ID: {series_id}")
        print(f"Total records: {len(df)}")
        print(f"Saved to: {file_path}")

        # --------------------------------------------------
        # MISSING VALUE CHECK
        # --------------------------------------------------

        missing_count = (
            df[series_id].str.strip() == "."
        ).sum()

        print(
            f"Missing-value markers: {missing_count}"
        )

        # --------------------------------------------------
        # DATE RANGE
        # --------------------------------------------------

        dates = pd.to_datetime(
            df["observation_date"],
            errors="coerce"
        )

        print(f"Start date: {dates.min()}")
        print(f"End date: {dates.max()}")

        # --------------------------------------------------
        # DISPLAY SAMPLE RECORDS
        # --------------------------------------------------

        print("\nFirst five records:")

        print(df.head().to_string(index=False))

        return True

    except (
        requests.exceptions.RequestException,
        ValueError,
        KeyError,
        OSError,
        RuntimeError
    ) as error:

        print(
            f"\nError downloading {commodity_name}: {error}"
        )

        return False


# --------------------------------------------------
# 4. MAIN EXECUTION
# --------------------------------------------------

def main():

    print("=" * 60)
    print("COMMODITY DATA COLLECTION PIPELINE")
    print("Source: FRED API")
    print("=" * 60)

    if not API_KEY:

        raise SystemExit(
            "FRED API key not found. "
            "Check your .env file."
        )

    start_time = datetime.now()

    successful_downloads = 0
    failed_downloads = []

    # Download all commodity datasets

    for series_id, commodity_name in COMMODITIES.items():

        success = download_commodity_data(
            series_id,
            commodity_name
        )

        if success:

            successful_downloads += 1

        else:

            failed_downloads.append(commodity_name)

    # --------------------------------------------------
    # FINAL SUMMARY
    # --------------------------------------------------

    end_time = datetime.now()

    execution_time = (
        end_time - start_time
    ).total_seconds()

    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)

    print(
        f"Successful downloads: "
        f"{successful_downloads}/{len(COMMODITIES)}"
    )

    print(
        f"Failed downloads: "
        f"{len(failed_downloads)}"
    )

    if failed_downloads:

        print("\nFailed commodities:")

        for commodity in failed_downloads:
            print(f"- {commodity}")

    print(
        f"\nExecution time: {execution_time:.2f} seconds"
    )

    print(f"Completed at: {end_time}")

    if successful_downloads == len(COMMODITIES):

        print(
            "\nAll commodity datasets downloaded successfully!"
        )

    else:

        raise SystemExit(
            "\nPipeline failed. Check the errors above."
        )


# --------------------------------------------------
# 5. RUN PIPELINE
# --------------------------------------------------

if __name__ == "__main__":

    main()
