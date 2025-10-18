#!/usr/bin/env python3
"""
Regina Parking Tickets Data Downloader and Combiner

This script downloads XLS files from the Regina parking tickets dataset
and combines them into a single CSV file for analysis.

Dataset URL: https://openregina.ca/dataset/parking-services-tickets-issued-report-2025-to-2028
"""

import pandas as pd
import requests
import os
from pathlib import Path
import logging
from typing import List, Optional
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ReginaParkingDataDownloader:
    def __init__(self, output_dir: str = "data"):
        """Initialize the downloader with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Direct download URLs for XLS files (these would need to be updated with actual URLs)
        self.xls_urls = {
            "Q1_2025": "https://openregina.ca/dataset/parking-services-tickets-issued-report-2025-to-2028/resource/.../download/...",  # Q1 2025 XLS
            "Q2_2025": "https://openregina.ca/dataset/parking-services-tickets-issued-report-2025-to-2028/resource/.../download/...",  # Q2 2025 XLS
        }

        # Alternative: Use CKAN API to get actual download URLs
        self.ckan_base_url = "https://openregina.ca/api/3/action"
        self.dataset_id = "parking-services-tickets-issued-report-2025-to-2028"

    def get_dataset_resources(self) -> List[dict]:
        """Get dataset resources using CKAN API to find XLS files."""
        try:
            url = f"{self.ckan_base_url}/package_show"
            params = {"id": self.dataset_id}

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                resources = data["result"]["resources"]
                # Filter for XLS files
                xls_resources = [
                    r for r in resources if r.get("format", "").upper() == "XLS"
                ]
                logger.info(f"Found {len(xls_resources)} XLS resources")
                return xls_resources
            else:
                logger.error(f"API error: {data.get('error', 'Unknown error')}")
                return []

        except Exception as e:
            logger.error(f"Error fetching dataset resources: {e}")
            return []

    def download_file(self, url: str, filename: str) -> bool:
        """Download a file from URL to local directory."""
        try:
            logger.info(f"Downloading {filename}...")
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            file_path = self.output_dir / filename
            with open(file_path, "wb") as f:
                f.write(response.content)

            logger.info(f"Downloaded {filename} ({len(response.content)} bytes)")
            return True

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return False

    def read_xls_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Read XLS file and return DataFrame."""
        try:
            logger.info(f"Reading {file_path.name}...")
            df = pd.read_excel(file_path, engine="xlrd")
            logger.info(f"Read {len(df)} rows from {file_path.name}")
            return df
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None

    def combine_dataframes(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple DataFrames into one."""
        if not dataframes:
            logger.warning("No dataframes to combine")
            return pd.DataFrame()

        logger.info(f"Combining {len(dataframes)} dataframes...")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined dataframe has {len(combined_df)} rows")

        # Process the combined dataframe
        combined_df = self.process_dataframe(combined_df)

        return combined_df

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the dataframe: convert datetime and split location."""
        logger.info("Processing dataframe...")

        # Convert VIOLATION_DATETIME to proper datetime format
        if "VIOLATION_DATETIME" in df.columns:
            logger.info("Converting VIOLATION_DATETIME to datetime format...")
            df["VIOLATION_DATETIME"] = pd.to_datetime(
                df["VIOLATION_DATETIME"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
            )

            # Split datetime into separate date and time columns
            logger.info("Splitting datetime into VIOLATION_DATE and VIOLATION_TIME...")
            df["VIOLATION_DATE"] = df["VIOLATION_DATETIME"].dt.date
            df["VIOLATION_TIME"] = df["VIOLATION_DATETIME"].dt.time

        # Split VIOL_LOC into LOCATION and ADDRESS
        if "VIOL_LOC" in df.columns:
            logger.info("Splitting VIOL_LOC into LOCATION and ADDRESS...")
            df = self.split_location(df)

        # Clean up known typos and data issues
        df = self.clean_data(df)

        logger.info("Processing complete")
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up known typos and data quality issues."""
        logger.info("Cleaning data...")

        if "ADDRESS" in df.columns:
            # Fix typo: "Augus St" should be "Angus St"
            df["ADDRESS"] = df["ADDRESS"].str.replace(
                "Augus St,", "Angus St,", regex=False
            )
            # Fix typo: "August St" should be "Angus St"
            df["ADDRESS"] = df["ADDRESS"].str.replace(
                "August St,", "Angus St,", regex=False
            )

        return df

    def split_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split VIOL_LOC into LOCATION (direction) and ADDRESS (street address)."""
        import re

        def parse_location(viol_loc):
            """Parse the VIOL_LOC string into location and address."""
            if pd.isna(viol_loc):
                return None, None

            viol_loc_str = str(viol_loc).strip()

            # Pattern to match location prefixes like "WEST SIDE", "IN FRONT OF", "OPPOSITE", etc.
            location_patterns = [
                r"^(WEST SIDE|EAST SIDE|NORTH SIDE|SOUTH SIDE)\s+(.+)$",
                r"^(IN FRONT OF|OPPOSITE)\s+(.+)$",
            ]

            location = None
            address = None

            for pattern in location_patterns:
                match = re.match(pattern, viol_loc_str, re.IGNORECASE)
                if match:
                    location = match.group(1)
                    address = match.group(2)
                    break

            # If no pattern matched, treat entire string as address
            if location is None:
                address = viol_loc_str

            # Format location with proper capitalization
            if location:
                location = location.title()

            # Format address with proper capitalization and add Regina, Saskatchewan
            if address:
                # Capitalize first letter of each word
                address = address.title()
                # Add Regina, Saskatchewan
                address = f"{address}, Regina, Saskatchewan"

            return location, address

        # Apply the parsing function
        df[["LOCATION", "ADDRESS"]] = df["VIOL_LOC"].apply(
            lambda x: pd.Series(parse_location(x))
        )

        return df

    def download_and_combine(self) -> bool:
        """Main method to download XLS files and combine them."""
        try:
            # Get available XLS resources
            resources = self.get_dataset_resources()
            if not resources:
                logger.error("No XLS resources found")
                return False

            downloaded_files = []
            dataframes = []

            # Download each XLS file
            for i, resource in enumerate(resources):
                url = resource.get("url")
                name = resource.get("name", f"parking_data_{i+1}")

                if not url:
                    logger.warning(f"No URL found for resource: {name}")
                    continue

                filename = f"{name}.xls"
                if self.download_file(url, filename):
                    downloaded_files.append(self.output_dir / filename)

            if not downloaded_files:
                logger.error("No files were downloaded successfully")
                return False

            # Read and combine all XLS files
            for file_path in downloaded_files:
                df = self.read_xls_file(file_path)
                if df is not None:
                    dataframes.append(df)

            if not dataframes:
                logger.error("No dataframes were read successfully")
                return False

            # Combine all dataframes
            combined_df = self.combine_dataframes(dataframes)

            # Save combined data
            output_file = self.output_dir / "combined_parking_tickets.csv"
            combined_df.to_csv(output_file, index=False)
            logger.info(f"Combined data saved to {output_file}")

            # Display summary
            self.display_summary(combined_df)

            return True

        except Exception as e:
            logger.error(f"Error in download_and_combine: {e}")
            return False

    def display_summary(self, df: pd.DataFrame):
        """Display summary statistics of the combined data."""
        print("\n" + "=" * 50)
        print("PARKING TICKETS DATA SUMMARY")
        print("=" * 50)
        print(f"Total records: {len(df):,}")
        print(f"Columns: {list(df.columns)}")

        # Display date range
        if "VIOLATION_DATE" in df.columns:
            print(
                f"Date range: {df['VIOLATION_DATE'].min()} to {df['VIOLATION_DATE'].max()}"
            )

        # Display time statistics
        if "VIOLATION_TIME" in df.columns:
            # Create hour column for analysis
            df_temp = df.copy()
            df_temp["hour"] = pd.to_datetime(
                df_temp["VIOLATION_TIME"].astype(str)
            ).dt.hour
            top_hours = df_temp["hour"].value_counts().head(3)
            print(
                f"Most common violation hours: {dict(zip(top_hours.index, top_hours.values))}"
            )

        # Display location statistics
        if "LOCATION" in df.columns:
            print(f"Unique location types: {df['LOCATION'].nunique()}")
            print(f"Top locations: {df['LOCATION'].value_counts().head(3).to_dict()}")

        # Display address statistics
        if "ADDRESS" in df.columns:
            print(f"Unique addresses: {df['ADDRESS'].nunique()}")

        # Display infraction statistics
        if "INF_DESCR" in df.columns:
            print(f"Unique infractions: {df['INF_DESCR'].nunique()}")

        print("=" * 50)


def main():
    """Main function to run the downloader."""
    print("Regina Parking Tickets Data Downloader")
    print("=" * 40)

    downloader = ReginaParkingDataDownloader()

    if downloader.download_and_combine():
        print("\n✅ Data download and combination completed successfully!")
        print(f"📁 Data saved in: {downloader.output_dir.absolute()}")
    else:
        print("\n❌ Data download failed. Check the logs for details.")


if __name__ == "__main__":
    main()
