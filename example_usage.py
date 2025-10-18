#!/usr/bin/env python3
"""
Example usage of the Regina Parking Data Downloader

This script demonstrates how to use the download_parking_data.py module
to download and combine parking ticket data from Regina's open data portal.
"""

from download_parking_data import ReginaParkingDataDownloader
import pandas as pd


def main():
    """Example of how to use the parking data downloader."""

    # Initialize the downloader
    downloader = ReginaParkingDataDownloader(output_dir="regina_parking_data")

    # Download and combine the data
    success = downloader.download_and_combine()

    if success:
        print("✅ Data successfully downloaded and combined!")

        # Load the combined data for analysis
        combined_file = downloader.output_dir / "combined_parking_tickets.csv"
        df = pd.read_csv(combined_file)

        print(f"\n📊 Dataset Overview:")
        print(f"Total records: {len(df):,}")
        print(f"Columns: {list(df.columns)}")

        # Show first few rows
        print(f"\n📋 First 5 rows:")
        print(df.head())

        # Basic statistics if date column exists
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            print(f"\n📅 Date range: {df['Date'].min()} to {df['Date'].max()}")

        # Top infractions if infraction column exists
        if "Infraction" in df.columns:
            print(f"\n� Top 5 infractions:")
            print(df["Infraction"].value_counts().head())

        # Top locations if location column exists
        if "Location" in df.columns:
            print(f"\n📍 Top 5 locations:")
            print(df["Location"].value_counts().head())

    else:
        print("❌ Failed to download data. Check the logs for details.")


if __name__ == "__main__":
    main()
