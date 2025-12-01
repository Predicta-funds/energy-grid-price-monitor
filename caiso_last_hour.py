# =============================================================================
#  CAISO Real-Time LMP (last hour) - SP15, NP15, ZP26
#  Works in 2025 - no API key required
# =============================================================================

import requests
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO
from zipfile import ZipFile
from typing import Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

CONFIG = {
    "api_base_url": "http://oasis.caiso.com/oasisapi/SingleZip",
    "nodes": {
        "TH_SP15_GEN-APND": "SP15",
        "TH_NP15_GEN-APND": "NP15",
        "TH_ZP26_GEN-APND": "ZP26"
    },
    "lookback_minutes": 70,
    "timeout": 60,
    "output_dir": "."
}

# =============================================================================
# Helper Functions
# =============================================================================

def build_oasis_url(
    query_name: str,
    start_str: str,
    end_str: str,
    extra_params: Optional[Dict[str, str]] = None
) -> str:
    """
    Build a URL for CAISO OASIS API query.

    Args:
        query_name: OASIS query name (e.g., 'PRC_INTVL_LMP')
        start_str: Start datetime in OASIS format (YYYYMMDDTHH:MM-0000)
        end_str: End datetime in OASIS format (YYYYMMDDTHH:MM-0000)
        extra_params: Additional query parameters

    Returns:
        Complete OASIS API URL
    """
    params = {
        "queryname": query_name,
        "startdatetime": start_str,
        "enddatetime": end_str,
        "version": "1",
        "resultformat": "6"  # ZIP with CSV inside
    }

    if extra_params:
        params.update(extra_params)

    param_string = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{CONFIG['api_base_url']}?{param_string}"


def fetch_oasis_data(url: str) -> pd.DataFrame:
    """
    Fetch data from CAISO OASIS API and return as DataFrame.

    Args:
        url: OASIS API URL

    Returns:
        DataFrame containing the OASIS data

    Raises:
        Exception: If API request fails or data is empty
    """
    try:
        logger.info(f"Fetching data from OASIS API...")
        response = requests.get(url, timeout=CONFIG["timeout"])

        if response.status_code != 200:
            raise Exception(
                f"OASIS API error: {response.status_code} - {response.text[:200]}"
            )

        # Decompress and read CSV from ZIP
        with ZipFile(BytesIO(response.content)) as z:
            if not z.namelist():
                raise Exception("ZIP file is empty")

            csv_name = z.namelist()[0]
            logger.info(f"Reading CSV: {csv_name}")

            with z.open(csv_name) as f:
                df = pd.read_csv(f)

        if df.empty:
            raise Exception("Downloaded data is empty")

        logger.info(f"Successfully fetched {len(df)} rows")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while fetching OASIS data: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching OASIS data: {e}")
        raise


def get_time_window() -> Tuple[datetime, datetime, str, str]:
    """
    Calculate time window for data queries.

    Returns:
        Tuple of (start_time, end_time, start_str, end_str)
        where str versions are in OASIS format (YYYYMMDDTHH:MM-0000)
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=CONFIG["lookback_minutes"])

    # Format required by OASIS: YYYYMMDDTHH:MM-0000
    start_str = start_time.strftime("%Y%m%dT%H:%M-0000")
    end_str = end_time.strftime("%Y%m%dT%H:%M-0000")

    logger.info(f"Time window: {start_time.strftime('%Y-%m-%d %H:%M')} to "
                f"{end_time.strftime('%Y-%m-%d %H:%M')} UTC")

    return start_time, end_time, start_str, end_str


# =============================================================================
# LMP Data Processing
# =============================================================================

def fetch_lmp_data(start_str: str, end_str: str) -> pd.DataFrame:
    """
    Fetch Locational Marginal Pricing (LMP) data for all hubs.

    Args:
        start_str: Start time in OASIS format
        end_str: End time in OASIS format

    Returns:
        Raw LMP DataFrame
    """
    node_ids = ",".join(CONFIG["nodes"].keys())
    url = build_oasis_url(
        "PRC_INTVL_LMP",
        start_str,
        end_str,
        {"market_run_id": "RTM", "node": node_ids}
    )
    return fetch_oasis_data(url)


def process_lmp_data(df: pd.DataFrame, start_time: datetime) -> pd.DataFrame:
    """
    Process LMP data: filter, pivot, rename columns.

    Args:
        df: Raw LMP DataFrame
        start_time: Minimum timestamp to include

    Returns:
        Processed LMP DataFrame with one row per hub per timestamp
    """
    logger.info("Processing LMP data...")

    # Convert timestamp
    df["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df["INTERVALSTARTTIME_GMT"])

    # Filter to requested time window
    df = df[df["INTERVALSTARTTIME_GMT"] >= start_time].copy()

    # Keep only relevant columns
    df = df[["INTERVALSTARTTIME_GMT", "NODE", "LMP_TYPE", "MW"]]

    # Pivot: transform from long to wide format
    # Each node-interval will have separate columns for LMP, MCC, MCE, MCL
    df = df.pivot_table(
        index=["INTERVALSTARTTIME_GMT", "NODE"],
        columns="LMP_TYPE",
        values="MW",
        aggfunc="first"
    ).reset_index()

    df.columns.name = None

    # Rename columns for clarity
    df.rename(columns={
        "INTERVALSTARTTIME_GMT": "timestamp_utc",
        "NODE": "hub",
        "LMP": "lmp_total",
        "MCC": "congestion",
        "MCE": "energy",
        "MCL": "loss"
    }, inplace=True)

    # Map hub names to readable format
    df["hub"] = df["hub"].map(CONFIG["nodes"])

    # Sort by timestamp and hub
    df = df.sort_values(["timestamp_utc", "hub"]).reset_index(drop=True)

    logger.info(f"Processed {len(df)} LMP records")
    return df


# =============================================================================
# Generation Data Processing
# =============================================================================

def fetch_renewable_data(start_str: str, end_str: str) -> pd.DataFrame:
    """
    Fetch renewable energy generation data (Solar, Wind).

    Args:
        start_str: Start time in OASIS format
        end_str: End time in OASIS format

    Returns:
        Raw renewable generation DataFrame
    """
    url = build_oasis_url("SLD_REN_FCST", start_str, end_str)
    return fetch_oasis_data(url)


def process_renewable_data(df: pd.DataFrame, start_time: datetime) -> pd.DataFrame:
    """
    Process renewable generation data: filter and aggregate.

    Args:
        df: Raw renewable generation DataFrame
        start_time: Minimum timestamp to include

    Returns:
        Processed renewable generation DataFrame
    """
    logger.info("Processing renewable generation data...")

    # Convert timestamp
    df["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df["INTERVALSTARTTIME_GMT"])

    # Filter to real-time dispatch (RTD) and time window
    df = df[
        (df["INTERVALSTARTTIME_GMT"] >= start_time) &
        (df["MARKET_RUN_ID"] == "RTD")
    ].copy()

    # Aggregate by renewable type and timestamp
    df_agg = df.groupby(
        ["INTERVALSTARTTIME_GMT", "RENEWABLE_TYPE"]
    )["MW"].sum().reset_index()

    # Pivot to wide format
    df_agg = df_agg.pivot(
        index="INTERVALSTARTTIME_GMT",
        columns="RENEWABLE_TYPE",
        values="MW"
    ).reset_index()

    df_agg.columns.name = None

    # Calculate total renewables
    df_agg["renewables_total"] = (
        df_agg.get("Solar", 0) + df_agg.get("Wind", 0)
    )
    df_agg = df_agg.fillna(0)

    logger.info(f"Processed {len(df_agg)} renewable generation records")
    return df_agg


def fetch_total_generation_data(start_str: str, end_str: str) -> pd.DataFrame:
    """
    Fetch total system generation data.

    Args:
        start_str: Start time in OASIS format
        end_str: End time in OASIS format

    Returns:
        Raw total generation DataFrame
    """
    url = build_oasis_url(
        "ENE_SLRS",
        start_str,
        end_str,
        {"market_run_id": "RTM"}
    )
    return fetch_oasis_data(url)


def process_total_generation_data(
    df: pd.DataFrame,
    start_time: datetime
) -> pd.DataFrame:
    """
    Process total generation data: filter and aggregate.

    Args:
        df: Raw total generation DataFrame
        start_time: Minimum timestamp to include

    Returns:
        Processed total generation DataFrame
    """
    logger.info("Processing total generation data...")

    # Convert timestamp
    df["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df["INTERVALSTARTTIME_GMT"])

    # Filter to CAISO total and time window
    df = df[
        (df["INTERVALSTARTTIME_GMT"] >= start_time) &
        (df["TAC_ZONE_NAME"] == "Caiso_Totals") &
        (df["SLRS_TYPE"] == "ALL")
    ].copy()

    # Aggregate by timestamp
    df_agg = df.groupby("INTERVALSTARTTIME_GMT")["MW"].sum().reset_index()
    df_agg.rename(columns={"MW": "total_generation"}, inplace=True)

    logger.info(f"Processed {len(df_agg)} total generation records")
    return df_agg


def merge_generation_data(
    df_renewable: pd.DataFrame,
    df_total: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge renewable and total generation data.

    Args:
        df_renewable: Processed renewable generation DataFrame
        df_total: Processed total generation DataFrame

    Returns:
        Combined generation DataFrame with thermal/other column
    """
    logger.info("Merging generation data...")

    df_merged = df_renewable.merge(
        df_total,
        on="INTERVALSTARTTIME_GMT",
        how="outer"
    )

    # Rename timestamp
    df_merged.rename(
        columns={"INTERVALSTARTTIME_GMT": "timestamp_utc"},
        inplace=True
    )

    # Calculate thermal/other as difference
    df_merged["thermal_and_other"] = (
        df_merged.get("total_generation", 0) -
        df_merged.get("renewables_total", 0)
    )

    logger.info(f"Merged {len(df_merged)} generation records")
    return df_merged


# =============================================================================
# Data Merging
# =============================================================================

def merge_lmp_with_generation(
    df_lmp: pd.DataFrame,
    df_generation: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge LMP data with generation data on timestamp.

    Args:
        df_lmp: Processed LMP DataFrame
        df_generation: Processed generation DataFrame

    Returns:
        Tuple of (combined_df, lmp_aggregated_df)
        - combined_df: LMP averaged across hubs merged with generation
        - lmp_aggregated_df: LMP aggregated by timestamp (for reference)
    """
    logger.info("Merging LMP with generation data...")

    # Aggregate LMP by timestamp (average across hubs)
    df_lmp_agg = df_lmp[[
        "timestamp_utc", "lmp_total", "congestion", "energy", "loss"
    ]].groupby("timestamp_utc").mean().reset_index()

    # Merge with generation data
    df_combined = df_lmp_agg.merge(
        df_generation,
        on="timestamp_utc",
        how="inner"
    )

    # Sort and reset index
    df_combined = df_combined.sort_values("timestamp_utc").reset_index(drop=True)

    # Rename timestamp for clarity
    df_combined.rename(
        columns={"timestamp_utc": "timestamp_utc_interval"},
        inplace=True
    )

    logger.info(f"Combined {len(df_combined)} records")
    return df_combined, df_lmp_agg


# =============================================================================
# Output
# =============================================================================

def save_results(
    df_combined: pd.DataFrame,
    df_lmp: pd.DataFrame,
    timestamp: datetime
) -> Tuple[str, str]:
    """
    Save results to CSV files.

    Args:
        df_combined: Combined LMP and generation DataFrame
        df_lmp: Raw LMP DataFrame with hub-level detail
        timestamp: Current timestamp for filename

    Returns:
        Tuple of (combined_filename, lmp_filename)
    """
    logger.info("Saving results...")

    # Save combined data
    combined_filename = (
        f"{CONFIG['output_dir']}/caiso_lmp_generation_"
        f"{timestamp.strftime('%Y%m%d_%H%M')}UTC.csv"
    )
    df_combined.to_csv(combined_filename, index=False)
    logger.info(f"Saved: {combined_filename}")

    # Save raw LMP data (legacy format)
    lmp_filename = (
        f"{CONFIG['output_dir']}/caiso_lmp_last_hour_"
        f"{timestamp.strftime('%Y%m%d_%H%M')}UTC.csv"
    )
    df_lmp.to_csv(lmp_filename, index=False)
    logger.info(f"Saved: {lmp_filename}")

    return combined_filename, lmp_filename


def print_results(
    df_lmp_agg: pd.DataFrame,
    df_combined: pd.DataFrame,
    combined_filename: str,
    lmp_filename: str
) -> None:
    """
    Print summary of results.

    Args:
        df_lmp_agg: Aggregated LMP DataFrame
        df_combined: Combined LMP and generation DataFrame
        combined_filename: Path to combined output file
        lmp_filename: Path to LMP output file
    """
    print("\n" + "="*70)
    print("LATEST LMP DATA (Average Hub)")
    print("="*70)
    print(df_lmp_agg.tail())

    print("\n" + "="*70)
    print("LATEST GENERATION DATA")
    print("="*70)
    print(df_combined.tail())

    print("\n" + "="*70)
    print("FILES SAVED")
    print("="*70)
    print(f"Combined (LMP + Generation): {combined_filename}")
    print(f"LMP Only (Legacy):           {lmp_filename}")
    print("="*70 + "\n")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    """
    Main orchestration function: fetch, process, and save energy data.
    """
    try:
        logger.info("Starting CAISO data collection...")

        # Get time window
        start_time, end_time, start_str, end_str = get_time_window()

        # Fetch and process LMP data
        logger.info("\n--- Fetching LMP Data ---")
        df_lmp_raw = fetch_lmp_data(start_str, end_str)
        df_lmp = process_lmp_data(df_lmp_raw, start_time)

        # Fetch and process generation data
        logger.info("\n--- Fetching Generation Data ---")
        df_renewable_raw = fetch_renewable_data(start_str, end_str)
        df_renewable = process_renewable_data(df_renewable_raw, start_time)

        df_total_raw = fetch_total_generation_data(start_str, end_str)
        df_total = process_total_generation_data(df_total_raw, start_time)

        df_generation = merge_generation_data(df_renewable, df_total)

        # Merge LMP with generation data
        logger.info("\n--- Merging Data ---")
        df_combined, df_lmp_agg = merge_lmp_with_generation(df_lmp, df_generation)

        # Save results
        logger.info("\n--- Saving Results ---")
        current_timestamp = datetime.now(timezone.utc)
        combined_filename, lmp_filename = save_results(
            df_combined,
            df_lmp,
            current_timestamp
        )

        # Print summary
        print_results(df_lmp_agg, df_combined, combined_filename, lmp_filename)

        logger.info("CAISO data collection completed successfully!")

    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
