# =============================================================================
#  CAISO Real-Time LMP (last hour) - SP15, NP15, ZP26
#  Works in 2025 - no API key required
# =============================================================================

import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
from io import BytesIO
from zipfile import ZipFile

# ----------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------
# Nodes (trading hubs) of interest
nodes = "TH_SP15_GEN-APND,TH_NP15_GEN-APND,TH_ZP26_GEN-APND"

# Calculate start = 70 minutes ago (to ensure we capture the last hour)
end_time   = datetime.now(timezone.utc)
start_time = end_time - timedelta(minutes=70)

# Format required by OASIS: YYYYMMDDTHH:MM-0000 (with colons!)
start_str = start_time.strftime("%Y%m%dT%H:%M-0000")
end_str   = end_time.strftime("%Y%m%dT%H:%M-0000")

# URL OASIS (PRC_INTVL_LMP = Real-Time 5-min)
url = (
    f"http://oasis.caiso.com/oasisapi/SingleZip?"
    f"queryname=PRC_INTVL_LMP&"
    f"startdatetime={start_str}&"
    f"enddatetime={end_str}&"
    f"version=1&"
    f"market_run_id=RTM&"
    f"node={nodes}&"
    f"resultformat=6"      # 6 = ZIP with CSV inside
)

print(f"Downloading CAISO prices for last hour...")
print(f"From: {start_time.strftime('%Y-%m-%d %H:%M')} UTC")
print(f"To  : {end_time.strftime('%Y-%m-%d %H:%M')} UTC")
print(url)
print()

# ----------------------------------------------------------------------
# Download and decompress
# ----------------------------------------------------------------------
response = requests.get(url, timeout=60)
if response.status_code != 200:
    raise Exception(f"OASIS Error: {response.status_code} - {response.text[:200]}")

with ZipFile(BytesIO(response.content)) as z:
    # Inside the zip there is one CSV file
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        df = pd.read_csv(f)

# ----------------------------------------------------------------------
# Cleanup and last hour filter
# ----------------------------------------------------------------------
# Convert the timestamp column
df["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df["INTERVALSTARTTIME_GMT"])
df = df[df["INTERVALSTARTTIME_GMT"] >= start_time]

# Keep only useful columns
df = df[["INTERVALSTARTTIME_GMT", "NODE", "LMP_TYPE", "MW"]].copy()

# Transform from "long" to "wide" format
# Each node and interval will have separate columns for LMP, MCC, MCE, MCL
df = df.pivot_table(
    index=["INTERVALSTARTTIME_GMT", "NODE"],
    columns="LMP_TYPE",
    values="MW",
    aggfunc="first"
).reset_index()

# Remove the pivot column name
df.columns.name = None

# Rename for convenience
df.rename(columns={
    "INTERVALSTARTTIME_GMT": "timestamp_utc",
    "NODE": "hub",
    "LMP": "lmp_total",
    "MCC": "congestion",
    "MCE": "energy",
    "MCL": "loss"
}, inplace=True)

# Map more readable hub names
hub_names = {
    "TH_SP15_GEN-APND": "SP15",
    "TH_NP15_GEN-APND": "NP15",
    "TH_ZP26_GEN-APND": "ZP26"
}
df["hub"] = df["hub"].map(hub_names)

# Sort
df = df.sort_values(["timestamp_utc", "hub"]).reset_index(drop=True)

# ----------------------------------------------------------------------
# Download generation data (SLD_REN_FCST for Solar/Wind + ENE_SLRS for total)
# ----------------------------------------------------------------------
print("\nDownloading generation data...")

# 1. Renewable data (Solar, Wind)
url_ren = (
    f"http://oasis.caiso.com/oasisapi/SingleZip?"
    f"queryname=SLD_REN_FCST&"
    f"startdatetime={start_str}&"
    f"enddatetime={end_str}&"
    f"version=1&"
    f"resultformat=6"
)

response_ren = requests.get(url_ren, timeout=60)
with ZipFile(BytesIO(response_ren.content)) as z:
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        df_ren = pd.read_csv(f)

# Filter only RTD (real-time dispatch) and last 70 minutes
df_ren["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df_ren["INTERVALSTARTTIME_GMT"])
df_ren = df_ren[(df_ren["INTERVALSTARTTIME_GMT"] >= start_time) &
                 (df_ren["MARKET_RUN_ID"] == "RTD")]

# Aggregate by renewable type and timestamp
df_ren_agg = df_ren.groupby(["INTERVALSTARTTIME_GMT", "RENEWABLE_TYPE"])["MW"].sum().reset_index()
df_ren_agg = df_ren_agg.pivot(index="INTERVALSTARTTIME_GMT",
                               columns="RENEWABLE_TYPE",
                               values="MW").reset_index()
df_ren_agg.columns.name = None
df_ren_agg["renewables_total"] = df_ren_agg.get("Solar", 0) + df_ren_agg.get("Wind", 0)
df_ren_agg = df_ren_agg.fillna(0)

# 2. Total generation data (ENE_SLRS)
url_gen = (
    f"http://oasis.caiso.com/oasisapi/SingleZip?"
    f"queryname=ENE_SLRS&"
    f"startdatetime={start_str}&"
    f"enddatetime={end_str}&"
    f"version=1&"
    f"market_run_id=RTM&"
    f"resultformat=6"
)

response_gen = requests.get(url_gen, timeout=60)
with ZipFile(BytesIO(response_gen.content)) as z:
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        df_gen = pd.read_csv(f)

# Filter only CAISO total and last 70 minutes
# SLRS_TYPE='ALL' contains renewable generation data (Solar, Wind, etc.)
df_gen["INTERVALSTARTTIME_GMT"] = pd.to_datetime(df_gen["INTERVALSTARTTIME_GMT"])
df_gen = df_gen[(df_gen["INTERVALSTARTTIME_GMT"] >= start_time) &
                 (df_gen["TAC_ZONE_NAME"] == "Caiso_Totals") &
                 (df_gen["SLRS_TYPE"] == "ALL")]

df_gen_agg = df_gen.groupby(["INTERVALSTARTTIME_GMT"])["MW"].sum().reset_index()
df_gen_agg.rename(columns={"MW": "total_generation"}, inplace=True)

# Merge generation data
df_gen_combined = df_ren_agg.merge(df_gen_agg, on="INTERVALSTARTTIME_GMT", how="outer")
df_gen_combined.rename(columns={"INTERVALSTARTTIME_GMT": "timestamp_utc"}, inplace=True)

# Add thermal/other column as difference
df_gen_combined["thermal_and_other"] = (df_gen_combined.get("total_generation", 0) -
                                         df_gen_combined.get("renewables_total", 0))

print(f"Generation data downloaded: {len(df_gen_combined)} intervals")

# Merge with LMP data (join on timestamp)
# First group LMP by timestamp (average across 3 hubs)
df_lmp_agg = df[["timestamp_utc", "lmp_total", "congestion", "energy", "loss"]].groupby("timestamp_utc").mean().reset_index()

df_combined = df_lmp_agg.merge(df_gen_combined, on="timestamp_utc", how="inner")

# Sort
df_combined = df_combined.sort_values("timestamp_utc").reset_index(drop=True)

# Rename timestamp for clarity
df_combined.rename(columns={"timestamp_utc": "timestamp_utc_interval"}, inplace=True)

# Save the combined file
filename = f"caiso_lmp_generation_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}UTC.csv"
df_combined.to_csv(filename, index=False)

# Keep the old LMP file for compatibility
df.to_csv(f"caiso_lmp_last_hour_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}UTC.csv", index=False)

# Print results
print("Latest LMP data (average hub):")
print(df_lmp_agg.tail())

# ----------------------------------------------------------------------
# Result
# ----------------------------------------------------------------------
print("\n\nLatest generation data:")
print(df_combined.tail())

print(f"\nFile saved: {filename}")
print(f"  - LMP + generation: {filename}")
print(f"  - LMP only (legacy): caiso_lmp_last_hour_...csv")