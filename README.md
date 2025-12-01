# Energy Grid Price Monitor

A real-time data collection tool for California's electricity market. Fetches Locational Marginal Pricing (LMP) and generation data from CAISO (California Independent System Operator) OASIS API with no API key required.

## Features

- **Real-Time LMP Pricing**: Collects locational marginal prices for three major trading hubs:
  - SP15 (Southern California)
  - NP15 (Northern California)
  - ZP26 (Zone 26)

- **Price Components**: Breaks down pricing into:
  - LMP (Locational Marginal Price)
  - MCC (Marginal Congestion Cost)
  - MCE (Marginal Energy Cost)
  - MCL (Marginal Loss Cost)

- **Generation Data**: Real-time and near-term forecast data including:
  - Solar generation
  - Wind generation
  - Total generation
  - Thermal/Other generation

- **Automated Data Combination**: Merges pricing and generation data into a single CSV with timestamp alignment

- **No Authentication Required**: Uses public CAISO OASIS API endpoints

## Requirements

- Python 3.7+
- `pandas`: Data manipulation and transformation
- `requests`: HTTP requests for API calls

## Installation

```bash
pip install pandas requests
```

## Usage

Run the script to fetch the latest hour of data:

```bash
python caiso_last_hour.py
```

### Output

The script generates two CSV files:

1. **caiso_lmp_generation_YYYYMMDD_HHMMUTC.csv** - Combined LMP and generation data
   - Columns: timestamp, LMP prices, generation data
   - Data: Averaged across all 3 hubs per timestamp

2. **caiso_lmp_last_hour_YYYYMMDD_HHMMUTC.csv** - Raw LMP data per hub
   - One row per hub per timestamp
   - Useful for detailed hub-level analysis

### Example Output

```
timestamp_utc_interval    lmp_total  congestion  energy      loss  Solar  Wind  total_generation  renewables_total  thermal_and_other
2025-01-15 12:05:00       45.23      2.10        41.50       1.63   2500  4200  18500             6700              11800
2025-01-15 12:10:00       44.87      1.95        41.20       1.72   2600  4150  18600             6750              11850
```

## API Endpoints Used

- **PRC_INTVL_LMP**: Real-time 5-minute interval locational marginal prices
- **SLD_REN_FCST**: Solar and wind generation forecasts
- **ENE_SLRS**: Total system load and renewable supply

## Data Refresh

The script looks back 70 minutes to ensure capturing the last complete hour of data. Can be scheduled with cron or task scheduler for periodic updates.

## CAISO Documentation

- [CAISO OASIS](http://oasis.caiso.com)
- [API Documentation](http://www.caiso.com/market/Pages/ReportsBulletins/MarketNotices/OASISAPIDocumentation.aspx)

## License

[Add your license here]

## Contributing

Feel free to submit issues and enhancement requests!
