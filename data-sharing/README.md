# Data Sharing - ERDDAP Automated Fetch

This directory contains an automated process for fetching streamflow data from the Hakai Institute's ERDDAP server.

## What it does

The system automatically retrieves the latest provisional stream station data (water levels) from Hakai's watersheds monitoring network.

## Components

- **`fetch_erddap_data.R`**: R script that queries the Hakai ERDDAP server for recent streamflow data (last 24 hours)
- **`r-config.json`**: Configuration file specifying R dependencies and script path
- **`.github/workflows/fetch_erddap_data.yaml`**: GitHub Actions workflow that runs the fetch script

## Data Retrieved

The script fetches data from the `HakaiWatershedsStreamStationsProvisional` [dataset](https://catalogue.hakai.org/erddap/tabledap/HakaiWatershedsStreamStationsProvisional.subset)

## Automation

- **Scheduled runs**: Every 24 hours on the main branch
- **Testing**: Runs once on pull requests as a check

## Data Source

Data is pulled from: `https://catalogue.hakai.org/erddap/`
