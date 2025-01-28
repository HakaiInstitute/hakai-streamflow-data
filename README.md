
# Streamflow Time Series Data Repository

## Overview
- This dataset represents streamflow time series collected at 5-minute intervals and updated annually, offering high-resolution data for monitoring and analyzing streamflow dynamics. The data is generated using the autosalt dilution method, a novel and reliable technique that involves injecting a salt solution into the stream and measuring changes in electrical conductivity to calculate discharge. 
- This method ensures accurate streamflow measurements, even during high-flow events, making it particularly useful for hydrological studies, water resource management, and environmental assessments. Updated annually, the dataset provides a comprehensive and dependable record of streamflow conditions, crucial for understanding the impacts of climate variability and land-use changes on watershed hydrology.



---

## Repository Structure
- **`data/`**: Contains raw and processed streamflow data in `.csv` format.
- **`scripts/`**: Contains scripts for data processing, analysis, and visualization.
- **`archived/`**: Contains links to archived datasets.
- **`reports/`**: Detailed descriptions of the autosalt dilution methods used in streamflow measurements.
- **`README.md`**: Documentation for understanding and using this repository.

---

## Features
- Frequency: 5-minute interval data.
- Format: Data provided in `.csv` files.
- Updates: Annually updated with the latest measurements.
- Streams/Rivers: [SSN626, SSN703, SSN844, SSN1015]

---

## Data Description
| **Column Name** | **Description**                             |
|------------------|---------------------------------------------|
| `timestamp`      | Date and time of measurement (PST). |
| `qlevel`     | Quality level. |
| `qflag`     | Quality flag. |
| `qrate`     | Discharge rate (m³/s). |
| `site_id`        | Unique identifier for each stream site.    |



---

## How to Use
### Cloning the Repository
```bash
git clone https://github.com/emilyhaughton/hakai-streamflow-data.git
cd hakai-streamflow-data
```

### Accessing the Data
- Navigate to the `data/` directory to find `.csv` files.
- Files are named by stream and year, e.g., `streamname_YYYY.csv`.

### File Format
- Example file: `streamname_2023.csv`
- Example rows:
  ```
  timestamp,qlevel,qflag,qrate,latitude,longitude
  2019-01-01 00:05:00,2,AV,12.3,51.69,-128.2
  2019-01-01 00:10:00,,2,MV,12.7,51.69,-128.2

  ```

---

## Usage Examples
### In R
```R
df <- read.csv("data/streamname_2023.csv")
head(df)
```

---

## Reports Folder

The reports/ folder includes:
- Quality Reports: Detailed reports on the data quality, including outlier detection, data gaps, and cleaning methods.
- Autosalt Dilution Methodology: A comprehensive explanation of the autosalt dilution methods used to measure streamflow, including setup, calibration, and considerations for accurate data collection.
These reports are essential for understanding the methodology behind the data collection and ensuring the validity of the results.

---

## Scripts Folder

The scripts/ folder contains reusable code to help users process and analyze the streamflow data:
- Data Processing Scripts: For cleaning, transforming, and preparing the data.
- Analysis Tools: Scripts assessing and developing rating curves.


## License
- **Creative Commons Attribution 4.0**, **MIT License**

---

## Acknowledgments
- Data collected by [Hakai Institute].
- Supported by [Tula Foundation/Hakai Institute].

---

## Citing This Repository
- Include a citation format if this data is used in academic or professional work:
  ```
  Floyd, W.C., Haughton, E.R., Korver, M. (2024). Streamflow Time Series Data Repository. GitHub. https://github.com/yourusername/streamflow-data
  ```

---

## Contact
- Name: [Emily Haughton]
- Email: [emily.haughton@hakai.org]
- GitHub: [@emilyhaughton](https://github.com/emilyhaughton)
