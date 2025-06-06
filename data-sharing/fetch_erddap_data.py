from erddapy import ERDDAP
from datetime import date
import pandas as pd


def main(
    dataset_id: str = "HakaiWatershedsStreamStationsProvisional",
    # columns: list = ["station", "longitude" "time", "pls_lvl", "pls_lvl_ql", "pls_lvl_qc"],
) -> dict:
    """
    Fetch discharge data from Hakai ERDDAP server
    """

    time_constraint = date.today().isoformat()

    # Initialize ERDDAP client
    e = ERDDAP(server="https://catalogue.hakai.org/erddap/")

    # Configure the request
    e.dataset_id = dataset_id
    e.protocol = "tabledap"
    # e.variables = columns
    e.constraints = {"time>=": time_constraint}

    # Fetch data
    try:
        discharge = e.download_file("csv")
        discharge_df = pd.read_csv(discharge, skiprows=2)

        result = {
            "success": True,
            "row_count": len(discharge_df),
            "columns": list(discharge_df.columns),
            "data": discharge_df.to_dict("records"),
            "query_info": {
                "dataset_id": dataset_id,
                "time_constraint": time_constraint,
                "server": "https://catalogue.hakai.org/erddap/",
            },
        }

        return result

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query_info": {
                "dataset_id": dataset_id,
                "time_constraint": time_constraint,
                "server": "https://catalogue.hakai.org/erddap/",
            },
        }


if __name__ == "__main__":
    result = main()
    print(f"Retrieved {result.get('row_count', 0)} rows")
