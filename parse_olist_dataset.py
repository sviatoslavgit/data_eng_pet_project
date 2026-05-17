from pathlib import Path

import kagglehub
import pandas as pd


DATASET_HANDLE = "olistbr/brazilian-ecommerce"
OUTPUT_FILE = Path("data/olist_dataset.pkl")


def download_dataset() -> Path:
    """Download the latest Olist dataset version and return its local path."""
    return Path(kagglehub.dataset_download(DATASET_HANDLE))


def load_csv_files(dataset_path: Path) -> dict[str, pd.DataFrame]:
    """Load every CSV file from the dataset directory without transforming it."""
    csv_files = sorted(dataset_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {dataset_path}")

    return {
        csv_file.stem: pd.read_csv(csv_file)
        for csv_file in csv_files
    }


def print_dataset_summary(tables: dict[str, pd.DataFrame]) -> None:
    """Print table names, shapes, and columns for a quick sanity check."""
    for table_name, dataframe in tables.items():
        columns = ", ".join(dataframe.columns)
        print(f"{table_name}: {dataframe.shape[0]} rows x {dataframe.shape[1]} columns")
        print(f"  columns: {columns}")


def save_tables(tables: dict[str, pd.DataFrame], output_file: Path) -> Path:
    """Save all loaded tables into one local file for later analysis."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(tables, output_file)
    return output_file


def main() -> None:
    dataset_path = download_dataset()
    print("Path to dataset files:", dataset_path)

    tables = load_csv_files(dataset_path)
    print_dataset_summary(tables)

    output_file = save_tables(tables, OUTPUT_FILE)
    print("Saved parsed dataset to:", output_file)


if __name__ == "__main__":
    main()
