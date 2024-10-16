import os
from typing import List

import columns
from sfs_processing import SFSHistoDataProcessor

INPUT_DIR = "inputs"


def read_csv_file_names() -> List[str]:
    """read CSV file names from the 'inputs' folder"""
    return [
        f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f)) and f.endswith(".csv")
    ]


if __name__ == "__main__":
    file_names = read_csv_file_names()
    processor = SFSHistoDataProcessor(file_names, INPUT_DIR)
    processor.setup_columns(
        drop_columns_list=columns.drop_columns,
        renames_dict=columns.renames_dict,
        new_columns_list=columns.new_columns,
        reordered_columns_list=columns.reordered_columns,
    )
    processor.start()
