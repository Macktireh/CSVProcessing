import os
from typing import List, Union

import pandas as pd
from tqdm import tqdm


class SFSHistoDataProcessor:
    """A class used to process a DataFrame from a CSV file."""

    df: pd.DataFrame

    def __init__(self, file_names: Union[str, List[str]], input_dir: str = "inputs"):
        self.file_names = file_names
        self.input_dir = input_dir

    def read_csv(self, file_name: str):
        """Reads the CSV file into a DataFrame."""
        path_input_file = f"{self.input_dir}/{file_name}"
        self.df = pd.read_csv(path_input_file)

    def rename_columns(self):
        """Renames the columns of the DataFrame."""
        renames_dict = {
            "[v__SHOP]":"[NberShopActivity]",
            "[v__SHOP2]":"[PerShopActivity]",
            "[v__Total_Sites_Active]":"[TotalSitesActive]",
            "[v__Food_Services2]":"[NberFoodServicesActivity]",
            "[v__Food_Services]":"[PerFoodServicesActivity]",
            "[v__Car_Services2]":"[NberCarServicesActivity]",
            "[v__Car_Services]":"[PerCarServicesActivity]",
            "[v__CarWash2]":"[NberCarWashActivity]",
            "[v__CarWash]":"[PerCarWashActivity]",
        }
        self.df.rename(columns = renames_dict, inplace = True)

    def drop_column(self):
        """Drops a specific column from the DataFrame."""
        self.df.drop(columns=['Geography[Id SubZone]'], inplace=True)

    def reorder_columns(self):
        """Reorders the columns of the DataFrame."""
        reordered_columns = [
            "[IsGrandTotalRowTotal]",
            "[TotalSitesActive]",
            "[NberShopActivity]",
            "[PerShopActivity]",
            "[NberFoodServicesActivity]",
            "[PerFoodServicesActivity]",
            "[NberCarServicesActivity]",
            "[PerCarServicesActivity]",
            "[NberCarWashActivity]",
            "[PerCarWashActivity]",
            "Geography[Zone]",
            "Geography[Sub Zone]",
            "Geography[Affiliates]"
        ]
        self.df = self.df.reindex(columns=reordered_columns)

    def insert_columns(self):
        """Inserts new columns into the DataFrame."""
        self.df.insert(loc=1, column="[TotalSites]", value=None, allow_duplicates=False)
        self.df.insert(loc=11, column="[NberHotBeverageActivity]", value=None, allow_duplicates=False)
        self.df.insert(loc=12, column="[PerHotBeverageActivity]", value=None, allow_duplicates=False)
        self.df.insert(loc=13, column="[NberServicesActivity]", value=None, allow_duplicates=False)
        self.df.insert(loc=14, column="[PerServicesActivity]", value=None, allow_duplicates=False)

    def export_to_csv(self, file_name: str):
        """Exports the DataFrame to a new CSV file."""
        output_dir = "outputs"
        path_output_file = f"{output_dir}/{file_name}"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.df.to_csv(path_output_file, index=False)

    def process(self, file_name: str):
        """
        Executes all the processing methods in order.

        The methods are executed in the following order:
        - read_csv
        - rename_columns
        - drop_column
        - reorder_columns
        - insert_columns
        - export_to_csv
        """
        self.read_csv(file_name)
        self.rename_columns()
        self.drop_column()
        self.reorder_columns()
        self.insert_columns()
        self.export_to_csv(file_name)
    
    def start(self):
        """Starts the processing of the CSV files."""
        if isinstance(self.file_names, list):
            for _file_name in tqdm(self.file_names):
                self.process(_file_name)
        else:
            self.process(self.file_names)
