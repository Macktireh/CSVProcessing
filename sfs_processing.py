import os
from typing import List, Union

import pandas as pd
from tqdm import tqdm


class SFSHistoDataProcessor:
    """A class used to process a DataFrame from a CSV file."""

    df: pd.DataFrame

    def __init__(self, file_names: Union[str, List[str]], input_dir: str = "inputs") -> None:
        self.file_names = file_names
        self.input_dir = input_dir

    def setup_columns(
        self,
        drop_columns_list: list[str],
        renames_dict: dict[str, str],
        new_columns_list: list[str],
        reordered_columns_list: list[str],
    ) -> None:
        self.drop_columns_list = drop_columns_list
        self.renames_dict = renames_dict
        self.new_columns_list = new_columns_list
        self.reordered_columns_list = reordered_columns_list

    def read_csv(self, file_name: str) -> None:
        """Reads the CSV file into a DataFrame."""
        path_input_file = f"{self.input_dir}/{file_name}"
        self.df = pd.read_csv(path_input_file)

    def delete_rows(self) -> None:
        self.df.drop(self.df[self.df["[IsGrandTotalRowTotal]"] == True].index, inplace=True)  # noqa: E712

    def drop_columns(self) -> None:
        """Drops a specific column from the DataFrame."""
        self.df.drop(columns=self.drop_columns_list, inplace=True)

    def rename_columns(self) -> None:
        """Renames the columns of the DataFrame."""
        # renames_dict = {
        #     "Geography[Zone]": "[Zone]",
        #     "Geography[Sub Zone]": "[SubZone]",
        #     "Geography[Affiliates]": "[Affiliates]",
        # }
        self.df.rename(columns=self.renames_dict, inplace=True)

    def insert_columns(self) -> None:
        """Inserts new columns into the DataFrame."""
        # new_columns = [
        #     "[NberNoShopActivity]",
        #     "[PerNoShopActivity]",
        #     "[NberNotDeclaredShopActivity]",
        #     "[PerNotDeclaredShopActivity]",
        #     "[NberNoFoodServicesActivity]",
        #     "[PerNoFoodServicesActivity]",
        #     "[NberNotDeclaredFoodServicesActivity]",
        #     "[PerNotDeclaredFoodServicesActivity]",
        #     "[NberNoHotBeverageActivity]",
        #     "[PerNoHotBeverageActivity]",
        #     "[NberNotDeclaredHotBeverageActivity]",
        #     "[PerNotDeclaredHotBeverageActivity]",
        #     "[NberNoCarServicesActivity]",
        #     "[PerNoCarServicesActivity]",
        #     "[NberNotDeclaredCarServicesActivity]",
        #     "[PerNotDeclaredCarServicesActivity]",
        #     "[NberNoCarWashActivity]",
        #     "[PerNoCarWashActivity]",
        #     "[NberNotDeclaredCarwashActivity]",
        #     "[PerNotDeclaredCarwashActivity]",
        #     "[NberNoServicesActivity]",
        #     "[PerNoServicesActivity]",
        #     "[NberNotDeclaredServicesActivity]",
        #     "[PerNotDeclaredServicesActivity]",
        # ]
        
        for column in self.new_columns_list:
            self.df.insert(loc=1, column=column, value=None, allow_duplicates=False)

    def reorder_columns(self) -> None:
        """Reorders the columns of the DataFrame."""
        # reordered_columns = [
        #     "[IsGrandTotalRowTotal]",
        #     "[TotalSitesActive]",
        #     "[NberShopActivity]",
        #     "[PerShopActivity]",
        #     "[NberFoodServicesActivity]",
        #     "[PerFoodServicesActivity]",
        #     "[NberCarServicesActivity]",
        #     "[PerCarServicesActivity]",
        #     "[NberCarWashActivity]",
        #     "[PerCarWashActivity]",
        #     "Geography[Zone]",
        #     "Geography[Sub Zone]",
        #     "Geography[Affiliates]",
        # ]
        self.df = self.df.reindex(columns=self.reordered_columns_list)

    def export_to_csv(self, file_name: str) -> None:
        """Exports the DataFrame to a new CSV file."""
        output_dir = "outputs"
        path_output_file = f"{output_dir}/{file_name}"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.df.to_csv(path_output_file, index=False)

    def process(self, file_name: str) -> None:
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
        self.delete_rows()
        self.drop_columns()
        self.rename_columns()
        self.insert_columns()
        self.reorder_columns()
        self.export_to_csv(file_name)

    def start(self):
        """Starts the processing of the CSV files."""
        if isinstance(self.file_names, list):
            for _file_name in tqdm(self.file_names):
                self.process(_file_name)
        else:
            self.process(self.file_names)
