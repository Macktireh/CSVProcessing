import os

from sfs_processing import SFSHistoDataProcessor

INPUT_DIR = 'inputs'


def read_csv_file_names():
    """read CSV file names from the 'inputs' folder"""
    return [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f)) and f.endswith('.csv')]


if __name__ == '__main__':
    file_names = read_csv_file_names()
    processor = SFSHistoDataProcessor(file_names, INPUT_DIR)
    processor.start()
