import os
import logging
import subprocess
from subprocess import CompletedProcess
from typing import List
from pathlib import PosixPath
from utils.utils import capture_output
from zipfile36 import ZipFile
from pathlib import PosixPath
from constants import *
import shutil
import pandas as pd

def extract(datasets: List[str]) -> None:

    set_kaggle_config_dir_venv()
    logging.info('Starting extract process')
    create_dir_to_save_dataset(datasets)
    download_data_from_kaggle(datasets)
    move_downloaded_zip_file(datasets)
    extract_data_from_zip_file(datasets)
    remove_zip_files(datasets)
    # convert_csv_to_parquet_format(datasets)
    # delete_csv_files()
    logging.info('Finished extract process')

def set_kaggle_config_dir_venv() -> None:

    pwd: str = os.getcwd()
    logging.info('Setting KAGGLE_CONFIG_DIR')
    os.environ['KAGGLE_CONFIG_DIR'] = f'{pwd}/.kaggle'

def create_dir_to_save_dataset(datasets: List[str]) -> None:

    for original_dataset in datasets:
        dataset = original_dataset.split('/')[1]
        folder_path = f'data/raw/{dataset}'

        if not os.path.exists(folder_path):
            try:
                os.makedirs(folder_path)
                log_message = f"Carpeta creada para {original_dataset} en: {folder_path}"
                logging.info(log_message)
            except OSError as e:
                log_message = f"Error al crear la carpeta para {original_dataset}: {e}"
                logging.info(log_message)
        else:
            log_message = f"La carpeta para {original_dataset} ya existe en: {folder_path}"
            logging.info(log_message)

def download_data_from_kaggle(datasets: List[str]) -> None:
    for dataset in datasets:
        logging.info(f'Started to download {dataset} dataset')
        completed_process: CompletedProcess = subprocess.run(['kaggle', 'datasets', 'download', '-d', f'{dataset}'], capture_output=True)
        capture_output(completed_process)
        logging.info(f'Finished to download {dataset} dataset')

def move_downloaded_zip_file(datasets: List[str]) -> None:

    for dataset in datasets:
        dataset_name = dataset.split('/')[1]
        logging.info(f'Moving {dataset_name}.zip to data/raw/{dataset_name}')
        
        source_path = f'{dataset_name}.zip'
        destination_path = f'data/raw/{dataset_name}/'
        
        try:
            shutil.move(source_path, destination_path)
            logging.info(f'Successfully moved {dataset_name}.zip')
        except shutil.Error as e:
            logging.error(f'Error moving {dataset_name}.zip: {e}')

def extract_data_from_zip_file(datasets: List[str]) -> None:

    for dataset in datasets:
        dataset = dataset.split('/')[1]
        dataset_path = DATA_RAW_DIR.resolve() / f'{dataset}'
        file_path = list(dataset_path.glob('*.zip'))[0]
        logging.info(f'Starting to extract {dataset}.zip')
        extract_zip_file(file_path)
        logging.info(f'Finished extraction process of the {dataset}.zip file')

def extract_zip_file(file_path: PosixPath) -> None:

    path = str(file_path.parent)
    file_path = str(file_path)
    with ZipFile(file_path, 'r') as zip:
        namelist: List[str] = zip.namelist()
        for member in namelist:
            logging.info(f'Start to extract {member} file')
            zip.extract(member=member, path=path)
            logging.info(f'{member} file extracted successfully')

def remove_zip_files(datasets: List[str]) -> None:

    for dataset in datasets:
        dataset_name = dataset.split('/')[1]
        zip_file_path = f'data/raw/{dataset_name}/{dataset_name}.zip'
        
        logging.info(f'{dataset_name}.zip will be deleted')
        
        try:
            os.remove(zip_file_path)
            logging.info(f'{dataset_name} was deleted')
        except OSError as e:
            logging.error(f'Error deleting {dataset_name}.zip: {e}')

def convert_csv_to_parquet_format(datasets: List[str]) -> None:

    for dataset in datasets:
        dataset = dataset.split('/')[1]
        logging.info(f'Start to convert csv files in {dataset} to parquet')
        dataset_path = DATA_RAW_DIR.resolve() / f'{dataset}'
        csv_files: List[PosixPath] = list(dataset_path.glob('*.csv'))
        for csv_file in csv_files:
            save_parquet_file(csv_file)
        logging.info(f'conversion process finished in the {dataset} folder')

def save_parquet_file(csv_file: PosixPath) -> None:

    df = pd.read_csv(csv_file)
    new_file: str = str(csv_file).replace('csv', 'parquet')
    logging.info(f'save {new_file.split("/")[-1]}')
    df.to_parquet(new_file, engine='fastparquet', index=False)


def delete_csv_files() -> None:

    logging.info('Deleting CSV files')

    try:
        csv_files = [file for file in Path('.').rglob('*.csv')]

        for csv_file in csv_files:
            os.remove(csv_file)

        logging.info('CSV files deleted successfully')
    except OSError as e:
        logging.error(f'Error deleting CSV files: {e}')