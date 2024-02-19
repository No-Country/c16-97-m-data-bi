import logging
from constants import *
from transform.brazilian_ecommerce import *
import os

def transform() -> None:

    logging.info('Starting transform process')  
    create_processed_data_dir()

def create_processed_data_dir() -> None:
    logging.info('Starting to create a processed data folder')

    folder_path = f'data/processed/'

    if not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
            log_message = f"Carpeta creada"
            logging.info(log_message)
        except OSError as e:
            log_message = f"Error al crear la carpeta"
            logging.info(log_message)
    else:
        log_message = f"La carpeta ya existe"
        logging.info(log_message)