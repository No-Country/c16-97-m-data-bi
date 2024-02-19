import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='log.log', filemode='w', level=logging.INFO)

from extract.extract import extract
# from transform.transform import transform
from load.load import load
from constants import OLIST_BRAZILIAN_ECOMMERCE

def pipeline() -> None:
    
    datasets = [OLIST_BRAZILIAN_ECOMMERCE]

    # extract(datasets)
    load()
    # transform()

if __name__ == '__main__':
    pipeline()