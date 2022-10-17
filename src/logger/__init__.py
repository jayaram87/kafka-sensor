import logging
import os
from datetime import datetime

log_dir = 'logs'

log_file = f'log_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.log'

os.makedirs(log_dir, exist_ok=True)

log_file_path = os.path.join(log_dir, log_file)

logging.basicConfig(filename=log_file_path,
filemode='w', 
level=logging.INFO, 
format='[%(asctime)s]^;%(levelname)s^;%(lineno)d^;%(filename)s^;%(funcName)s()^;%(message)s')