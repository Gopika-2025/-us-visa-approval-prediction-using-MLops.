import os
import sys

# Add root directory to sys.path before local imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import dill
import yaml
from pandas import DataFrame

from US_Visa.exception import USvisaException
from US_Visa.logger import logging


def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns the contents as a dictionary.
    """
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise USvisaException(e, sys) from e


def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Writes a Python object to a YAML file.

    Parameters:
    - file_path: Path where the YAML file will be written.
    - content: Python object to write.
    - replace: If True, replaces the file if it exists.
    """
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise USvisaException(e, sys) from e


def load_object(file_path: str) -> object:
    """
    Loads a serialized Python object from a file using dill.

    Parameters:
    - file_path: Path to the .pkl/.dill file

    Returns:
    - The loaded Python object
    """
    logging.info("Entered the load_object method of utils")

    try:
        with open(file_path, "rb") as file_obj:
            obj = dill.load(file_obj)
        logging.info("Exited the load_object method of utils")
        return obj
    except Exception as e:
        raise USvisaException(e, sys) from e


def save_object(file_path: str, obj: object) -> None:
    """
    Saves a Python object to a file using dill.

    Parameters:
    - file_path: File path to save the object
    - obj: Python object to serialize
    """
    logging.info("Entered the save_object method of utils")

    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
        logging.info("Exited the save_object method of utils")
    except Exception as e:
        raise USvisaException(e, sys) from e


def save_numpy_array_data(file_path: str, array: np.array) -> None:
    """
    Save numpy array data to file.

    Parameters:
    - file_path: Location of file to save
    - array: np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, 'wb') as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise USvisaException(e, sys) from e


def load_numpy_array_data(file_path: str) -> np.array:
    """
    Load numpy array data from file.

    Parameters:
    - file_path: Location of file to load

    Returns:
    - np.array: Loaded data
    """
    try:
        with open(file_path, 'rb') as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise USvisaException(e, sys) from e


def drop_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Drop specified columns from a pandas DataFrame.

    Parameters:
    - df: Input DataFrame
    - cols: List of column names to drop

    Returns:
    - DataFrame with specified columns dropped
    """
    logging.info("Entered the drop_columns method of utils")

    try:
        df = df.drop(columns=cols, axis=1)
        logging.info("Exited the drop_columns method of utils")
        return df
    except Exception as e:
        raise USvisaException(e, sys) from e
