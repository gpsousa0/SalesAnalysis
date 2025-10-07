import yaml
from typing import Dict, List

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


def read_yaml_config(file_path: str) -> Dict:
    """
    This function is responsible for reading a YAML file.

    Args:
        file_path (str): YAML file path.
    
    Returns:
        Dict containing the data from YAML file.
    """
    with open(file_path) as file:
        config_list = yaml.load(file, Loader=yaml.FullLoader)

    return config_list


def read_config_parameters(config_file: Dict, structure: List[str]) -> str:
    """
    This function is responsible for reading a specific parameter from a config file.

    Args:
        config_file (dict): Config file to read.
        structure (list): Structure from config parameter.

    Returns:
        Str containing the parameter value.
    """
    config_parameter = ""

    for item in structure:
        if config_parameter == "":
            config_parameter = config_file.get(item)
        else:
            config_parameter = config_parameter.get(item)

    return config_parameter


def csv_reader(spark: SparkSession, file_path: str, header: bool = True, sep: str = ',') -> DataFrame:
    """
    Reads a CSV file and returns a PySpark DataFrame.

    Args:
        file_path (str): CSV file path.
        header (bool, optional): Indicates if the CSV file contains a header. Default is True.
        sep (str, optional): File separator. Default is ','.

    Returns:
        pyspark.sql.DataFrame: DataFrame with CSV data.
    """
    return spark.read.format('csv').options(header=header, sep=sep).load(file_path)


def parquet_writer(df: DataFrame, file_path: str, partition_keys: list = None) -> None:
    """
    Writes a PySpark DataFrame to a Parquet file.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be written.
        file_path (str): Output Parquet file path.
        partition_keys (list, optional): List of column names to partition by. Default is None.
    """
    writer = df.write.mode('overwrite')
    
    if partition_keys:
        writer = writer.partitionBy(*partition_keys)
    
    writer.parquet(file_path)
