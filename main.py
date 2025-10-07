from pyspark.sql import SparkSession

from modules.utils import read_yaml_config, read_config_parameters, csv_reader, parquet_writer
from modules.normalizer import products_normalizer, seller_order_details_normalizer, seller_order_header_normalizer
from modules.transformer import products_transformer, seller_order_details_transformer


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Upstart13") \
        .getOrCreate()
    
    # Reading YAML config file
    config = read_yaml_config(file_path="config.yaml")

    # Setting CSV bronze paths
    brz_path_products = read_config_parameters(config_file=config, structure=["bronze", "products"])
    brz_path_sales_order_details = read_config_parameters(config_file=config, structure=["bronze", "sales_order_details"])
    brz_path_sales_order_header = read_config_parameters(config_file=config, structure=["bronze", "sales_order_header"])

    # Setting Parquet silver paths
    slv_path_products = read_config_parameters(config_file=config, structure=["silver", "products"])
    slv_path_sales_order_details = read_config_parameters(config_file=config, structure=["silver", "sales_order_details"])
    slv_path_sales_order_header = read_config_parameters(config_file=config, structure=["silver", "sales_order_header"])

    # Setting Parquet gold paths
    gld_path_products = read_config_parameters(config_file=config, structure=["gold", "products"])
    gld_path_publish_orders = read_config_parameters(config_file=config, structure=["gold", "publish_orders"])

    # Reading CSV files from bronze layer
    raw_products_df, raw_sales_order_details_df, raw_sales_order_header_df = map(
        lambda path: csv_reader(spark, path)
        , [brz_path_products, brz_path_sales_order_details, brz_path_sales_order_header]
    )

    # Normalizing DataFrames Data Types
    store_products_df = products_normalizer(raw_products_df)
    store_sales_order_details_df = seller_order_details_normalizer(raw_sales_order_details_df)
    store_sales_order_header_df = seller_order_header_normalizer(raw_sales_order_header_df)

    # Writing Parquet files to silver layer
    parquet_writer(store_products_df, slv_path_products)
    parquet_writer(store_sales_order_details_df, slv_path_sales_order_details)
    parquet_writer(store_sales_order_header_df, slv_path_sales_order_header)

    # Transforming data
    gld_products_df = products_transformer(store_products_df)
    publish_orders = seller_order_details_transformer(store_sales_order_details_df, store_sales_order_header_df)

    # Writing Parquet files to gold layer
    parquet_writer(gld_products_df, gld_path_products)
    parquet_writer(publish_orders, gld_path_publish_orders) 

    # Stop Spark session
    spark.stop()

    
if __name__ == "__main__":
    main()
