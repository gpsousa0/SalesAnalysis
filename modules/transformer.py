from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


def products_transformer(df: DataFrame) -> DataFrame:
    """
    This function is responsible for transforming Products data by:
        1. Filling 'N/A' for null values in Color column.
        2. Setting Category Name for each SubCategory group.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be transformed.

    Returns:
        DataFrame containing data transformed.
    """
    return (
        df
        .withColumn(
            'Color',
            F.coalesce(F.col('Color'), F.lit('N/A'))
        )
        .withColumn(
            'ProductCategoryName',
            F.when(
                F.col('ProductSubCategoryName').isin('Gloves', 'Shorts', 'Socks', 'Tights', 'Vests'),
                'Clothing'
            ).when(
                F.col('ProductSubCategoryName').isin('Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps'),
                'Accessories'
            ).when(
                F.col('ProductSubCategoryName').rlike('Frames') | F.col('ProductSubCategoryName').isin('Wheels', 'Saddles'),
                'Components'
            ).otherwise(F.col('ProductCategoryName'))
        )
    )


def seller_order_details_transformer(seller_order_details: DataFrame, seller_order_header: DataFrame) -> DataFrame:
    """
    This function is responsible for transforming Sales Order Details data by:
        1. Join Orders Details with Orders Header.
        2. Calculate Delivery in Days excluding Saturdays and Sundays.
        3. Calculate Revenue for each item * quantity.

    Args:
        seller_order_details (pyspark.sql.DataFrame): DataFrame to be transformed.
        seller_order_header (pyspark.sql.DataFrame): DataFrame to join with Order Details.

    Returns:
        DataFrame containing data transformed.
    """
    return (
        seller_order_details
        .join(seller_order_header, 'SalesOrderID', 'inner')
        .withColumn(
            'TotalDifferenceInDays',
            F.datediff(F.col('ShipDate'), F.col('OrderDate'))
        )
        .withColumn(
            'AllDates',
            F.expr('sequence(OrderDate, ShipDate, interval 1 day)')
        )
        .withColumn(
            'WeekendDays',
            F.size(F.expr('filter(AllDates, x -> dayofweek(x) IN (1,7))'))
        )
        .withColumn(
            'LeadTimeInBusinessDays',
            F.col('TotalDifferenceInDays') - F.col('WeekendDays')
        )
        .withColumn(
            'TotalLineExtendedPrice',
            F.col('OrderQty') * (F.col('UnitPrice') - F.col('UnitPriceDiscount'))
        )
        .withColumnRenamed('Freight', 'TotalOrderFreight')
        .drop('TotalDifferenceInDays', 'AllDates', 'WeekendDays')
    )
