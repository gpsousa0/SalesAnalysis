from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, DoubleType, DateType


def products_normalizer(df: DataFrame) -> DataFrame:
    """
    This function is responsible for normalizing Products data types.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be normalized.
    
    Returns:
        DataFrame containing data types normalized.
    """
    return (
        df
        .withColumn('ProductID', F.col('ProductID').cast(IntegerType()))
        .withColumn('ProductDesc', F.col('ProductDesc').cast(StringType()))
        .withColumn('ProductNumber', F.col('ProductNumber').cast(StringType()))
        .withColumn('MakeFlag', F.col('MakeFlag').cast(BooleanType()))
        .withColumn('Color', F.col('Color').cast(StringType()))
        .withColumn('SafetyStockLevel', F.col('SafetyStockLevel').cast(IntegerType()))
        .withColumn('ReorderPoint', F.col('ReorderPoint').cast(IntegerType()))
        .withColumn('StandardCost', F.col('StandardCost').cast(DoubleType()))
        .withColumn('ListPrice', F.col('ListPrice').cast(DoubleType()))
        .withColumn('Size', F.col('Size').cast(StringType()))
        .withColumn('SizeUnitMeasureCode', F.col('SizeUnitMeasureCode').cast(StringType()))
        .withColumn('Weight', F.col('Weight').cast(DoubleType()))
        .withColumn('WeightUnitMeasureCode', F.col('WeightUnitMeasureCode').cast(StringType()))
        .withColumn('ProductCategoryName', F.col('ProductCategoryName').cast(StringType()))
        .withColumn('ProductSubCategoryName', F.col('ProductSubCategoryName').cast(StringType()))
    )


def seller_order_details_normalizer(df: DataFrame) -> DataFrame:
    """
    This function is responsible for normalizing Sales Order Details data types.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be normalized.
    
    Returns:
        DataFrame containing data types normalized.
    """
    return (
        df
        .withColumn('SalesOrderID', F.col('SalesOrderID').cast(IntegerType()))
        .withColumn('SalesOrderDetailID', F.col('SalesOrderDetailID').cast(IntegerType()))
        .withColumn('OrderQty', F.col('OrderQty').cast(IntegerType()))
        .withColumn('ProductID', F.col('ProductID').cast(IntegerType()))
        .withColumn('UnitPrice', F.col('UnitPrice').cast(DoubleType()))
        .withColumn('UnitPriceDiscount', F.col('UnitPriceDiscount').cast(DoubleType()))
    )


def seller_order_header_normalizer(df: DataFrame) -> DataFrame:
    """
    This function is responsible for normalizing Sales Order Header data types.

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be normalized.
    
    Returns:
        DataFrame containing data types normalized.
    """
    return (
        df
        .withColumn('SalesOrderID', F.col('SalesOrderID').cast(IntegerType()))
        .withColumn('OrderDate', F.col('OrderDate').cast(DateType()))
        .withColumn('ShipDate', F.col('ShipDate').cast(DateType()))
        .withColumn('OnlineOrderFlag', F.col('OnlineOrderFlag').cast(BooleanType()))
        .withColumn('AccountNumber', F.col('AccountNumber').cast(StringType()))
        .withColumn('CustomerID', F.col('CustomerID').cast(IntegerType()))
        .withColumn('SalesPersonID', F.col('SalesPersonID').cast(IntegerType()))
        .withColumn('Freight', F.col('Freight').cast(DoubleType()))
    )
