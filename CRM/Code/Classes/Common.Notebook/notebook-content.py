# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, force=True)

# This ensures that table names are not all lower case.
spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, ArrayType, BooleanType, LongType
)

import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("storeId", LongType(), nullable=False),
    StructField("firstName", StringType(), nullable=False),
    StructField("lastName", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False),
    StructField("address", StringType(), nullable=True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_item_schema = StructType([
    StructField("orderItemId", LongType(), nullable=False),
    StructField("productId", LongType(), nullable=False),
    StructField("sku", StringType(), nullable=False),
    StructField("productName", StringType(), nullable=False),
    StructField("unitPrice", DoubleType(), nullable=False),
    StructField("quantity", LongType(), nullable=False),
    StructField("lineTotal", DoubleType(), nullable=False),
])

customer_orders_schema = StructType([
    StructField("orderId", LongType(), nullable=False),
    StructField("customerId", LongType(), nullable=False),
    StructField("storeId", LongType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("items", ArrayType(order_item_schema, containsNull=False), nullable=False),
    StructField("orderTotal", DoubleType(), nullable=False),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("sku", StringType(), nullable=False),
    StructField("description", StringType(), nullable=True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stores_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inventory_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("storeId", LongType(), nullable=False),
    StructField("productId", LongType(), nullable=False),
    StructField("quantity", LongType(), nullable=False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PipelineStepBase

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class PipelineStepBase(ABC):
    """Abstract Base class for all pipeline classes"""

    @abstractmethod
    def _get_data(self) -> DataFrame:  # A `DataFrame` containing the query results
        """Method to get data needed to run this pipeline step"""
        pass

    @abstractmethod
    def _write_to_lakehouse(
        self, df: DataFrame  # A `DataFrame` containing the query results
    ):
        """Method to write data from this pipeline step to the lakehouse"""
        pass

    @abstractmethod
    def run(self) -> bool:
        """Method to run the pipeline step"""
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_schema(
        df: DataFrame, schema: StructType
        ) -> bool:
        """
        Validate that the DataFrame has all columns present and with correct types
        according to the schema.

        :param schema: The expected schema as a StructType.
        :param df: The DataFrame to validate.
        :return: True if the DataFrame is valid according to the schema; False otherwise.
        """
        # Get the list of expected columns and their data types from the schema
        expected_columns = {field.name: field.dataType for field in schema.fields}

        # Get the actual columns and their data types from the sampled DataFrame
        actual_columns = {field.name: field.dataType for field in df.schema.fields}

        assert len(expected_columns) == len(actual_columns), f"Column count mismatch.\nActual: {df.schema.simpleString()}\nExpected: {schema.simpleString()}"

        # Check if each column's data type matches the expected data type
        for col_name, expected_type in expected_columns.items():
            actual_type = actual_columns[col_name]
            assert actual_type == expected_type, f"Schema mismatch.\nActual:   {df.schema.simpleString()}\nExpected: {schema.simpleString()}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_json_file(file_path:str, schema:StructType) -> DataFrame:
    data_df = spark.read.option("multiline", "true").json(file_path)        
    validate_schema(data_df, schema)
    return data_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
