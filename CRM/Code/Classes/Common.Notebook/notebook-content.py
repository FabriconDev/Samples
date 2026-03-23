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
from datetime import datetime


class PipelineResult:
    def __init__(self, step_name: str):
        self.step_name:str = step_name
        self.is_success:bool = False
        self.message:str = None
        self.start_time:datetime = None
        self.end_time:datetime = None
        self.exception:Exception = None

    def __str__(self):
        return f"step_name: {self.step_name}, is_success: {self.is_success}, message: {self.message}, execution_time: {self.end_time - self.start_time}"

    def start(self):
        self.start_time = datetime.now()

    def complete(self, success: bool, message: str, exception: Exception = None):
        self.end_time = datetime.now()
        self.message = message
        self.is_success = success
        self.exception = exception


class PipelineResultList:
    def __init__(self):
        self._results = []

    def add(self, result):
        if not isinstance(result, PipelineResult):
            raise TypeError("Only PipelineResult objects can be added.")
        self._results.append(result)

    def __iter__(self):
        return iter(self._results)

    def __len__(self):
        return len(self._results)


class PipelineStepBase(ABC):
    """Abstract Base class for all pipeline classes"""

    @abstractmethod
    def _get_data(self) -> DataFrame:  # A `DataFrame` containing the query results
        """Mehtod to get data need to run this pipeline step"""
        pass

    @abstractmethod
    def _write_to_lakehouse(
        self, df: DataFrame  # A `DataFrame` containing the query results
    ):
        """Mehtod to write data from this pipeline step to lakehouse"""
        pass

    @abstractmethod
    def run(self) -> PipelineResult:
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
