# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c3448316-627e-49ce-8e65-01fb7ceb58d9",
# META       "default_lakehouse_name": "CRMBronze",
# META       "default_lakehouse_workspace_id": "214ebfed-db78-4346-851a-48464f5c69e0",
# META       "known_lakehouses": [
# META         {
# META           "id": "c3448316-627e-49ce-8e65-01fb7ceb58d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run Common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class CrmStoresPipelineStep(PipelineStepBase):
    def _get_data(self) -> DataFrame:
        data_df = read_json_file("Files/Zava/stores.json", stores_schema)
        return data_df

    def _write_to_lakehouse(self, df):
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("dbo.Stores")

    def run(self) -> PipelineResult:
        result = PipelineResult("CrmStoresPipelineStep")
        result.start()
        try:
            df = self._get_data()
            self._write_to_lakehouse(df)
            result.complete(True, f"Processed {df.count()} records")
        except Exception as ex:
            result.complete(False, str(ex), ex)
            raise ex
        finally:
            return result

if __name__ == "__main__" and os.getenv("PIPELINE_RUN") != "True":
    step = CrmStoresPipelineStep()
    result = step.run()
    print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
