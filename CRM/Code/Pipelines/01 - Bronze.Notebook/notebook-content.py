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

import os

os.environ["PIPELINE_RUN"] = "True"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

start_time = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run CrmCustomersPipelineStep

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run CrmProductsPipelineStep

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run CrmInventoryPipelineStep

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run CrmStoresPipelineStep

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    result_list = PipelineResultList()
    status = "Success"

    pipeline_steps = [
        CrmCustomersPipelineStep(),
        CrmProductsPipelineStep(),
        CrmInventoryPipelineStep(),
        CrmStoresPipelineStep(),
    ]

    for step in pipeline_steps:
        result = step.run()
        result_list.add(result)

        if not result.is_success:
            raise result.exception

except Exception as ex:
    status = "Error"
    logging.error(f"Pipeline failed: {ex}")
finally:
    for result in result_list:
        logging.info(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
