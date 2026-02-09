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

import os

os.environ["PIPELINE_RUN"] = "True"

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

steps = [CrmCustomersPipelineStep(), CrmProductsPipelineStep(), CrmInventoryPipelineStep(), CrmStoresPipelineStep()]
for step in steps:
    step.run()
    logging.info(f"{step.__class__.__name__} step completed")

logging.info(f"All steps completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
