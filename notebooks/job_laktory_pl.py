# COMMAND ----------
dbutils.widgets.text("pipeline_name", "pl-demo-stock-prices")
dbutils.widgets.text("node_name", "")
dbutils.widgets.text("full_refresh", "False")

# COMMAND ----------
# MAGIC %pip install 'laktory==0.5.2'
# MAGIC %restart_python

# COMMAND ----------
import importlib
import sys
import os
import pyspark.sql.functions as F

from laktory import models
from laktory import get_logger
from laktory import settings

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

pl_name = dbutils.widgets.get("pipeline_name")
node_name = dbutils.widgets.get("node_name")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"
filepath = f"/Workspace{settings.workspace_laktory_root}pipelines/{pl_name}.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

if node_name:
    pl.nodes_dict[node_name].execute(spark=spark, full_refresh=full_refresh)
else:
    pl.execute(spark=spark, full_refresh=full_refresh)
