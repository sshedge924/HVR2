from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *

def ref_silver_RSEG(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("doc_number"), 
        col("fiscal_year"), 
        col("line_item_num"), 
        col("item_quant"), 
        col("record_plant_code"), 
        col("purchase_ord_num"), 
        col("_fivetran_synced"), 
        col("_loadtimestamp"), 
        col("_filename")
    )
