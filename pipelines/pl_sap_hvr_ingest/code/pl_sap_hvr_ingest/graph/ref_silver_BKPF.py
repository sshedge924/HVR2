from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *

def ref_silver_BKPF(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("doc_number"), 
        col("user_name"), 
        col("company_code"), 
        col("fiscal_year"), 
        col("client"), 
        col("ind_spl_post"), 
        col("doc_type"), 
        col("curr_type2"), 
        col("tran_post_date"), 
        col("tran_curr_key"), 
        col("post_date_month"), 
        col("doc_date"), 
        col("_fivetran_synced"), 
        col("_loadtimestamp"), 
        col("_filename")
    )
