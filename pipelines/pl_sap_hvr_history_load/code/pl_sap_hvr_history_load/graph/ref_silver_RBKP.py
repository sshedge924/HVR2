from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_history_load.config.ConfigStore import *
from pl_sap_hvr_history_load.functions import *

def ref_silver_RBKP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("doc_number"), 
        col("fiscal_year"), 
        col("client_id"), 
        col("reversal_doc_ind"), 
        col("doc_curr_amt"), 
        col("vendor_num"), 
        col("company_code"), 
        col("amt_locl_currency"), 
        col("tax_amt_local_curr"), 
        col("tran_amt"), 
        col("tax_category"), 
        col("_fivetran_synced"), 
        col("_loadtimestamp"), 
        col("_filename"), 
        col("_file_timestamp")
    )
