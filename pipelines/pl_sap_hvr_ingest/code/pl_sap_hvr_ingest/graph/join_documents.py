from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *

def join_documents(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.doc_number") == col("in1.doc_number")) & (col("in0.fiscal_year") == col("in1.fiscal_year"))),
          "left_outer"
        )\
        .join(
          in2.alias("in2"),
          ((col("in0.doc_number") == col("in2.doc_number")) & (col("in0.fiscal_year") == col("in2.fiscal_year"))),
          "left_outer"
        )\
        .select(col("in0.doc_number").alias("doc_number"), col("in0.user_name").alias("user_name"), col("in0.company_code").alias("company_code"), col("in0.fiscal_year").alias("fiscal_year"), col("in0.client").alias("client"), col("in0.ind_spl_post").alias("ind_spl_post"), col("in0.doc_type").alias("doc_type"), col("in0.curr_type2").alias("curr_type2"), col("in0.tran_post_date").alias("tran_post_date"), col("in0.tran_curr_key").alias("tran_curr_key"), col("in0.post_date_month").alias("post_date_month"), col("in0.doc_date").alias("doc_date"), col("in1.client_id").alias("client_id"), col("in1.reversal_doc_ind").alias("reversal_doc_ind"), col("in1.doc_curr_amt").alias("doc_curr_amt"), col("in1.vendor_num").alias("vendor_num"), col("in1.amt_locl_currency").alias("amt_locl_currency"), col("in1.tax_amt_local_curr").alias("tax_amt_local_curr"), col("in1.tran_amt").alias("tran_amt"), col("in1.tax_category").alias("tax_category"), col("in2.line_item_num").alias("line_item_num"), col("in2.item_quant").alias("item_quant"), col("in2.record_plant_code").alias("record_plant_code"), col("in2.purchase_ord_num").alias("purchase_ord_num"), col("in0._fivetran_synced").alias("_fivetran_synced"), col("in0._loadtimestamp").alias("_loadtimestamp"), col("in0._filename").alias("_filename"))
