from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_history_load.config.ConfigStore import *
from pl_sap_hvr_history_load.functions import *

def filter_deleted_records(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_fivetran_deleted") == lit(True)))
