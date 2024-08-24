from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *

def filter_by_maxtsp_bkpf(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((lit(Config.bron_bkpf_maxtsp) > lit(Config.slvr_bkpf_maxtsp)))
