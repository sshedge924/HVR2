from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_sap_hvr_history_load.config.ConfigStore import *
from pl_sap_hvr_history_load.functions import *
from prophecy.utils import *
from pl_sap_hvr_history_load.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ref_RSEG = ref_RSEG(spark)
    df_ref_RBKP = ref_RBKP(spark)
    df_ref_BKPF = ref_BKPF(spark)
    df_filter_deleted_records = filter_deleted_records(spark, df_ref_BKPF)
    df_ref_silver_BKPF = ref_silver_BKPF(spark)
    df_ref_silver_RSEG = ref_silver_RSEG(spark)
    df_ref_silver_RBKP = ref_silver_RBKP(spark)
    df_join_documents = join_documents(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("pl_sap_hvr_history_load")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_sap_hvr_history_load")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pl_sap_hvr_history_load", config = Config)(pipeline)

if __name__ == "__main__":
    main()
