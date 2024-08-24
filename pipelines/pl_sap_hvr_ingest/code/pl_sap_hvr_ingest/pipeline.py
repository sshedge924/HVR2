from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *
from prophecy.utils import *
from pl_sap_hvr_ingest.graph import *

def pipeline(spark: SparkSession) -> None:
    update_max_timestamps(spark)
    df_ref_BKPF = ref_BKPF(spark)
    df_ref_RSEG = ref_RSEG(spark)
    df_filter_data_2 = filter_data_2(spark, df_ref_RSEG)
    df_filter_data = filter_data(spark, df_ref_BKPF)
    df_ref_RBKP = ref_RBKP(spark)
    df_filter_data_1 = filter_data_1(spark, df_ref_RBKP)
    df_join_documents = join_documents(spark)
    df_filter_by_timestamp_comparison = filter_by_timestamp_comparison(spark, df_join_documents)
    df_ref_silver_RSEG = ref_silver_RSEG(spark)
    df_filter_by_max_timestamp_1 = filter_by_max_timestamp_1(spark, df_ref_silver_RSEG)
    df_ref_silver_RBKP = ref_silver_RBKP(spark)
    df_filter_by_max_timestamp = filter_by_max_timestamp(spark, df_ref_silver_RBKP)
    df_ref_silver_BKPF = ref_silver_BKPF(spark)
    df_filter_by_maxtsp_bkpf = filter_by_maxtsp_bkpf(spark, df_ref_silver_BKPF)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("pl_sap_hvr_ingest")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_sap_hvr_ingest")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pl_sap_hvr_ingest", config = Config)(pipeline)

if __name__ == "__main__":
    main()
