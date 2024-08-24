from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_ingest.config.ConfigStore import *
from pl_sap_hvr_ingest.functions import *

def update_max_timestamps(spark: SparkSession):
    from pyspark.sql import *
    from pyspark.sql.functions import max
    from datetime import datetime
    from pyspark.sql.functions import input_file_name, lit, substring
    #table = Config.tbl
    #schema = Config.sch_stg
    print('Init Timestamp BKPF: ', Config.bron_bkpf_maxtsp)
    print('Init Timestamp RBKP: ', Config.bron_rbkp_maxtsp)
    print('Init Timestamp RSEG: ', Config.bron_rseg_maxtsp)
    # LANDING LAYER DATA FROM FILES MAX SYNC TIMESTAMPS
    df11 = spark.read.format('parquet').load('dbfs:/mnt/HVR/transaction/*bkpf.parquet')
    df22 = spark.read.format('parquet').load('dbfs:/mnt/HVR/transaction/*rbkp.parquet')
    df33 = spark.read.format('parquet').load('dbfs:/mnt/HVR/transaction/*rseg.parquet')
    # Create a Temp Views for the dataframes
    df11.createOrReplaceTempView("v_bkpf_tbl")
    df22.createOrReplaceTempView("v_rbkp_tbl")
    df33.createOrReplaceTempView("v_rseg_tbl")
    # EXTRACTING AND ADDING LANDING LAYER _file_timestamp TO THE DATAFRAME
    df11 = spark.sql(
        f'''select *, substring(split(input_file_name(), '/')[4], 1, 14)   
                _file_timestamp from v_bkpf_tbl
                   '''
    )
    df22 = spark.sql(
        f'''select *, substring(split(input_file_name(), '/')[4], 1, 14)   
                _file_timestamp from v_rbkp_tbl
                   '''
    )
    df33 = spark.sql(
        f'''select *, substring(split(input_file_name(), '/')[4], 1, 14)   
                _file_timestamp from v_rseg_tbl
                   '''
    )
    # LANDING LAYER TIMESTAMPS
    var_maxsync_bkpf = df11.agg({"_file_timestamp" : "max"}).first()[0]
    var_maxsync_rbkp = df22.agg({"_file_timestamp" : "max"}).first()[0]
    var_maxsync_rseg = df33.agg({"_file_timestamp" : "max"}).first()[0]
    # LANDING LAYER TIMESTAMP CONV TO STRING TO STRIP MILLISECONDS
    # var_maxsync_bkpf = var_maxsync_bkpf.strftime("%Y-%m-%d %H:%M:%S")
    # var_maxsync_rbkp = var_maxsync_rbkp.strftime("%Y-%m-%d %H:%M:%S")
    # var_maxsync_rseg = var_maxsync_rseg.strftime("%Y-%m-%d %H:%M:%S")
    Config.maxsync_bkpf = var_maxsync_bkpf
    Config.maxsync_rbkp = var_maxsync_rbkp
    Config.maxsync_rseg = var_maxsync_rseg
    print('\nLANDING maxsync_bkpf:', Config.maxsync_bkpf)
    print('LANDING maxsync_rbkp:', Config.maxsync_rbkp)
    print('LANDING maxsync_rseg:', Config.maxsync_rseg)
    # BRONZE LAYER TIMESTAMPS
    df1 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_bronze.bkpf_tbl
                   ''')
    df2 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_bronze.rbkp_tbl
                   ''')
    df3 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_bronze.rseg_tbl
                   ''')
    var_bron_bkpf_maxtsp = df1.take(1)[0][0]
    var_bron_rbkp_maxtsp = df2.take(1)[0][0]
    var_bron_rseg_maxtsp = df3.take(1)[0][0]
    Config.bron_bkpf_maxtsp = var_bron_bkpf_maxtsp
    Config.bron_rbkp_maxtsp = var_bron_rbkp_maxtsp
    Config.bron_rseg_maxtsp = var_bron_rseg_maxtsp
    print('\nBRONZE Max _file_timestamp BKPF  : ', Config.bron_bkpf_maxtsp)
    print('BRONZE Max _file_timestamp RBKP  : ', Config.bron_rbkp_maxtsp)
    print('BRONZE Max _file_timestamp RSEG  : ', Config.bron_rseg_maxtsp)
    #############  SILVER LAYER #############
    df1 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_silver.bkpf_tbl
                   ''')
    var_slvr_bkpf_maxtsp = df1.take(1)[0][0]
    df2 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_silver.rbkp_tbl
                   ''')
    df3 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_silver.rseg_tbl
                   ''')
    var_slvr_bkpf_maxtsp = df1.take(1)[0][0]
    var_slvr_rbkp_maxtsp = df2.take(1)[0][0]
    var_slvr_rseg_maxtsp = df3.take(1)[0][0]
    Config.slvr_bkpf_maxtsp = var_slvr_bkpf_maxtsp
    Config.slvr_rbkp_maxtsp = var_slvr_rbkp_maxtsp
    Config.slvr_rseg_maxtsp = var_slvr_rseg_maxtsp
    print('\nSILVER Max _file_timestamp BKPF  : ', Config.slvr_bkpf_maxtsp)
    print('SILVER Max _file_timestamp RBKP  : ', Config.slvr_rbkp_maxtsp)
    print('SILVER Max _file_timestamp RSEG  : ', Config.slvr_rseg_maxtsp)
    # # # # # # # # #   GOLD LAYER   # # # # # # # # # # # #
    df1 = spark.sql(f'''select COALESCE(MAX(_file_timestamp), 0) 
                   from erp_gold.invoices_tbl
                   ''')
    var_gold_invoices_maxtsp = df1.take(1)[0][0]
    var_gold_bkpf_maxtsp = df1.take(1)[0][0]
    Config.gold_invoices_maxtsp = var_gold_invoices_maxtsp
    print('\nGOLD Max _fle_timestamp INVOICES  : ', Config.gold_invoices_maxtsp)

    return 
