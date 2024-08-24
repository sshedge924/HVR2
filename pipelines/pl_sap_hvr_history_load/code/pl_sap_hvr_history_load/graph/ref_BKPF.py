from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_sap_hvr_history_load.config.ConfigStore import *
from pl_sap_hvr_history_load.functions import *

def ref_BKPF(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("belnr").alias("doc_number"), 
        col("bukrs").alias("company_code"), 
        col("gjahr").alias("fiscal_year"), 
        col("mandt").alias("client"), 
        col("duefl").alias("due_date_flag"), 
        col("lotkz").alias("ind_spl_lot"), 
        col("stjah").alias("doc_fiscal_year"), 
        col("xref2_hd").alias("hdr_refkey2"), 
        col("blind").alias("ind_blind_post"), 
        col("kzkrs").alias("amt_local_curr"), 
        col("basw3").alias("ind_spl_post"), 
        col("xrueb").alias("ref_doc_number"), 
        col("kurst").alias("exch_rate_type"), 
        col("arcid").alias("arch_obj_id"), 
        col("xreorg").alias("ind_reorg"), 
        col("stblg").alias("clear_ref_doc_no"), 
        col("knumv").alias("acc_no_doc_posted"), 
        col("cputm").alias("time_last_doc_change"), 
        col("ccins").alias("ind_credit_ctrl_area"), 
        col("blart").alias("doc_type"), 
        col("intdate").alias("int_tran_date"), 
        col("basw2").alias("ind_tran"), 
        col("xsplit").alias("ind_split_tran"), 
        col("ssblk").alias("ind_block_tran"), 
        col("rldnr").alias("ref_doc_no_tran"), 
        col("xmwst").alias("ind_tax_calc"), 
        col("glvor").alias("ind_doc_reversal"), 
        col("ldgrp").alias("key_group_clear"), 
        col("kzwrs").alias("ind_curr_trran"), 
        col("wwert").alias("ind_val_adjust"), 
        col("curt3").alias("curr_type3"), 
        col("bvorg").alias("bus_tran_type"), 
        col("brnch").alias("branch_code"), 
        col("ctxkrs").alias("curr_exch_rate"), 
        col("cpudt").alias("date_last_change"), 
        col("penrc").alias("tran_penalty_rate"), 
        col("kuty3").alias("curr_conv_type"), 
        col("xusvr").alias("usr_spec_tran_var"), 
        col("xreversal").alias("ind_tran_reversal"), 
        col("fikrs").alias("tran_comp_code"), 
        col("propmano").alias("prop_mgmt_num"), 
        col("reindat").alias("tran_reenf_date"), 
        col("xblnr").alias("ref_doc_num"), 
        col("curt2").alias("curr_type2"), 
        col("budat").alias("tran_post_date"), 
        col("xwvof").alias("ind_tax__withholding"), 
        col("kuty2").alias("condition_type"), 
        col("kur2x").alias("curr_conv_exch_rate"), 
        col("dbblg").alias("doc_num_clearingdoc"), 
        col("awtyp").alias("aseet_tran_type"), 
        col("kurs3").alias("exch_rate_curr_conv3"), 
        col("ausbk").alias("auto_pay_ind"), 
        col("offset_refer_dat").alias("offset_ref_date"), 
        col("psotm").alias("doc_post_time"), 
        col("xsnet").alias("net_post_ind"), 
        col("psobt").alias("doc_post_text"), 
        col("kurs2").alias("exch_rate_curr2"), 
        col("offset_status"), 
        col("bktxt").alias("doc_header_txt"), 
        col("grpid").alias("doc_group_id"), 
        col("awkey").alias("doc_key"), 
        col("stodt").alias("doc_post_dt"), 
        col("intform").alias("doc_internal_form"), 
        col("hwae3").alias("curr_key3"), 
        col("adisc").alias("doc_addl_info"), 
        col("hwaer").alias("tran_curr_key"), 
        col("psozl").alias("local_curr_amt"), 
        col("psosg").alias("group_curr_amt"), 
        col("monat").alias("post_date_month"), 
        col("sampled").alias("tran_sampling_ind"), 
        col("doccat").alias("doc_category"), 
        col("upddt").alias("doc_last_upd_date"), 
        col("dokid").alias("document_id"), 
        col("hwae2").alias("curr_key_local2"), 
        col("tcode").alias("tran_code_doc"), 
        col("txkrs").alias("exch_rate_trancurr"), 
        col("psoak").alias("partial_clear_ind"), 
        col("psodt").alias("doc_post_date"), 
        col("waers").alias("tran_curr"), 
        col("xblnr_alt").alias("alt_doc_num"), 
        col("xref1_hd").alias("head_ref1"), 
        col("awsys").alias("doc_gen_sysname"), 
        col("kursf").alias("exch_rate_forgn_curr"), 
        col("resubmission").alias("doc_resubmit_ind"), 
        col("kursx").alias("exch_rate_local_curr"), 
        col("vatdate").alias("vat_date"), 
        col("batch").alias("grouping_batch_num"), 
        col("follow_on").alias("tran_followon_ind"), 
        col("iblar").alias("doc_type_ind"), 
        col("bstat").alias("doc_status"), 
        col("fm_umart").alias("fin_mgmt_field_us"), 
        col("stgrd").alias("fin_mgmt_field_st"), 
        col("sname").alias("supplier_name"), 
        col("bldat").alias("doc_date"), 
        col("cash_alloc").alias("cash_alloc_ind"), 
        col("umrd3").alias("spl_gl_tran_ind"), 
        col("psoks").alias("post_status_ind"), 
        col("numpg").alias("num_of_pages"), 
        col("frath").alias("exch_rate_foreign_curr"), 
        col("xmca").alias("cash_mgmt_ind"), 
        col("ppnam").alias("payment_part_name"), 
        col("ccnum").alias("credit_card_num"), 
        col("usnam").alias("user_name"), 
        col("xstov").alias("reversal_ind"), 
        col("psofn").alias("payment_order_num"), 
        col("psoty").alias("type_ofPosting"), 
        col("umrd2").alias("curr_conv_rate2"), 
        col("kur3x").alias("exch_rate_conv3"), 
        col("_sapf15_status"), 
        col("xnetb").alias("net_bal_aft_post"), 
        col("exclude_flag").alias("entry_exclude_flag"), 
        col("subset").alias("entry_subset"), 
        col("aedat").alias("entry_last_change_date"), 
        concat(
            substring(col("hvr_change_time"), 1, 4), 
            substring(col("hvr_change_time"), 6, 2), 
            substring(col("hvr_change_time"), 9, 2), 
            substring(col("hvr_change_time"), 12, 2), 
            substring(col("hvr_change_time"), 15, 2), 
            substring(col("hvr_change_time"), 18, 2)
          )\
          .cast(LongType())\
          .alias("hvr_change_time"), 
        col("_fivetran_deleted"), 
        date_format(col("_fivetran_synced"), "yyyy-MM-dd HH:mm:ss").alias("_fivetran_synced"), 
        date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("_loadtimestamp"), 
        split(input_file_name(), "/")[4].alias("_filename"), 
        substring(split(input_file_name(), "/")[4], 1, 14).cast(LongType()).alias("_file_timestamp")
    )
