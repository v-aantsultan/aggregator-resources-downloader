package com.eci.common.schema

import org.apache.spark.sql.types._

object SlpCsfReceivableAgingSchema {

  val schema = StructType(Array(
    StructField("loan_id", StringType, true),
    StructField("customer_id", StringType, true),
    StructField("financing_type", StringType, true),
    StructField("source_of_fund", StringType, true),
    StructField("disbursement_date", TimestampType, true),
    StructField("due_date", TimestampType, true),
    StructField("dpd", IntegerType, true),
    StructField("loan_bucket", StringType, true),
    StructField("outstanding_principal_amount_-_customer", IntegerType, true),
    StructField("outstanding_principal_amount_-_csf's_portion", IntegerType, true),
    StructField("outstanding_principal_amount_-_third_party's_portion", IntegerType, true),
    StructField("outstanding_interest_amount_after_subsidy_-_customer", IntegerType, true),
    StructField("outstanding_interest_amount_after_subsidy_-_csf's_portion", IntegerType, true),
    StructField("outstanding_interest_amount_after_subsidy_-_third_party's_portion", IntegerType, true),
    StructField("outstanding_late_fee_-_customer", IntegerType, true),
    StructField("outstanding_late_fee_-_csf's_portion", DoubleType, true),
    StructField("outstanding_late_fee_-_third_party's_portion", DoubleType, true),
    StructField("accrued_interest_amount_before_subsidy", IntegerType, true),
    StructField("unearned_interest_amount_before_subsidy", IntegerType, true),
    StructField("accrued_subsidy_amount", IntegerType, true),
    StructField("deferred_subsidy_amount", IntegerType, true),
    StructField("is_written_off", StringType, true),
    StructField("channeling_agent_name", StringType, true)
  ))

}
