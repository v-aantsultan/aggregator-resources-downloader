package com.eci.anaplan.ins.sum.aggregations.constructors

import com.eci.anaplan.ins.sum.services.S3SourceService
import org.apache.spark.sql.functions.{expr, lit, regexp_replace, split, substring, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class INSNonAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSNonAutoDf
      .withColumn("payment_scope",
        regexp_replace($"`payment_scope`","adjustment.*,","")
      )
      .select(
        to_date($"`booking_issued_date`" + expr("INTERVAL 7 HOURS")).as("report_date"),
        $"`booking_id`".as("booking_id"),
        split($"payment_scope",",")(0).as("payment_channel"),
        substring($"`locale`",-2,2).as("customer"),
        when($"`booking_type`".isin("CROSSSELL_ADDONS","ADDONS","CROSSSELL_BUNDLE"),lit("IA"))
          .otherwise(lit("IS"))
          .as("product")
      )
  }
}
