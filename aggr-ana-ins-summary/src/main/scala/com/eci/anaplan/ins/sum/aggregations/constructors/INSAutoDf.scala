package com.eci.anaplan.ins.sum.aggregations.constructors

import com.eci.anaplan.ins.sum.services.S3SourceService
import org.apache.spark.sql.functions.{expr, regexp_replace, substring, to_date, split}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class INSAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSAutoDf
      .withColumn("payment_scope",
        regexp_replace($"`payment_scope`","adjustment.*,","")
      )
      .select(
        to_date($"`recognition_date`" + expr("INTERVAL 7 HOURS")).as("report_date"),
        $"`booking_id`".as("booking_id"),
        $"`product_type`".as("product"),
        split($"payment_scope",",")(0).as("payment_channel"),
        substring($"`locale`",-2,2).as("customer")
      )
  }
}