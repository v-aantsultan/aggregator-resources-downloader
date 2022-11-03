package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.Inject

class MappingUnderlyingProductDF @Inject()(
                                            val sparkSession: SparkSession,
                                            s3SourceService: S3SourceService
                                           ){
  import sparkSession.implicits._

  def getData: DataFrame = {
      s3SourceService.MappingUnderLyingProductSrc
        .select(
          // fs_product_type only for dummy
//          when($"fs_product_type" === "FL", lit("FLIGHT"))
//            .when($"fs_product_type" === "HT", lit("HOTEL"))
//            .when($"fs_product_type" === "BS", lit("BUS"))
//            .when($"fs_product_type" === "TR", lit("TRAIN"))
//            .when($"fs_product_type" === "CU", lit("CULINARY"))
//            .when($"fs_product_type" === "FBHB", lit("BUNDLE"))
//            .when($"fs_product_type" === "EBILL", lit("BP"))
//            .otherwise($"fs_product_type")
//            .as("fs_product_type"),

          // fs_product_type real data
          $"fs_product_type".as("fs_product_type"),

          $"underlying_product".as("underlying_product")
        )
  }
}
