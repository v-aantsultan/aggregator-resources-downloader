package com.eci.anaplan.hs_nrd.details.aggregations.constructors

import com.eci.anaplan.hs_nrd.details.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class HealthServicesNrdDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.HealthServiceNrdDf
      .select(
        $"non_refundable_date",
        $"locale",
        $"business_model",
        $"fulfillment_id",
        $"category",
        $"booking_id",
        $"coupon_code",
        $"number_of_tickets",
        $"recommended_price_contract_currency",
        $"published_rate_contract_currency",
        $"actual_gross_commission_contract_currency",
        $"discount_premium_incoming_fund_currency",
        $"discount_tax_expense",
        $"discount_premium_incoming_fund_currency",
        $"unique_code",
        $"coupon_value",
        $"actual_gross_commission_contract_currency",
        $"nta_contract_currency",
        $"transaction_fee",
        $"point_redemption",
        $"vat_out_contract_currency"
      )
  }

}