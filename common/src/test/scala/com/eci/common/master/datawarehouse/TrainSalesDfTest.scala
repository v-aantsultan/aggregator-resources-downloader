package com.eci.common.master.datawarehouse

import com.eci.common.services.S3SourceService
import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito.when

class TrainSalesDfTest extends SharedBaseTest with TestSparkSession {
  private val mockSourceService: S3SourceService = mock[S3SourceService]
  private val testTrainSalesDataFrame: TrainSalesDf =
    new TrainSalesDf(testSparkSession, mockSourceService)

  before {
    when(mockSourceService.TrainSalesAllPeriodSrc).thenReturn(mockedTrainSalesDataFrameSource)
  }

  "Train Sales Dataframe get All Period" should "contain valid columns in output" in {
    val resDf = testTrainSalesDataFrame.getallperiod
    val validExpectedColumns = Array(
      "non_refundable_date",
      "locale",
      "business_model",
      "fulfillment_id",
      "pd_booking_id",
      "coupon_code",
      "pax_quantity",
      "published_rate_contracting_idr",
      "provider_commission_idr",
      "wht_discount_idr",
      "discount_or_premium_idr",
      "unique_code_idr",
      "coupon_amount_idr",
      "total_amount_idr",
      "transaction_fee_idr",
      "vat_out_idr",
      "point_redemption_idr",
      "final_refund_status",
      "delivery_fee_idr"
    )

    val resColumns = resDf.columns
    validExpectedColumns shouldBe resColumns
  }

}
