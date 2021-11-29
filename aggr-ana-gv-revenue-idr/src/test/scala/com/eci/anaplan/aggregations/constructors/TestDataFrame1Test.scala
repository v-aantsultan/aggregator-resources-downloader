package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import com.eci.anaplan.utils.{TestSparkSession, SharedBaseTest}
import org.mockito.Mockito.{mock, when}

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TestDataFrame1Test extends SharedBaseTest with TestSparkSession {
  import testSparkSession.implicits._

  private val mockS3SourceService: S3SourceService = mock(classOf[S3SourceService])
  private val testDiscountPremiumDataFrame: GVRevenueDf = new GVRevenueDf(testSparkSession, mockS3SourceService)
  before {
    when(mockS3SourceService.dataFrameSource1).thenReturn(mockedDataFrameSource1)
  }

  /*
  TODO :  Write Some test Like this



  "Discount PremiumData Frame get" should "only contain valid columns" in {
    val validExpectedColumns = Array(
      "sales_delivery_id",
      "amount",
      "booking_id",
      "created_at"
    )
    val resColumns = testDiscountPremiumDataFrame.get.columns

    validExpectedColumns shouldBe resColumns
  }

  it should "only contain discount_or_premium as charge type" in {
    val testDiscountPremiumDf = testDiscountPremiumDataFrame.get
    val filteredSalesDeliveryDfCount = filterSalesDeliveryDataFrame(salesDeliveryDf)
      .where($"`sales.sales_delivery_item_charge.charge_type_name`" === "discount_or_premium").count()
    val expectedSalesDeliveryDfCount = testDiscountPremiumDf.count()

    filteredSalesDeliveryDfCount shouldBe expectedSalesDeliveryDfCount
  }*/
}
