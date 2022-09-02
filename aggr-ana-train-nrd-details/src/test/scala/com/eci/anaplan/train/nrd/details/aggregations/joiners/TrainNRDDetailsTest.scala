package com.eci.anaplan.train.nrd.details.aggregations.joiners

import com.eci.anaplan.train.nrd.details.aggregations.constructors.TrainSalesbyNRD
import com.eci.anaplan.train.nrd.details.utils.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.master.datawarehouse.TrainSalesDf
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TrainNRDDetailsTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService: S3SourceService = mock[S3SourceService]

  private val TestTrainSalesDf: TrainSalesDf = new TrainSalesDf(testSparkSession, mockS3SourceService)

  before {
    when(mockS3SourceService.TrainSalesAllPeriodSrc).thenReturn(mockedTrainSalesAllPeriodSrc)
  }

  private val TestTrainSalesbyNRD: TrainSalesbyNRD = new TrainSalesbyNRD(testSparkSession, TestTrainSalesDf)

  private val TestTrainNRDDetails: TrainNRDDetails = new TrainNRDDetails(testSparkSession, TestTrainSalesbyNRD)

  // TODO :  Write Some test Like this

  "Train Sales by NRD DataFrame get" should "only contain valid columns" in {
    val resDf = TestTrainNRDDetails.joinWithColumn()
    val validExpectedColumns = Array(
      "report_date",
      "customer",
      "business_model",
      "business_partner",
      "product_category",
      "no_of_transactions",
      "no_of_coupon",
      "transaction_volume",
      "gmv",
      "gross_revenue",
      "commission",
      "discount",
      "premium",
      "unique_code",
      "coupon",
      "nta",
      "transaction_fee",
      "vat_out",
      "point_redemption"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validExpectedColumns
  }
}
