package anaplan.ic.paylater.waterfall.aggregations.joiners

import anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.SlpCsf01DF
import com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners.IcPaylaterWaterFallDetail
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterWaterFallDetailTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]

  before {
    Mockito.when(mockS3SourceService.SlpCsf01Src).thenReturn(getMockSlpCsf01Src())
  }

  private val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
  private val icPaylaterWaterFallDetail: IcPaylaterWaterFallDetail = new IcPaylaterWaterFallDetail(testSparkSession, slpCsf01DF)

  "Data's column" should "only contain valid columns" in {
    val dataColumn = icPaylaterWaterFallDetail.joinWithColumn().columns
    val expectedColumn = Array (
      "report_date",
      "source_of_fund",
      "transaction_type",
      "loan_disbursed"
    )
    dataColumn shouldBe expectedColumn
  }

  "data" should "not 0" in {
    val countData = icPaylaterWaterFallDetail.joinWithColumn().count()
    assert(countData != 0)
  }
}
