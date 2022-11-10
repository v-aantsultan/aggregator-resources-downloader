package anaplan.ic.paylater.waterfall.aggregations.constructors

import anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.SlpCsf01DF
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class SlpCsf01DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]

  before {
    when(mockS3SourceService.SlpCsf01Src).thenReturn(getMockSlpCsf01Src())
  }

  private val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf01DF.getSpecific
    val validationColumn = Array(
      "report_date",
      "source_of_fund",
      // "transaction_type",
      "loan_disbursed"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validationColumn
  }

  it should "not 0" in {
    val countData = slpCsf01DF.getSpecific.count()
    println(s"count data : $countData")
    assert(countData != 0)
  }

}
