package com.eci.common.config

import com.eci.common.SharedBaseTest
import com.typesafe.config.Config
import org.mockito.Mockito.when

import java.time.format.DateTimeParseException

class SourceConfigTest extends SharedBaseTest{
  import SourceConfigTest._
  val mockConfig: Config = mock[Config]
  override def beforeEach(): Unit = {
    when(mockConfig.getString(FromDateKey)).thenReturn(FromDateVal)
    when(mockConfig.getString(ToDateKey)).thenReturn(ToDateVal)
    when(mockConfig.getString(PathKey)).thenReturn(PathVal)
    when(mockConfig.getString(DataWarehouseKey)).thenReturn(DataWarehouseVal)
  }

  it should "throw illegal argument if path is empty" in {
    when(mockConfig.getString(PathKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw illegal argument if datawarehouse-path is empty" in {
    when(mockConfig.getString(DataWarehouseKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw date time parse exception if from date is not formatted as ZonedDate" in {
    when(mockConfig.getString(FromDateKey)).thenReturn("2019-05-28")
    an[DateTimeParseException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw date time parse exception if to date is not formatted as ZonedDate" in {
    when(mockConfig.getString(ToDateKey)).thenReturn("2019-05-28")
    an[DateTimeParseException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw illegal argument if from date is in the future" in {
    when(mockConfig.getString(FromDateKey)).thenReturn("3000-01-01T00:00:00Z")
    an[IllegalArgumentException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw illegal argument if to date is in the future" in {
    when(mockConfig.getString(ToDateKey)).thenReturn("3000-01-01T00:00:00Z")
    an[IllegalArgumentException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }

  it should "throw illegal argument if to date is less or equal to from date" in {
    when(mockConfig.getString(FromDateKey)).thenReturn("2019-07-20T00:00:00Z")
    when(mockConfig.getString(ToDateKey)).thenReturn("2019-07-19T00:00:00Z")
    an[IllegalArgumentException] shouldBe thrownBy(new SourceConfig(mockConfig))
  }
}

object SourceConfigTest {
  private val FromDateKey = "start-date"
  private val FromDateVal = "2019-05-28T00:00:00Z"
  private val ToDateKey = "end-date"
  private val ToDateVal = "2019-05-30T00:00:00Z"
  private val PathKey = "flattener-srcdtl"
  private val PathVal = "path"
  private val DataWarehouseKey = "flattener-src"
  private val DataWarehouseVal = "datawarehouse-path"
}