package com.eci.common.config

import com.eci.common.SharedBaseTest
import com.typesafe.config.Config
import org.mockito.Mockito.when

class DestinationConfigTest extends SharedBaseTest {
  import DestinationConfigTest._

  val mockConfig: Config = mock[Config]

  override def beforeEach(): Unit = {
    when(mockConfig.getString(SchemaKey)).thenReturn(SchemaVal)
    when(mockConfig.getString(TableKey)).thenReturn(TableVal)
    when(mockConfig.getString(PartitionKey)).thenReturn(PartitionVal)
    when(mockConfig.getString(PathKey)).thenReturn(PathVal)
  }

  "Constructor" should "create DestinationConfig with expected value" in {
    val destinationConfig = new DestinationConfig(mockConfig)

    destinationConfig.schema shouldBe SchemaVal
    destinationConfig.table shouldBe TableVal
    destinationConfig.partitionKey shouldBe PartitionVal
    destinationConfig.path shouldBe PathVal
  }

  it should "throw illegal argument if schema is empty" in {
    when(mockConfig.getString(SchemaKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new DestinationConfig(mockConfig))
  }

  it should "throw illegal argument if data-table val is empty" in {
    when(mockConfig.getString(TableKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new DestinationConfig(mockConfig))
  }

  it should "throw illegal argument if partition-key is empty" in {
    when(mockConfig.getString(PartitionKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new DestinationConfig(mockConfig))
  }

  it should "throw illegal argument if path is empty" in {
    when(mockConfig.getString(PathKey)).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(new DestinationConfig(mockConfig))
  }

  "toString" should "return all the expected field and val" in {
    new DestinationConfig(mockConfig).toString shouldBe "DestinationConfig{" +
      s"schema=$SchemaVal, " +
      s"table=$TableVal, " +
      s"partitionKey=$PartitionVal, " +
      s"path=$PathVal" +
      "}"
  }
}

object DestinationConfigTest {
  private val SchemaKey = "schema-name"
  private val SchemaVal = "schema"
  private val TableKey = "table-name"
  private val TableVal = "table"
  private val PartitionKey = "partition-key"
  private val PartitionVal = "partition"
  private val PathKey = "aggregator-dest"
  private val PathVal = "path"
}
