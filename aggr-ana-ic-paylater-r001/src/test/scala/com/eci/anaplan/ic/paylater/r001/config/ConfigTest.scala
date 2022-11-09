package com.eci.anaplan.ic.paylater.r001.config

import com.eci.anaplan.SharedBaseTest
import com.eci.common.config.{AppConfig, Environment, SourceConfig}
import com.eci.common.providers.SparkSessionProvider
import com.eci.common.services.S3SourceService
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class ConfigTest extends SharedBaseTest {

  private val mockConfig: Config = MockitoSugar.mock[Config]
  var s3SourceService: S3SourceService = _
  var sparkSession: SparkSession = _
  var appConfig: AppConfig = _
  var sparkSessionProvider: SparkSessionProvider = _
  var sourceConfig: SourceConfig = _

  before {
    Mockito.when(mockConfig.getString("app.name")).thenReturn("aggr-ana-ic-paylater-r001")
    Mockito.when(mockConfig.getString("flattener-srcdtl"))
      .thenReturn("s3a://ecidtpl-datalake-674080593013-6f9e42b6241969df")
    Mockito.when(mockConfig.getString("flattener-src"))
      .thenReturn("s3a://ecidtpl-data-warehouse-674080593013-d94c46ee172a319c/data_warehouse")
    Mockito.when(mockConfig.getString("start-date")).thenReturn("2022-06-25T00:00:00Z")
    Mockito.when(mockConfig.getString("end-date")).thenReturn("2022-06-28T00:00:00Z")
    Mockito.when(mockConfig.getString("aws-access-key-id"))
      .thenReturn("ASIAZZ4SVSR255J6V3KT")
    Mockito.when(mockConfig.getString("aws-secret-access-key"))
      .thenReturn("Bhn4NE8VV8+0KbWFKTz4r9Jo8VHkoTsJR5O7swiR")
    Mockito.when(mockConfig.getString("aws-session-token"))
      .thenReturn("IQoJb3JpZ2luX2VjEEkaCXVzLWVhc3QtMSJIMEYCIQCQcemjeKyghQSMkLqnur3B0RSPzHTBgZdMpC/4B56JPgIhAKiem" +
        "msyerUFWL6kSOzRveidj6SQZJWR7ZgB8FAH0k92KqgCCEIQAxoMNjc0MDgwNTkzMDEzIgznv3pGCuA4qpb6e5MqhQKYnUivlU/5JNXz60DrGnzA" +
        "ecqy93fL46SBmv1eQa259vEVilBk47lXmkodDChVY3zVXaM/N/Xj196MTrQDYqqfuLHn33IOiGK1zC1HwKocqFWt61BK73Nnd7xT+BGSbbKrUlE" +
        "TL6Jb6S4ey6/Vgg0b3RmggHu9OU9ithu8tIT8J4hJXmGsQ1e1YYt2rBkvGsLBPm/6ratxxU7PX0N5vzMqd6mBWxgDmgRkS8o+0TuZy3uP8sFW2O" +
        "8eLOys0nQHVGKMYKTratFgnN8ljeJmik/kbovjd5Eo3zzY9umg9Mo6KQYg/1KYo+Nqm36Z/4CIFdhgxq0CPrx+nbO+8Iwo87hUhz3An9Yw5quomw" +
        "Y6nAEgJjCNoZa0WXcOPtoyEyWeDVAEJkbGNDkGa/wqYkrAaGtByr7R6PuDhGkMBHrHB4cy6pAP68m8fjB9A3eZmEZzLpwyxPHwGk2FJ1PfGK4sjz" +
        "L4UdoPGqg+i9FQSXbh8D6y9w/MYyyAVRD9x91hld70v5nSS1A1lzoKbQI521qeag22fepystkwieyU2UItAcvrC0CTH8HP6N4/i/8="
      )

    appConfig = new AppConfig(mockConfig) // application name
    sparkSessionProvider = new SparkSessionProvider(Environment.LOCAL, appConfig, mockConfig)
    sourceConfig = new SourceConfig(mockConfig)
    sparkSession = sparkSessionProvider.get()
    s3SourceService = new S3SourceService(sparkSession, sourceConfig)
  }
}
