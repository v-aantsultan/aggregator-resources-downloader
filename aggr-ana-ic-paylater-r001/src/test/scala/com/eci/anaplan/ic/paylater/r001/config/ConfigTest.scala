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
  protected var s3SourceService: S3SourceService = _
  protected var sparkSession: SparkSession = _

  before {
    Mockito.when(mockConfig.getString("app.name")).thenReturn("aggr-ana-ic-paylater-r001")
    Mockito.when(mockConfig.getString("flattener-srcdtl"))
      .thenReturn("s3a://ecidtpl-datalake-674080593013-6f9e42b6241969df")
    Mockito.when(mockConfig.getString("flattener-src"))
      .thenReturn("s3a://ecidtpl-data-warehouse-674080593013-d94c46ee172a319c/data_warehouse")
    Mockito.when(mockConfig.getString("start-date")).thenReturn("2022-06-25T00:00:00Z")
    Mockito.when(mockConfig.getString("end-date")).thenReturn("2022-07-28T00:00:00Z")
    Mockito.when(mockConfig.getString("aws-access-key-id"))
      .thenReturn("ASIAZZ4SVSR24QAWNX7Y")
    Mockito.when(mockConfig.getString("aws-secret-access-key"))
      .thenReturn("ynDPTlQ8MGy7zFFkXQAArE+36LN8XudMuDVsTkj/")
    Mockito.when(mockConfig.getString("aws-session-token"))
      .thenReturn("IQoJb3JpZ2luX2VjEEMaCXVzLWVhc3QtMSJGMEQCIGloSh/PcvtaAbbdFttFWFUvBbegsXLEVTjTpQQ/IRS/AiASxBy6TN" +
        "xD5Pjce/Mw8906zh6qLP0NldM3QRFKXF6nRSqoAgg8EAMaDDY3NDA4MDU5MzAxMyIMdPvtYJrAbvm/K0xHKoUCHBpn6v1x2HuB9b4xoUcUqfjC7" +
        "yWRFZbE/oYKus9f4weUyTCo7IFau5B+MQmxyxWEmFwK3AeXNkeYtDgdDgN9GXsgvLrkyT52v5myXh+TrPQvyOjsL+aJiKevAI/frtvLwMF9jeUG" +
        "KqIBpSUhKevai0KSemLcZcDk/1S3g7Fz+xm6hS1GJeW+3svjY8wqFWJ3N34Bmgz3X0swQR1PHuUkSWk0PkpcAIDsIVepr+BUmAincj4HRArZfE+" +
        "tKJUhVrIPeSybPn0wCByAGCHPEB6bZXRbxxyBfzJ+wtMz0B8ERaadbwOebjeDStZf82va0rAVM1jgCNv+qIBbSfsMEW/ARLpHEthAMM37ppsGOp" +
        "4BCpsCztjidD+4JM1YuoU8ti2t+sgnuxPvmlyAReIQmcyjYb59+NDsbHX6aGwxtD+L0BthDrmofvEemSgKm3vRKvQJnaHlf33jNnVkS4iF18kSU" +
        "2kOf+BV/VfOqTD7XNxxb3csRp5L27HgVLq7HgtZhhtbtwQhM654FkO9b3WHFdgh7PKXQZPMv9wOI7nKvDSrBwgKkibJswAw+1AdNUY="
      )

    val appConfig: AppConfig = new AppConfig(mockConfig) // application name
    val sparkSessionProvider: SparkSessionProvider = new SparkSessionProvider(Environment.LOCAL, appConfig, mockConfig)
    val sourceConfig: SourceConfig = new SourceConfig(mockConfig)
    sparkSession = sparkSessionProvider.get()
    s3SourceService = new S3SourceService(sparkSession, sourceConfig)
  }
}
