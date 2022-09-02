package com.eci.common.config

import com.eci.common.SharedBaseTest
import com.typesafe.config.Config
import org.mockito.Mockito.when

class StatusDbConfigTest extends SharedBaseTest {
  import StatusDbConfigTest._

  val mockConfig: Config = mock[Config]

  override def beforeEach(): Unit = {
    when(mockConfig.getString(s"$prefix.$UrlVal")).thenReturn(UrlVal)
    when(mockConfig.getString(s"$prefix.$UserVal")).thenReturn(UserVal)
    when(mockConfig.getString(s"$prefix.$PasswordVal")).thenReturn(PasswordVal)
  }

  "Constructor" should "create StatusDbConfig with expected value" in {
    val config = createStatusdbConfig(mockConfig)

    config.statusManagerConfig.url shouldBe UrlVal
    config.statusManagerConfig.username shouldBe UserVal
    config.statusManagerConfig.password shouldBe PasswordVal
  }

  it should "throw illegal argument if url is empty" in {
    when(mockConfig.getString(s"$prefix.$UrlVal")).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(createStatusdbConfig(mockConfig))
  }

  it should "throw illegal argument if user is empty" in {
    when(mockConfig.getString(s"$prefix.$UserVal")).thenReturn("")
    an[IllegalArgumentException] shouldBe thrownBy(createStatusdbConfig(mockConfig))
  }

  private def createStatusdbConfig(conf: Config): StatusDbConfig = {
    new StatusDbConfig(conf)
  }
}

object StatusDbConfigTest {
  val prefix = "statusmanager"
  val UrlVal = "url"
  val UserVal = "username"
  val PasswordVal = "password"
}
