package com.eci.common.module

import com.eci.common.config.Environment
import com.eci.common.providers.{SlackClientProvider, SparkSessionProvider, StatusManagerClientProvider}
import com.eci.common.slack.SlackClient
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.traveloka.eci.statusmanager.client.StatusManagerClient
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import scala.util.Properties

/**
 * Dependency Injection module initialisation class
 */
class AnaplanModuleCommon extends AbstractModule {
  protected def initialize(configFileName: String): Unit = {
    val environment = Environment.withName(Properties.propOrElse("environment", "local").toLowerCase)
    bind(classOf[Environment.Value]).toInstance(environment)

    bind(classOf[SparkSession]).toProvider(classOf[SparkSessionProvider]).asEagerSingleton()
    bind(classOf[StatusManagerClient]).toProvider(classOf[StatusManagerClientProvider]).asEagerSingleton()
    bind(classOf[SlackClient]).toProvider(classOf[SlackClientProvider]).asEagerSingleton()
    bindConstant().annotatedWith(Names.named("TENANT_ID")).to("1")

    val config = {
      environment match {
        case Environment.LOCAL =>
          val environmentName = environment.toString.toLowerCase()
          ConfigFactory.load(s"$configFileName-$environmentName")
        case _ => ConfigFactory.load(configFileName)
      }
    }
    bind(classOf[Config]).toInstance(config)
  }
}
