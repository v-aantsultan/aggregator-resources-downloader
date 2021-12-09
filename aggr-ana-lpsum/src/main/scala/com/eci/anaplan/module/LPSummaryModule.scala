package com.eci.anaplan.module

import com.eci.common.config.Environment
import com.eci.anaplan.providers.LPSummarySparkSession
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.traveloka.eci.statusmanager.client.{DefaultStatusDbClientFactory, StatusManagerClientFactory}
import org.apache.spark.sql.SparkSession
import scala.util.Properties

/**
 * Dependency Injection module initialisation class
 */
class LPSummaryModule extends AbstractModule {
  override def configure(): Unit = {
    val env = Environment.withName(Properties.propOrElse("environment", "local").toLowerCase)
    bind(classOf[Environment.Value]).toInstance(env)
    bind(classOf[SparkSession]).toProvider(classOf[LPSummarySparkSession]).asEagerSingleton()
    bindConstant().annotatedWith(Names.named("TENANT_ID")).to("1")
    bind(classOf[StatusManagerClientFactory]).toInstance(DefaultStatusDbClientFactory)
  }
}