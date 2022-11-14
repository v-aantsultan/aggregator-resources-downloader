package com.eci.anaplan.ic.paylater.r003.module

import com.eci.anaplan.ic.paylater.r003.SharedBaseTest
import com.eci.common.config.Environment.Environment
import com.eci.common.providers.{SlackClientProvider, SparkSessionProvider, StatusManagerClientProvider}
import com.google.inject.Guice

class IcPaylaterR003ModuleTest extends SharedBaseTest{

  "Configure" should "inject classes" in {
    val injector = Guice.createInjector(new IcPaylaterR003Module)

    injector.getProvider(classOf[Environment])
    injector.getProvider(classOf[SparkSessionProvider])
    injector.getProvider(classOf[StatusManagerClientProvider])
    injector.getProvider(classOf[SlackClientProvider])
  }
}
