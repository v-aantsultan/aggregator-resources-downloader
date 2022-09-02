package com.eci.anaplan.train.nrd.details.module

import com.eci.anaplan.train.nrd.details.utils.SharedBaseTest
import com.eci.common.config.Environment.Environment
import com.eci.common.providers.{SlackClientProvider, SparkSessionProvider, StatusManagerClientProvider}
import com.google.inject.Guice

class TrainNRDDetailsModuleTest extends SharedBaseTest{
  // https://stackoverflow.com/questions/2448013/how-do-i-test-guice-injections
  // Testing to make sure AirportTransferAndBusModule take care to inject those provider.
  "Configure" should "inject classes" in {
    val injector = Guice.createInjector(new TrainNRDDetailsModule)

    injector.getProvider(classOf[Environment])
    injector.getProvider(classOf[SparkSessionProvider])
    injector.getProvider(classOf[StatusManagerClientProvider])
    injector.getProvider(classOf[SlackClientProvider])
  }

}
