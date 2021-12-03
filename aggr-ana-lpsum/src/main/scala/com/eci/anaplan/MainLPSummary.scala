package com.eci.anaplan

import com.eci.anaplan.module.LPSummaryModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainLPSummary {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new LPSummaryModule)
    val coordinator: LPSummaryCoordinator = injector.getInstance(classOf[LPSummaryCoordinator])
    coordinator.coordinate()
  }
}
