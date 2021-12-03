package com.eci.anaplan

import com.eci.anaplan.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainLPSummary {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: LPSummaryCoordinator = injector.getInstance(classOf[LPSummaryCoordinator])
    coordinator.coordinate()
  }
}
