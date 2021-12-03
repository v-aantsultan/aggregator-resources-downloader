package com.eci.anaplan

import com.eci.anaplan.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainLPDetails {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: LPDetailsCoordinator = injector.getInstance(classOf[LPDetailsCoordinator])
    coordinator.coordinate()
  }
}
