package com.eci.anaplan

import com.eci.anaplan.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainLPMutationIDR {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: LPMutationCoordinator = injector.getInstance(classOf[LPMutationCoordinator])
    coordinator.coordinate()
  }
}
