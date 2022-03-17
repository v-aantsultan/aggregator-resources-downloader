package com.eci.anaplan.wm.details

import com.eci.anaplan.wm.details.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainWealthManagementDetails {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: Coordinator = injector.getInstance(classOf[Coordinator])
    coordinator.coordinate()
  }
}
