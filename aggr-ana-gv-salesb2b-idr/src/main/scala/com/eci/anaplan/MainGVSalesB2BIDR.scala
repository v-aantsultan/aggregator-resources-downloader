package com.eci.anaplan

import com.eci.anaplan.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainGVSalesB2BIDR {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: GVB2BCoordinator = injector.getInstance(classOf[GVB2BCoordinator])
    coordinator.coordinate()
  }
}
