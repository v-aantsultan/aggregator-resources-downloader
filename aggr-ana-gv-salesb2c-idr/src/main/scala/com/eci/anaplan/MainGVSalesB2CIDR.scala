package com.eci.anaplan

import com.eci.anaplan.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainGVSalesB2CIDR {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: GVB2CCoordinator = injector.getInstance(classOf[GVB2CCoordinator])
    coordinator.coordinate()
  }
}
