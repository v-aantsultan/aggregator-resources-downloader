package com.eci.anaplan

import com.eci.anaplan.module.GVIssuedModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainGVIssuedIDR {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new GVIssuedModule)
    val coordinator: GVIssuedCoordinator = injector.getInstance(classOf[GVIssuedCoordinator])
    coordinator.coordinate()
  }
}
