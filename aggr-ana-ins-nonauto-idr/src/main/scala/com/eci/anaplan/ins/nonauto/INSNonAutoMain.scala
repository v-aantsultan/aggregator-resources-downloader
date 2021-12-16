package com.eci.anaplan.ins.nonauto

import com.eci.anaplan.ins.nonauto.module.INSNonAutoModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object INSNonAutoMain {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new INSNonAutoModule)
    val coordinator: INSNonAutoCoordinator = injector.getInstance(classOf[INSNonAutoCoordinator])
    coordinator.coordinate()
  }
}
