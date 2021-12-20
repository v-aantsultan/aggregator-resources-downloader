package com.eci.anaplan.ins.nonauto

import com.eci.anaplan.ins.nonauto.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object Main {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: Coordinator = injector.getInstance(classOf[Coordinator])
    coordinator.coordinate()
  }
}
