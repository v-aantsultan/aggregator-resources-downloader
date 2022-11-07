package com.eci.anaplan.hs

import com.eci.anaplan.hs.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainHealthService {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: Coordinator = injector.getInstance(classOf[Coordinator])
    coordinator.coordinate()
  }
}
