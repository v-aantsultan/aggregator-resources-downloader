package com.eci.anaplan

import com.eci.anaplan.module.GVRedeemModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object MainGVRedeemIDR {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new GVRedeemModule)
    val coordinator: GVRedeemCoordinator = injector.getInstance(classOf[GVRedeemCoordinator])
    coordinator.coordinate()
  }
}
