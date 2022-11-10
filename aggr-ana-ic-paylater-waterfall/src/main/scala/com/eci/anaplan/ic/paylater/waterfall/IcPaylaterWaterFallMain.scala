package com.eci.anaplan.ic.paylater.waterfall

import com.eci.anaplan.ic.paylater.waterfall.module.IcPaylaterWaterFallModule
import com.google.inject.Guice

object IcPaylaterWaterFallMain {

  def main (args: Array[String]) = {
    val injector = Guice.createInjector(new IcPaylaterWaterFallModule)
    val coordinator: IcPaylaterWaterFallCoordinator = injector.getInstance(classOf[IcPaylaterWaterFallCoordinator])
    coordinator.callCoordinate()
  }
}