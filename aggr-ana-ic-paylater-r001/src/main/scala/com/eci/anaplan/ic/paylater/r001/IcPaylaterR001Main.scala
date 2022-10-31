package com.eci.anaplan.ic.paylater.r001

import com.eci.anaplan.ic.paylater.r001.module.IcPaylaterR001Module
import com.google.inject.Guice

object IcPaylaterR001Main {

  def main (args: Array[String]) = {
    val injector = Guice.createInjector(new IcPaylaterR001Module)
    val coordinator: IcPaylaterR001Coordinator = injector.getInstance(classOf[IcPaylaterR001Coordinator])
    coordinator.callCoordinate()
  }

}
