package com.eci.anaplan.ic.paylater.r003

import com.eci.anaplan.ic.paylater.r003.module.IcPaylaterR003Module
import com.google.inject.Guice

object IcPaylaterR003Main {

  def main (args: Array[String]) = {
    val injector = Guice.createInjector(new IcPaylaterR003Module)
    val coordinator: IcPaylaterR003Coordinator = injector.getInstance(classOf[IcPaylaterR003Coordinator])
    coordinator.callCoordinate()
  }
}