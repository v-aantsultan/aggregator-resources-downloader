package com.eci.anaplan.train.nrd.details

import com.eci.anaplan.train.nrd.details.module.TrainNRDDetailsModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object TrainNRDDetailsMain {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new TrainNRDDetailsModule)
    val coordinator: TrainNRDDetailsCoordinator = injector.getInstance(classOf[TrainNRDDetailsCoordinator])
    coordinator.callCoordinate()
  }
}
