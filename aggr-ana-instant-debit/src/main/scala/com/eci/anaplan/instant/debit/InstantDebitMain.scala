package com.eci.anaplan.instant.debit

import com.eci.anaplan.instant.debit.module.AnaplanModule
import com.google.inject.Guice

/**
 * The main class for anaplan report domain
 */
object InstantDebitMain {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(new AnaplanModule)
    val coordinator: Coordinator = injector.getInstance(classOf[Coordinator])
    coordinator.coordinate()
  }
}
