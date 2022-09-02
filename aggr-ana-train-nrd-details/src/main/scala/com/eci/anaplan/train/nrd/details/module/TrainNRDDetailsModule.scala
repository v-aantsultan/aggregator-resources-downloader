package com.eci.anaplan.train.nrd.details.module

import com.eci.common.module.AnaplanModuleCommon

/**
 * Dependency Injection module initialisation class
 */
class TrainNRDDetailsModule extends AnaplanModuleCommon {
  override def configure(): Unit = {
    initialize("aggr-ana-train-nrd-details")
  }
}
