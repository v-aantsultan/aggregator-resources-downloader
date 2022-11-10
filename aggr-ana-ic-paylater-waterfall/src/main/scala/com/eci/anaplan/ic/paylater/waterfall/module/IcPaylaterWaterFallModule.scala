package com.eci.anaplan.ic.paylater.waterfall.module

import com.eci.common.module.AnaplanModuleCommon

class IcPaylaterWaterFallModule extends AnaplanModuleCommon{

  override def configure(): Unit = {
    initialize("aggr-ana-ic-paylater-waterfall")
  }
}
