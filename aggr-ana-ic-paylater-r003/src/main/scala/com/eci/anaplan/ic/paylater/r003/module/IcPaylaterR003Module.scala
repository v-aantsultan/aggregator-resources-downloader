package com.eci.anaplan.ic.paylater.r003.module

import com.eci.common.module.AnaplanModuleCommon

class IcPaylaterR003Module extends AnaplanModuleCommon{

  override def configure(): Unit = {
    initialize("aggr-ana-ic-paylater-r003")
  }
}
