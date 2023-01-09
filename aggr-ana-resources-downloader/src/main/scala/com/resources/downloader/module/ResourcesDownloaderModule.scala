package com.resources.downloader.module

import com.eci.common.module.AnaplanModuleCommon

class ResourcesDownloaderModule extends AnaplanModuleCommon {

  override def configure(): Unit = {
    initialize("aggr-ana-resources-downloader")
  }
}
