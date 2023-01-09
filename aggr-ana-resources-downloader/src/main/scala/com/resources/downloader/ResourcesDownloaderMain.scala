package com.resources.downloader

import com.google.inject.{Guice, Injector}
import com.resources.downloader.module.ResourcesDownloaderModule
import com.resources.downloader.util.ObjectConstructors

object ResourcesDownloaderMain {

  def main (args:Array[String]) = {
    val injector : Injector = Guice.createInjector(new ResourcesDownloaderModule)
    val coordinator : ResourcesDownloaderCoordinator = injector.getInstance(classOf[ResourcesDownloaderCoordinator])
    coordinator.callCoordinate()
  }


}
