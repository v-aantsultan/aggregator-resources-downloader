package com.eci.common.providers

import com.eci.common.config.StatusDbConfig
import com.google.inject.Provider
import com.traveloka.eci.statusmanager.client.{DefaultStatusDbClientFactory, StatusManagerClient}

import javax.inject.Inject

class StatusManagerClientProvider @Inject()(statusDbConfig: StatusDbConfig) extends Provider[StatusManagerClient] {
  def get(): StatusManagerClient = {
    DefaultStatusDbClientFactory.create(statusDbConfig.statusManagerConfig)
  }
}
