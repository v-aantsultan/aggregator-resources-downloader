package anaplan.ic.paylater.r001.module

import anaplan.SharedBaseTest
import com.eci.anaplan.ic.paylater.waterfall.module.IcPaylaterWaterFallModule
import com.eci.common.config.Environment.Environment
import com.eci.common.providers.{SlackClientProvider, SparkSessionProvider, StatusManagerClientProvider}
import com.google.inject.Guice

class IcPaylaterWaterFallModuleTest extends SharedBaseTest{

  "Configure" should "inject classes" in {
    val injector = Guice.createInjector(new IcPaylaterWaterFallModule)

    injector.getProvider(classOf[Environment])
    injector.getProvider(classOf[SparkSessionProvider])
    injector.getProvider(classOf[StatusManagerClientProvider])
    injector.getProvider(classOf[SlackClientProvider])
  }
}
