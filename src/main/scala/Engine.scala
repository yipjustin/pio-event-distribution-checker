package io.prediction.e2.eventdistributionchecker

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(q: String) extends Serializable

case class PredictedResult(p: String) extends Serializable

object VanillaEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("" -> classOf[Algorithm]),
      classOf[Serving])
  }
}
