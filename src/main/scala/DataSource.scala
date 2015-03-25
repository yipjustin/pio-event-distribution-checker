package io.prediction.e2.eventdistributionchecker

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage
import io.prediction.workflow.StopAfterReadInterruption

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.FutureAction
import org.apache.spark.rdd.AsyncRDDActions
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.Accumulator
import org.apache.spark.Accumulable

import grizzled.slf4j.Logger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.Vectors
import org.joda.time.Days
import org.joda.time.Hours
import scala.reflect.ClassTag

case class DataSourceParams(
    appId: Int,
    sample: Option[Int],  // # of sample events for each key
    dateKey: Option[String]  // define the granularity of date. Default is yyyy-MM
) extends Params


abstract class AbstractChecker extends Serializable {
  def check(): Unit
} 

class BasicChecker(dsp: DataSourceParams, inputEventsRDD: RDD[Event])
    extends AbstractChecker {
  @transient lazy val logger = Logger[this.type]

  import io.prediction.e2.eventdistributionchecker.AggEvent.rddToEventRDD
  import io.prediction.e2.eventdistributionchecker.AggEvent.rddToKeyedEventRDD

  // Only take whatever needed for this checker, and cache it.
  val eventsRDD: RDD[Event] = inputEventsRDD.map { e => 
    new Event(
      eventId = e.eventId,
      event = e.event,
      entityType = e.entityType,
      entityId = e.entityId,
      targetEntityType = e.targetEntityType,
      targetEntityId = e.targetEntityId,
      eventTime = e.eventTime,
      prId = e.prId,
      creationTime = e.creationTime)
  }
  .setName("BasicChecker.eventsRDD")
  
  val _sample = dsp.sample.getOrElse(0)
  val sample = _sample
  val dateKey = dsp.dateKey.getOrElse("yyyy-MM")

  @transient val fDist = eventsRDD
    .flatMap { e => Seq(
      ("1. Total", e),
      (("2. Type", e.entityType, e.targetEntityType), e),
      (("3. Time", e.eventTime.toString(dateKey)), e),
      (("4. TimeNameType", e.eventTime.toString(dateKey), e.event, 
        e.entityType, e.targetEntityType), e)) }
    .countAndSample(_sample)
    .collectAsync()
  
  @transient val fTypeDistinctId = EventUtils.distinctEntityId(
    eventsRDD, _.entityType).collectAsync()

  def check(): Unit = {
    logger.info("Event Distribution")
    AggEvent.print(fDist.get(), true)
    
    logger.info("Event (EntityType) Distinct Id")
    val typeDistinctIdStats = EventUtils.distStats(fTypeDistinctId.get(), true)
    logger.info(typeDistinctIdStats)
  }
}


class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]
  @transient lazy val eventsDb = Storage.getPEvents()

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsRDD: RDD[Event] = eventsDb.find(appId = dsp.appId)(sc)

    val checkers = Seq[AbstractChecker](
      new BasicChecker(dsp, eventsRDD)
    )

    checkers.foreach(_.check)

    throw new StopAfterReadInterruption()
  }
}

class TrainingData(
  val events: RDD[Event]
) extends Serializable {
  override def toString = {
    s"events: [${events.count()}] (${events.take(2).toList}...)"
  }
}
