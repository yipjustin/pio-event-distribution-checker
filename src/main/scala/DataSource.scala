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


case class DataSourceParams(
    appId: Int,
    sample: Option[Int]
) extends Params


abstract class AbstractChecker extends Serializable {
  def check(): Unit
} 

class BasicChecker(ds: DataSource, eventsRDD: RDD[Event], sample: Option[Int])
    extends AbstractChecker {
  @transient lazy val logger = Logger[this.type]

  import io.prediction.e2.eventdistributionchecker.AggEvent.rddToEventRDD
  import io.prediction.e2.eventdistributionchecker.AggEvent.rddToKeyedEventRDD
  
  val _sample = sample.getOrElse(0)
  
  val fTypeDist: FutureAction[Seq[((String, Option[String]), AggEvent.Result)]] =
  eventsRDD
  .map(e => ((e.entityType, e.targetEntityType), e))
  .countAndSample(_sample)
  .collectAsync()

  val fEventTimeDist = eventsRDD
    .map{ e => (e.eventTime.toString("yyyy-MM"), e) }
    .countAndSample(_sample)
    .collectAsync()

  val fEventTimeNameDist = eventsRDD
    .map{ e => ((e.eventTime.toString("yyyy-MM"), e.event), e) }
    .countAndSample(_sample)
    .collectAsync()

  val fEventTimeNameTypeDist = eventsRDD
    .map{ e => 
      ((e.eventTime.toString("yyyy-MM"), e.event, e.entityType, e.targetEntityType), e)
    }
    .countAndSample(_sample)
    .collectAsync()
  
  val fTypeDistinctId = EventUtils.distinctEntityId(
    eventsRDD, _.entityType).collectAsync()

  def check(): Unit = {
    logger.info("Entity Type Distribution")
    AggEvent.print(fTypeDist.get(), true)


    logger.info("Event Time Distribution")
    AggEvent.print(fEventTimeDist.get(), true)

    logger.info("Event (Time, Name, EntityType, TargetEntityType) Distribution")
    AggEvent.print(fEventTimeNameTypeDist.get(), true)
    
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
      .setName("EventsRDD")
      .cache

    logger.info(s"EventsCount: ${eventsRDD.count}")

    val checkers = Seq[AbstractChecker](
      new BasicChecker(this, eventsRDD, sample=dsp.sample)
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
