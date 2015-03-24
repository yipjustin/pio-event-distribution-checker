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
import org.apache.spark.rdd.PairRDDFunctions
import org.saddle.Series
import org.saddle.Frame
import org.saddle.stats.SeriesExpandingStats

import scala.reflect.ClassTag

class EventRDD(self: RDD[Event]) {
  def countAndSample[K](keyFunc: Event => K, sample: Int)
                       (implicit kt: ClassTag[K]): RDD[(K, AggEvent.Result)] = {
    AggEvent.distribution(self, keyFunc, sample)
  }
}

class KeyedEventRDD[K](self: RDD[(K, Event)]) {
  def countAndSample(sample: Int)
                    (implicit kt: ClassTag[K]): RDD[(K, AggEvent.Result)] = {
    self.aggregateByKey(AggEvent.z)(AggEvent.seqop(sample), AggEvent.combop(sample))
  }
}

object AggEvent {
  @transient lazy val logger = Logger[this.type]

  implicit def rddToEventRDD(rdd: RDD[Event]): EventRDD = new EventRDD(rdd)
  
  implicit def rddToKeyedEventRDD[K](rdd: RDD[(K, Event)]): KeyedEventRDD[K] = {
    new KeyedEventRDD[K](rdd)
  }

  type Result = (Int, Seq[Event])

  val z: Result = (0, IndexedSeq[Event]())

  def seqop(sample: Int): (Result, Event) => Result = {
    (u: (Int, Seq[Event]), v: Event) => {
      if (u._2.size >= sample) {
        (u._1 + 1, u._2)
      } else {
        (u._1 + 1, u._2 :+ v)
      }
    }
  }

  def combop(sample: Int): (Result, Result) => Result = {
    (u: Result, v: Result) => {
      (u._1 + v._1, (u._2 ++ v._2).take(sample))
    }
  }

  def distribution[K](events: RDD[Event], keyFunc: Event => K, sample: Int)
  (implicit kt: ClassTag[K])
  : RDD[(K, Result)] = {
    new PairRDDFunctions(events.map(e => (keyFunc(e), e)))
    .aggregateByKey(z)(seqop(sample), combop(sample))
  }

  def print[K](m: Seq[(K, Result)], sortKey: Boolean = false) {
    val frame = EventUtils.distStats(m.map(e => (e._1, e._2._1)), sortKey)

    logger.info(frame.stringify(frame.numRows))

    m.foreach { case (key, (count, events)) => 
      if (events.size > 0) { 
        logger.info(s"Key: $key")
        events.foreach { e => logger.info(s"  $e") }
      }
    }
  }

}



object EventUtils {

  def entityTypeDistribution(events: RDD[Event])
  : RDD[((String, Option[String]), Int)] = {
    events
    .map(e => ((e.entityType, e.targetEntityType), e))
    .aggregateByKey(0)( (u, e) => u + 1, (u, v) => u + v )
  }

  def distribution[K](events: RDD[Event], keyFunc: Event => K)
  (implicit kt: ClassTag[K])
  : RDD[(K, Int)] = {
    new PairRDDFunctions(events.map(e => (keyFunc(e), e)))
    .aggregateByKey(0)((u, e) => u + 1, (u, v) => u + v)
  }

  def distinctEntityId[K](events: RDD[Event], keyFunc: Event => K)
  (implicit kt: ClassTag[K]): RDD[(K, Int)] = {

    new PairRDDFunctions(events.map(e => (keyFunc(e), e)))
    .combineByKey[Set[String]](
      (e: Event) => Set(e.entityId),
      (s: Set[String], e: Event) => s + e.entityId,
      (s0: Set[String], s1: Set[String]) => s0 ++ s1)
    .mapValues(_.size)
  }

  def distStats[K](m: Seq[(K, Int)], sortKey: Boolean = false)
  : Frame[String, String, Double] = {
    val s = m.map(_._2).sum

    val sorted: Seq[(String, Double)] = (
      if (sortKey) (
        m.map{ case(k, v) => (k.toString, v.toDouble) }.sorted 
      ) else (
        m.map{ case(k, v) => (k.toString, v.toDouble) } 
      )
    )

    val count = Series(sorted:_*)

    val freq = count / s

    val cumCount = SeriesExpandingStats(count).cumSum
    val cumFreq = cumCount / s

    val f = Frame(
      "Count" -> count, 
      "Freq" -> freq,
      "CumCount" -> cumCount,
      "CumFreq" -> cumFreq
    )
    f
  }

}

