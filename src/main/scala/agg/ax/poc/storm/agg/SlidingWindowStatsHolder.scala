package agg.ax.poc.storm.agg

import backtype.storm.tuple.{Values, Tuple}
import java.util.concurrent.ConcurrentHashMap
import java.io.Serializable
import scala.collection.immutable._
import scala.collection.JavaConverters._
import java.lang.Long

/**
 * Created by Swaroop on 10/08/14.
 */
object SlidingWindowStatsHolder extends Serializable {

  import scala.collection.convert.decorateAsScala._


  var stats = (new ConcurrentHashMap[String, MinuteBucket]()).asScala

  case class MinuteBucket(time: Long) {
    private val KEY_SEP = "_$$_"
    private val ESCAPED_KEY_SEP = "_\\$\\$_"
    private final val stats = new ConcurrentHashMap[String, Counters].asScala

    final def forKey(proxy: String, product: String): Option[Counters] = {
      stats.putIfAbsent(proxy + KEY_SEP + product, new Counters)
      stats.get(proxy + KEY_SEP + product)
    }

    final def forEach(emitter: (Values) => Void) {
      stats foreach {
        case (timeKey: String, bucket: Counters) => {
          val temp = timeKey.split(ESCAPED_KEY_SEP)
          val proxy: String = temp(0)
          val product: String = temp(1)
          emitter(new Values(time, proxy, product, bucket.mc, bucket.endPointResponseTime, bucket.totalResponseTime, bucket.maxResponseTime, bucket.minResponseTime, bucket.dataexsize, bucket.errors, bucket.cache, None))
        }
      }

    }

    class Counters {
      var mc: java.lang.Long = 0L
      var endPointResponseTime: java.lang.Long = 0L
      var totalResponseTime: java.lang.Long = 0L
      var maxResponseTime: java.lang.Long = Long.MIN_VALUE
      var minResponseTime: java.lang.Long = Long.MAX_VALUE
      var dataexsize: java.lang.Long = 0L
      var cache: java.lang.Integer = 0
      var errors: java.lang.Long = 0L


      private def incrMc(): java.lang.Long = {
        mc = mc + 1L
        mc
      }

      private def sumEPRT(targetrt: Long) {
        endPointResponseTime = endPointResponseTime + targetrt
      }

      private def sumTRT(totalrt: Long) {
        totalResponseTime = totalResponseTime + totalrt
      }

      private def maxRT(totalrt: Long) {
        if (totalrt > maxResponseTime) maxResponseTime = totalrt
      }

      private def minRT(totalrt: Long) {
        if (totalrt < minResponseTime) minResponseTime = totalrt
      }

      private def sumDES(req: Long, res: Long) {
        dataexsize = dataexsize + req + res
      }

      private def sumErrors(err: Long) {
        errors = errors + err
      }

      final def sumCaches(c: Int) {
        cache = cache + c
      }


      def updateStats(tuple: Tuple): Boolean = {
        this.synchronized {
          val mc = incrMc
          sumEPRT(tuple.getString(3).toLong)
          sumTRT(tuple.getString(4).toLong)
          maxRT(tuple.getString(4).toLong)
          minRT(tuple.getString(4).toLong)
          sumDES(tuple.getString(5).toLong, tuple.getString(6).toLong)
          sumErrors(tuple.getString(7).toLong)
          sumCaches(tuple.getString(8).toInt)
          val msgID: String = tuple.getString(9)
          var ack = true
          if (!msgID.isEmpty) {
            ack = false
          }
          ack
        }
      }
    }


  }

  def bindTime(time: String, minute: Long): Option[MinuteBucket] = {
    stats.putIfAbsent(time, new MinuteBucket(minute))
    stats.get(time)
  }

  def filter(time: String): Seq[MinuteBucket] = {
    this.synchronized {
      var seen: Set[String] = Set[String]()
      var buckets: Seq[MinuteBucket] = Vector[MinuteBucket]()

      stats foreach {
        case (timeKey: String, bucket: MinuteBucket) if (timeKey != time) => {
          buckets = buckets.:+(bucket)
          seen = seen.+(timeKey)
        }
        case _ =>

      }
      val temp = stats.filter({
        case (timeKey: String, bucket: MinuteBucket) => {
          seen.contains(timeKey);
        }
      })

      for ((k, v) <- temp) {
        stats.remove(k)
      }

      buckets
    }
  }

}
