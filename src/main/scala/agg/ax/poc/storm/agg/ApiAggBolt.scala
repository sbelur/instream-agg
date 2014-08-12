package agg.ax.poc.storm.agg

import backtype.storm.topology.base.BaseRichBolt
import java.util
import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.tuple.{Values, Fields, Tuple}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.{Constants, Config}
import java.util.{TimeZone, Calendar, Date}
import scala.collection.immutable.Seq
import agg.ax.poc.storm.agg.SlidingWindowStatsHolder.MinuteBucket

/**
 * Created by Swaroop on 10/08/14.
 */
final class ApiAggBolt extends BaseRichBolt {

  val MARKER = "_marker_"
  var outputCollector: OutputCollector = _





  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("time", "apiproxy", "apiproduct", "mc", "endPointResponseTime", "totalResponseTime", "maxResponseTime", "minResponseTime", "dataexsize", "errors", "cache", "srctuple"))
  }

  override def execute(tuple: Tuple): Unit = {
    if (isTickTuple(tuple)) {
      val d: Date = new Date
      val calendar: Calendar = Calendar.getInstance
      calendar.setTimeZone(TimeZone.getTimeZone("GMT"))
      calendar.setTime(d)
      val uptoMin: String = "" + calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1) + "-" + calendar.get(Calendar.DATE) + " " + calendar.get(Calendar.HOUR_OF_DAY) + ":" + (calendar.get(Calendar.MINUTE) - 1)
      val minuteBuckets: Seq[MinuteBucket] = SlidingWindowStatsHolder.filter(uptoMin)
      minuteBuckets foreach {
        case minuteBucket => {
          minuteBucket forEach {
            case x: Values => {
              outputCollector.emit(x)
              null
            }
          }
        }
      }
    }
    else {
      val proxy: String = tuple.getString(0)
      val product: String = tuple.getString(1)
      val time: String = tuple.getString(2)
      val lTime: Long = time.toLong
      val d: Date = new Date(lTime)
      val calendar: Calendar = Calendar.getInstance
      calendar.setTimeZone(TimeZone.getTimeZone("GMT"))
      calendar.setTime(d)
      val uptoMin: String = "" + calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1) + "-" + calendar.get(Calendar.DATE) + " " + calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      val minute: Long = calendar.getTimeInMillis
      val ack: Option[Boolean] = SlidingWindowStatsHolder.bindTime(uptoMin, minute).flatMap {
        minuteBucket => minuteBucket.forKey(proxy, product).map {
          counters => counters.updateStats(tuple)
        }
      }

      ack match {
        case Some(true) => outputCollector.ack(tuple)
        case _ => {
          outputCollector.emit(tuple, new Values(minute:java.lang.Long, MARKER, MARKER, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, 0: java.lang.Integer, Some(tuple)))
          LogContainer.LOG.info("Emitted marker tuple for tuple " + tuple.getString(9) + " for minute " + minute)
        }
      }

    }
  }

  override def prepare(p1: util.Map[_, _], p2: TopologyContext, p3: OutputCollector): Unit = {
    this.outputCollector = p3
  }

  override def getComponentConfiguration = {
    val conf: Config = new Config
    val tickFrequencyInSeconds: Integer = 10
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds)
    conf
  }

  private def isTickTuple(tuple: Tuple): Boolean = {
    (tuple.getSourceComponent == Constants.SYSTEM_COMPONENT_ID) && (tuple.getSourceStreamId == Constants.SYSTEM_TICK_STREAM_ID)
  }
}
