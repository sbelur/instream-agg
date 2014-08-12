package agg.ax.poc.storm.agg

import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.task.{TopologyContext, OutputCollector}
import java.util.Date
import backtype.storm.tuple.Tuple
import collection.immutable._
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.JavaConverters._
import backtype.storm.topology.OutputFieldsDeclarer
import java.util

/**
 * Created by Swaroop on 10/08/14.
 */
class CounterPersistor extends BaseRichBolt {
  private var outputCollector: OutputCollector = _
  private var persistorThread: PersistorThread = _
  private var markerTuples: Seq[Tuple] = Vector[Tuple]()
  private val MARKER: String = "_marker_"

  override def prepare(p1: util.Map[_, _], p2: TopologyContext, p3: OutputCollector): Unit = {
    persistorThread = new PersistorThread(10)
    persistorThread.start()
    this.outputCollector = p3

  }

  override def execute(tuple: Tuple) {
    if (MARKER == tuple.getString(1)) {
      markerTuples = markerTuples.:+(tuple)
    }
    else {
      persistorThread.offer(tuple)
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {
  }

  private class PersistorThread extends Thread {


    private var batchMaxSize: Int = 0
    private var queue: LinkedBlockingQueue[Tuple] = new LinkedBlockingQueue[Tuple]

    def this(batchMaxSize: Int) = {
      this()
      this.batchMaxSize = batchMaxSize
      setDaemon(true);
    }


    override def run() = {
      while (true) {
        try {
          var d: Date = new Date
          Thread.sleep(40000)
          d = new Date
        }
        catch {
          case ignore: InterruptedException => {
          }
        }
        var batch: util.List[Tuple] = new util.ArrayList[Tuple]()
        var allMins: Set[Long] = null
        try {
          val t: Tuple = queue.take
          batch.add(t)
          queue.drainTo(batch, batchMaxSize)
          allMins = save(batch)
          onSuccess(batch, allMins)
        }
        catch {
          case e: Exception => {
            onError(batch, allMins, e)
          }
        }
      }
    }

    private def onSuccess(batch: java.util.List[Tuple], allMins: Set[Long]) {

      batch.asScala foreach {
        case tupleToAck => {
          outputCollector.ack(tupleToAck)
        }
      }

      markerTuples foreach {
        case marker => {
          outputCollector.ack(marker)
        }
      }

      allMins foreach {
        case aMin => {
          val anchoredTuplesList: Seq[Tuple] = findOriginatingTuples(aMin)

          anchoredTuplesList foreach {
            case tupleToAck => {

              val d1: Date = new Date
              outputCollector.ack(tupleToAck)
              val d2: Date = new Date
            }
          }
        }
      }
    }

    private def findOriginatingTuples(aMin: Long): Seq[Tuple] = {
      var reqd: Seq[Tuple] = Vector[Tuple]()

      markerTuples foreach {
        case t => {
          if (t.getLong(0) == aMin) {
            val tuple = t.getValue(11)
            tuple match {
              case Some(x:Tuple) => reqd = reqd.:+ (x)
            }
          }
        }
      }
      reqd
    }

    private def onError(batch: java.util.List[Tuple], allMins: Set[Long], e: Exception) {
      batch.asScala foreach {
        case tupleToAck => {
          outputCollector.fail(tupleToAck)
        }
      }
      markerTuples foreach {
        case marker => {
          outputCollector.fail(marker)
        }
      }

      allMins foreach {
        case aMin => {
          val anchoredTuplesList: Seq[Tuple] = findOriginatingTuples(aMin)
          for (tupleToAck <- anchoredTuplesList) {
            val d1: Date = new Date
            outputCollector.fail(tupleToAck)
            val d2: Date = new Date
          }
        }
      }
    }

    private def save(batch: util.List[Tuple]): Set[Long] = {
      val minutesSaved: Set[Long] = PGPersistor.save(batch)
      LogContainer.LOG.info("Saved batch -- no of msgs " + batch.size)
      return minutesSaved
    }

    private[CounterPersistor] def offer(tuple: Tuple) {
      queue.offer(tuple)
    }


  }


}
