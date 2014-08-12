package agg.ax.poc.storm.agg

import backtype.storm.topology.base.BaseRichSpout
import java.util
import backtype.storm.task.TopologyContext
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import scala.collection.immutable._
import backtype.storm.tuple.{Values, Fields}
import java.util.{UUID, TimeZone, Calendar, Date}


/**
 * Created by Swaroop on 10/08/14.
 */
class AxMsgSpout(val sleep: String = "1000") extends BaseRichSpout with Consumer {

  private var spoutOutputCollector: SpoutOutputCollector = null

  //private var jmsMsgMap = Map[String,JMSMesg]()


  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("apiproxy", "apiproduct", "client_received_start_timestamp", "target_response_time", "total_response_time", "request_size", "response_size", "is_error", "cache_hit", "msgUUID"))
  }

  override def nextTuple(): Unit = {
    var msgList: scala.Seq[Map[String, String]] = AxMessageEnvelope.getNext

    if (msgList == null || msgList.isEmpty) {
      System.out.println("nothing to do - WILL SLEEP ...")
      suspend()
      msgList = AxMessageEnvelope.getNext
      if (msgList == null || msgList.isEmpty) {
        System.out.println("still nothing to do... ")
        return
      }
    }


    LogContainer.LOG.info("Processing a batch " + msgList.size)

    var ctr: Int = 0
    msgList foreach {
      case msg => {
        val proxy = msg.get("apiproxy").get
        val product = msg.get("apiproduct").get
        val time = msg.get("client_received_start_timestamp").get
        val calendar: Calendar = Calendar.getInstance
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"))
        calendar.setTimeInMillis(time.toLong)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val minute: Long = calendar.getTimeInMillis
        val target_response_time = msg.get("target_response_time").get
        val total_response_time = msg.get("total_response_time").get
        val request_size = msg.get("request_size").get
        val response_size = msg.get("response_size").get
        val is_error = msg.get("is_error").get
        val cache_hit = msg.get("cache_hit").get
        val aMessageId: UUID = UUID.randomUUID
        if (ctr == (msgList.size - 1)) {
          spoutOutputCollector.emit(new Values(proxy, product, time, target_response_time, total_response_time, request_size, response_size, is_error, cache_hit, aMessageId.toString), aMessageId)
          //jmsMsgMap = jmsMsgMap.+(aMessageId.toString-> new JMSMesg(aMessageId))
          LogContainer.LOG.info("Mapped jms msg to id " + aMessageId.toString + " for minute " + minute)
        }
        else {
          spoutOutputCollector.emit(new Values(proxy, product, time, target_response_time, total_response_time, request_size, response_size, is_error, cache_hit, ""), aMessageId)
        }
        ctr = ctr + 1;
      }


    }
  }

  override def open(p1: util.Map[_, _], p2: TopologyContext, p3: SpoutOutputCollector): Unit = {
    this.spoutOutputCollector = p3
  }


  private def suspend() {
    try {
      Thread.sleep(sleep.toInt)
    }
    catch {
      case e: InterruptedException => {
      }
    }
  }

  override def onMessage(msg: Seq[Map[String, String]]) {
    AxMessageEnvelope.offer(msg)
  }



  override def ack(msgId: AnyRef) {
    val date: Date = new Date
    LogContainer.LOG.info("ACKED message " + msgId + " at date " + date)
    // Ack JMS msg here
  }

  override def fail(msgId: AnyRef) {
    LogContainer.LOG.info("FAILED message " + msgId)

    // Close the session here
  }
}
