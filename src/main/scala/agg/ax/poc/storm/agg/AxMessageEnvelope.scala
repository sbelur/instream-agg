package agg.ax.poc.storm.agg

import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import scala.collection.immutable._


/**
 * Created by Swaroop on 10/08/14.
 */
object AxMessageEnvelope {

  private var msgs: BlockingQueue[Seq[Map[String, String]]] = new LinkedBlockingQueue[Seq[Map[String, String]]]

  def offer(msgList: Seq[Map[String, String]]) {
    msgs.offer(msgList)
  }

  def getNext: Seq[Map[String, String]] = {
    msgs.poll
  }
}
