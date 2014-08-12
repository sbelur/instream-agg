package agg.ax.poc.storm.agg

import scala.collection.immutable._
/**
 * Created by Swaroop on 10/08/14.
 */
trait Consumer {
  def onMessage(msg: Seq[Map[String, String]])={}
}
