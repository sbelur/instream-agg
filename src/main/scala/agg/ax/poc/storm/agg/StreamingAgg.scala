package agg.ax.poc.storm.agg

import java.util.Random
import scala.collection.immutable._
import backtype.storm.{Config, LocalCluster}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

/**
 * Created by Swaroop Belur on 09/08/14.
 */
object StreamingAgg {

  def main(args: Array[String]) = {



    val cluster: LocalCluster = new LocalCluster



    new Thread(new Runnable {
      def run {
        val builder: TopologyBuilder = new TopologyBuilder
        val spout = new AxMsgSpout(args(0))
        builder.setSpout("msgs", spout, 1)
        builder.setBolt("api", new ApiAggBolt, 2).fieldsGrouping("msgs", new Fields("apiproxy", "apiproduct"))
        builder.setBolt("saver", new CounterPersistor, 1).globalGrouping("api")
        doCollection(spout, args(1))
        val conf: Config = new Config
        conf.setDebug(false)
        val debug: AnyRef = Boolean.box(false)
        conf.put(Config.TOPOLOGY_DEBUG, debug)
        conf.setMaxTaskParallelism(3)
        cluster.submitTopology("ax-instream-apiaggregator", conf, builder.createTopology)
      }
    }).start



    Thread.sleep(20 * 60 * 1000)


    cluster.shutdown

  }

  private def doCollection(spout: AxMsgSpout, loop: String) {
    new Thread(new Runnable {
      def run {


        val random: Random = new Random

        var msgs: Seq[Map[String, String]] = Vector[Map[String, String]]()

        (1 to loop.toInt) foreach {

          case m => {




            var i: Int = 1
            (1 to 5) foreach {
              case  _ => {
                var entry = Map[String, String]()
                entry = entry.+("apiproxy" -> ("apiproxy" + random.nextInt(2)))
                entry = entry.+("apiproduct" -> ("apiproduct" + random.nextInt(2)))
                entry = entry.+("client_received_start_timestamp" -> System.currentTimeMillis.toString)
                entry = entry.+("target_response_time" -> ("" + random.nextInt(10) * 1L))
                entry = entry.+("total_response_time" -> ("" + random.nextInt(10) * 1L))
                entry = entry.+("request_size" -> ("" + random.nextInt(1000) * 1L))
                entry = entry.+("response_size" -> ("" + random.nextInt(1000) * 1L))
                entry = entry.+("is_error" -> ("" + random.nextInt(1)))
                entry = entry.+("cache_hit" -> ("" + random.nextInt(1)))
                msgs = msgs.:+(entry)
              }

            }


            println("INPUTmsgs set 1 " + msgs.size + " in loop "+m)
            println("\n\n\n\n\n")
            spout.onMessage(msgs)
            try {
              Thread.sleep(60000L)
              System.out.println("Sleep over")
            }
            catch {
              case e: InterruptedException => {
              }
            }
            msgs = Vector[Map[String, String]]()

            (1 to 5) foreach {
              case _ => {
                var entry: Map[String, String] = new HashMap[String, String]
                entry = entry.+("apiproxy" -> ("apiproxy" + random.nextInt(2).toString))
                entry = entry.+("apiproduct" -> ("apiproduct" + random.nextInt(2).toString))
                entry = entry.+("client_received_start_timestamp" -> System.currentTimeMillis.toString)
                entry = entry.+("target_response_time" -> ("" + random.nextInt(10) * 1L))
                entry = entry.+("total_response_time" -> ("" + random.nextInt(10) * 1L))
                entry = entry.+("request_size" -> ("" + random.nextInt(1000) * 1L))
                entry = entry.+("response_size" -> ("" + random.nextInt(1000) * 1L))
                entry = entry.+("is_error" -> random.nextInt(1).toString)
                entry = entry.+("cache_hit" -> random.nextInt(1).toString)
                msgs = msgs.:+(entry)
              }

            }

            println("INPUTmsgs set 2 " + msgs.size + " in loop "+m)
            println("\n\n\n\n\n")
            spout.onMessage(msgs)

            try {
              Thread.sleep(60000L)
              System.out.println("Sleep over")
            }
            catch {
              case e: InterruptedException => {
              }
            }
            msgs = Vector[Map[String, String]]()
          }
        }
      }
    }).start
  }

}
