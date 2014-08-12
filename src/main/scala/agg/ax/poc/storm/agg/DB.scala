package agg.ax.poc.storm.agg

import backtype.storm.tuple.Tuple

/**
 * Created by Swaroop on 10/08/14.
 */
trait DB {

  def save(tupleList: java.util.List[Tuple]):Set[Long]

}
