package agg.ax.poc.storm.agg

import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Tuple
import org.apache.commons.dbcp.BasicDataSource
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Timestamp
import java.util.{TimeZone, Calendar, Date}
import scala.collection.immutable.Set

/**
 * Created by Swaroop on 10/08/14.
 */
object PGPersistor extends DB{


  val TEMPLATE =
    """insert into  analytics."akka.env.agg_api"(timestamp,apiproxy,api_product,message_count,end_point_response_time,total_response_time,max_response_time,min_response_time,
      data_exchange_size,error_count,cache_hit) values (?,?,?,?,?,?,?,?,?,?,?)"""


  val ds:BasicDataSource =createDataSource()
  val jdbcTemplate = new JdbcTemplate(ds)


  override def save(tupleList: java.util.List[Tuple]): Set[Long] = {

    var minutes: Set[Long] = Set[Long]()


    jdbcTemplate.batchUpdate(TEMPLATE, new BatchPreparedStatementSetter() {

      override def getBatchSize: Int = {
        return tupleList.size
      }

      override def setValues(statement: PreparedStatement, j: Int) {
        val tuple: Tuple = tupleList.get(j)
        val tv: Long = tuple.getLong(0)
        minutes = minutes.+(tv); // todo check
        val date: Date = new Date
        statement.setTimestamp(1, new Timestamp(tv), Calendar.getInstance(TimeZone.getTimeZone("GMT")))
        val proxy: String = tuple.getString(1)
        statement.setString(2, proxy)
        val product: String = tuple.getString(2)
        statement.setString(3, product)
        val mc: Long = tuple.getLong(3)
        statement.setLong(4, mc)
        val endPointResponseTime: Long = tuple.getLong(4)
        statement.setLong(5, endPointResponseTime)
        val totalResponseTime: Long = tuple.getLong(5)
        statement.setLong(6, totalResponseTime)
        val maxResponseTime: Long = tuple.getLong(6)
        statement.setLong(7, maxResponseTime)
        val minResponseTime: Long = tuple.getLong(7)
        statement.setLong(8, minResponseTime)
        val dataexsize: Long = tuple.getLong(8)
        statement.setLong(9, dataexsize)
        val errors: Long = tuple.getLong(9)
        statement.setLong(10, errors)
        val cache: Integer = tuple.getInteger(10)
        statement.setInt(11, cache)
      }
    })


    minutes
  }

  private def createDataSource(): BasicDataSource = {
    val dataSource = new BasicDataSource()
    dataSource.setUrl("jdbc:postgresql://<HOST>:5432/<db>")
    dataSource.setUsername("<user>")
    dataSource.setPassword("<password>")
    dataSource.setInitialSize(5)
    dataSource.setMaxActive(10)
    dataSource.setMaxIdle(1)
    dataSource.setMaxWait(20000)
    dataSource.setValidationQuery("select 1")
    dataSource.setTestOnReturn(true)
    dataSource.setPoolPreparedStatements(true)
    dataSource.setDriverClassName("org.postgresql.Driver")
    dataSource
  }
}
