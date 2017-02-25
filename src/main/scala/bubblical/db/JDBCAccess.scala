package bubblical.db

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Kirill on 2/18/2017.
  */
object JDBCAccess {

  import bubblical.config.Context.config

  private val connectionProperties = new Properties()
  connectionProperties.put("user", config.getString("db.username"))
  connectionProperties.put("password", config.getString("db.password"))
  connectionProperties.put("driver", "com.mysql.jdbc.Driver")

  private val mySqlURL = "jdbc:mysql://" + config.getString("db.host") + ":" + config.getString("db.port") + "/" + config.getString("db.schema")


  def read(spark: SparkSession, table: String): DataFrame = {
    val jdbcDF = spark.read
      .jdbc(mySqlURL, table, connectionProperties)
    jdbcDF
  }

  def write(df: DataFrame): Unit = {
    df.write
      .jdbc(mySqlURL, config.getString("db.bubbling_dev1" + ".Event"), connectionProperties)
  }

}
