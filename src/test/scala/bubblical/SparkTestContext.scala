package bubblical

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by kirill on 29/04/17.
  */
trait SparkTestContext {
  private val spConf = new SparkConf(true).setMaster("local[*]").setAppName("Bubblical test suite")

  val spark = SparkSession.builder().config(spConf).getOrCreate()

  val sc = spark.sparkContext

  def toDF[T <: Product: ClassTag: TypeTag](sequence: Seq[T]) = {
    import spark.implicits._
    sc.parallelize(sequence).toDF()
  }

  def toDF[T <: Product: ClassTag: TypeTag](sequence: Seq[T], columnNames: Seq[String]) = {
    import spark.implicits._
    sc.parallelize(sequence).toDF(columnNames: _*)
  }
}
