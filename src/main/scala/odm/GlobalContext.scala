package odm

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf


/**
 * Created by docker on 5/17/17.
 */
class GlobalContext extends Serializable {

  def createSparkConfig : SparkConf =
  {
    //Set up system properties from json config file

    System.setProperty("SPARK_YARN_MODE", "true")

    //Set up system properties from Global json config

    val config = new SparkConf()
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    config

  }

  def getFolderName ()={
     new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
  }

  val SConfig =createSparkConfig
  val NumPartitions : Int = 100
  val result_path : String = "maprfs:///datalake/uhclake/prd/developer/idemidov/results/"
  val result_folder = getFolderName()


Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)





}
