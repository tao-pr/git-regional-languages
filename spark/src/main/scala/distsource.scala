package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }

object DistDataSource {

  /**
   * Create an RDD from the specified JSON file
   * @param {SparkContext} underlying Spark context instance
   * @param {String} physical path to a JSON file containing distribution data
   */
  def readJSON(sc: SparkContext, path: String): SQLContext = {
    // Initialise a new SQL context from the underlying Spark context
    val sqlsc = new SQLContext(sc)

    // Read in the JSON file
    println(Console.CYAN + s"Reading JSON file : ${path}" + Console.RESET)
    val dists = sqlsc.jsonFile(path)

    // Set up a temporary table
    dists.registerTempTable("dists")
    sqlsc
  }

  /**
   * Get the list of the programming languages
   */
  def getLanguages(sqlsc: SQLContext): Array[Row] = {
    val langs = sqlsc.sql("SELECT lang._id from dists")
    langs.collect()
  }

  def getDistributionByLanguage(sqlsc: SQLContext): Array[Row] = {
    val dist = sqlsc.sql("SELECT lang._id as lang, coords from dists")
    // TAODEBUG:
    dist.show()
    dist.printSchema()

    dist.collect()
  }
}