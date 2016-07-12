package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame }

object DistDataSource {

  /**
   * Create an RDD from the specified JSON file
   * @param {SparkContext} underlying Spark context instance
   * @param {String} physical path to a JSON file containing distribution data
   */
  def readJSON(sc: SparkContext, path: String): DataFrame = {
    // Initialise a new SQL context from the underlying Spark context
    val sqlsc = new SQLContext(sc)

    // Read in the JSON file
    println(Console.CYAN + s"Reading JSON file : ${path}" + Console.RESET)
    val dists = sqlsc.jsonFile(path)

    // Set up a temporary table
    dists.registerTempTable("dists")
    dists
  }

  /**
   * Get the full list of the languages from the underlying SQL context
   * @param {SQLContext}
   */
  def getLanguageList(sqlsc: SQLContext): List[String] = {

    // TAOTODO:
    List()
  }
}