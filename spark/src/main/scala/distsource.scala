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
  def getLanguages(sqlsc: SQLContext, verbose: Boolean): Array[Row] = {
    val langs = sqlsc.sql("SELECT lang._id from dists")
    if (verbose) {
      println(Console.CYAN + "** LANGS **" + Console.RESET)
      langs.printSchema()
      langs.show()
    }

    langs.collect()
  }

  def getDistributionByLanguage(sqlsc: SQLContext, verbose: Boolean): DataFrame = {
    val dist = sqlsc.sql("SELECT lang._id as lang, coords from dists")
    if (verbose) {
      println(Console.CYAN + "** DISTS **" + Console.RESET)
      dist.show()
      dist.printSchema()
    }

    dist
  }
}