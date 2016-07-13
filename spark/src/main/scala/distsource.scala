package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }

/**
 * Geospatial distribution data source, responsible for following:
 * [x] - Facilitates the JSON data source
 * [x] - Data wrangling
 */
object DistDataSource {

  /**
   * Create an RDD from the specified JSON file
   * @param {SparkContext} underlying Spark context instance
   * @param {String} physical path to a JSON file containing distribution data
   */
  def readJSON(sc: SparkContext, path: String): SQLContext = {
    // Initialise a new SQL context from the underlying Spark context
    val sqlctx = new SQLContext(sc)

    // Read in the JSON file
    println(Console.CYAN + s"Reading JSON file : ${path}" + Console.RESET)
    val dists = sqlctx.jsonFile(path)

    // Set up a temporary table
    dists.registerTempTable("dists")
    sqlctx
  }

  /**
   * Get the list of the programming languages
   */
  def getLanguages(sqlctx: SQLContext, verbose: Boolean): Array[Row] = {
    val langs = sqlctx.sql("SELECT lang._id from dists")
    if (verbose) {
      println(Console.CYAN + "** LANGS **" + Console.RESET)
      langs.printSchema()
      langs.show()
    }

    langs.collect()
  }

  /**
   * Get the raw distribution of code contribution by language
   * over geospatial regions
   */
  def getDistributionByLanguage(sqlctx: SQLContext, verbose: Boolean): DataFrame = {
    val dist = sqlctx.sql("SELECT lang._id as lang, coords from dists")
    if (verbose) {
      println(Console.CYAN + "** DISTS **" + Console.RESET)
      dist.show()
      dist.printSchema()
    }

    dist
  }
}