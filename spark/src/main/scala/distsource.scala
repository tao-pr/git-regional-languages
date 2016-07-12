package gitdistsource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DistDataSource {

  /**
   * Create an RDD from the specified JSON file
   * @param {SparkContext} underlying Spark context instance
   * @param {String} physical path to a JSON file containing distribution data
   */
  def readJSON(sc: SparkContext, path: String) {
    // Initialise a new SQL context from the underlying Spark context
    val sqlsc = new SQLContext(sc)

    // Read in the JSON file
    val dists = sqlsc.jsonFile(path)

    // Set up a temporary table
    dists.registerTempTable("dists")
    dists
  }
}