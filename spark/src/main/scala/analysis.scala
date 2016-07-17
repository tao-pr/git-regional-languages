package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame }
import scala.collection.mutable.Map

object Analysis {

  /**
   * Accumulate the entire distribution map of the code contribution
   * by all languages in the current data frame and create a mapping
   * which maps: [Exact geospatial location of the centroid] => [Amount of code contribution in total]
   * @param {SQLContext}
   * @param {DataFrame} Filtered distribution dataset
   * @return {Map} [SpatialLocation] => [Float]
   */
  def accumGlobalDists(sqlctx: SQLContext, dists: DataFrame): Map[SpatialLocation, Float] = {
    import sqlctx.implicits._
    val contrib = dists.select($"coords").collect()

    // Create a mapping where geolocation plays as keys
    val map = Map.empty[SpatialLocation, Float]

    // TAOTODO: Consider using `groupBy`?
    contrib.foreach { (dataArray) =>
      val lat = dataArray(0).asInstanceOf[Double]
      val lng = dataArray(1).asInstanceOf[Double]
      val density = dataArray(2).asInstanceOf[Float]

      val loc = SpatialLocation(lat, lng)
      map(loc) = density + map(loc)
    }
    map
  }

  /**
   * Compute a bin vector from the given geospatial distribution data
   * @param {SQLContext}
   * @param {DataFrame} current distribution data
   */
  def geodistToBins(sqlctx: SQLContext, dists: DataFrame): Map[Int, Float] = {
    // TAOTODO:
  }
}