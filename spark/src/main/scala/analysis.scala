package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame }
import scala.collection.mutable.{ Map, WrappedArray }

object Analysis {

  /**
   * Accumulate the entire distribution map of the code contribution
   * by all languages in the current data frame and create a mapping
   * which maps: [Exact geospatial location of the centroid] => [Amount of code contribution in total]
   * @param {SQLContext}
   * @param {DataFrame} Filtered distribution dataset
   * @return {Map} [SpatialLocation] => [Long]
   */
  def accumGlobalDists(sqlctx: SQLContext, dists: DataFrame): Map[SpatialLocation, Long] = {
    import sqlctx.implicits._
    val contrib = dists.select($"coords").collect()

    // Create a mapping where geolocation plays as keys
    val map = Map.empty[SpatialLocation, Long]

    // Iterate through each language
    contrib.foreach { (dataArray) =>

      val elements = dataArray.getAs[WrappedArray[WrappedArray[Any]]](0)

      // Iterate through each element of the distributions of language
      elements.foreach { (components) =>
        val lat = components.apply(0) match {
          case s: String => s.toDouble
        }
        val lng = components.apply(1) match {
          case s: String => s.toDouble
        }
        val dense = components.apply(2) match {
          case s: String => try {
            s.toInt
          } catch {
            case e: Exception => 0
          }
        }

        // Only accumulate the spatial spots
        // in which density is not zero
        val location = SpatialLocation(lat, lng)
        if (dense > 0) {
          if (map contains location)
            map(location) += dense
          else
            map(location) = dense
        }
      }
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
    val map = Map.empty[Int, Float]

    map
  }
}