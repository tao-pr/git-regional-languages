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
   * @param {DataFrame}
   * @return {Map} [SpatialLocation] => [Float]
   */
  def accumAllDists(sqlctx: SQLContext, dists: DataFrame): Map[SpatialLocation,Float] = {
    import sqlctx.implicits._
    val contrib = dists.select($"coords")

    // Create a mapping where geolocation plays as keys
    val map = Map.empty[SpatialLocation,Float]

    // TAOTODO:
    map
  }
}