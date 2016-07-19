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
   * @param {Map[SpatialLocation, Long]} global distribution map, as bin reference
   * @return {Map} language => [List of [bins]]
   */
  def geoDistToBins(sqlctx: SQLContext, dists: DataFrame, globalDist: Map[SpatialLocation, Long]): Map[String, Array[Long]] = {

    var map = Map.empty[String, Array[Long]]
      .withDefaultValue(Array())

    val binLength = globalDist.keys.size

    println(Console.CYAN + "**************")
    println(s"Bin size := ${binLength}")
    println("**************" + Console.RESET)

    // Prepare the mapping from [SpatialLocation] => [bin index]
    val mapBinIndex = Map.empty[SpatialLocation, Int]
    globalDist.keys.foldLeft(mapBinIndex) { (mapBinIndex, key) =>
      mapBinIndex(key) = mapBinIndex.keys.size
      mapBinIndex
    }

    // Iterate through each language's row
    dists.collect().foreach { langRow =>

      val lang = langRow.getAs[String](0)
      val elements = langRow.getAs[WrappedArray[WrappedArray[Any]]](1)

      // Prepare the map key [lang]
      // with initial zero bin vector
      map(lang) = Array.fill(binLength)(0)

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

        // Accumulate the bin vector
        if (dense > 0) {
          val location = SpatialLocation(lat, lng)
          val index = mapBinIndex(location)
          map(lang)(index) += dense
        }
      }
    }

    map
  }
}