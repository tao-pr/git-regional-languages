package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }
import scala.collection.mutable.{ Map, WrappedArray }
import org.apache.spark.mllib.linalg.{ Vectors, Matrices }


// Data munging auxiliary functions

object Transform {
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

    // Neutralise the bin vectors
    // by keep the bin values which represent 
    // 66% of the total energy, otherwise set zero.
    val THRESH_E = 0.66
    val map_ = map.map {
      case (lang, binvec) =>

        // Compute the total energy of the vector
        val totalEsqr = binvec.foldLeft(0D) { (a, b) => a + scala.math.pow(b, 2) }
        val totalE = scala.math.sqrt(totalEsqr)
        val threshE = totalE * THRESH_E

        // Map the bin vector with its index
        val indexedBin = binvec.zipWithIndex

        // Sort the indexed bins by magnitude
        val sortedBin = indexedBin.sortWith {
          case (a, b) =>
            val (abin, aindex) = a
            val (bbin, bindex) = b
            abin > bbin
        }

        // Take the biggest bin values until
        // the aggregated energy reaches the specified threshold 
        val initBinVector = Array.fill[Long](binLength)(0)
        val resultBinVec = thresholdBinVector(
          initBinVector,
          sortedBin,
          0,
          threshE
        )

        (lang -> resultBinVec)
    }

    map_
  }

  /**
   * Sample the given bin vector (Array)
   * by taking only top bins with larger amount of magnitude.
   * The resultant bin vector will exhibit up to
   * the specified threshold of energy.
   */
  private def thresholdBinVector(binVec: Array[Long], sortedBin: Array[(Long, Int)], index: Int, threshold: Double): Array[Long] = {

    if (index >= sortedBin.length)
      return binVec

    val totalEsqr = binVec.foldLeft(0D) { (a, b) => a + scala.math.pow(b, 2) }
    val totalE = scala.math.sqrt(totalEsqr)

    if (totalE >= threshold)
      return binVec

    // Take the next element from the source bin vector
    val (b, i) = sortedBin.apply(index)
    binVec(i) = scala.math.round(100 * scala.math.sqrt(((b * b) / totalEsqr).toDouble))

    thresholdBinVector(binVec, sortedBin, index + 1, threshold)
  }
}