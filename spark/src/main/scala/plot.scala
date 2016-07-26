package gitlang

import breeze.plot._
import breeze.linalg._
import scala.collection.mutable.Map

object Plot {

  def plotBinVectors(binVectorMap: Map[String, Array[Long]]) {

    val fig = Figure()

    // Create a new plot
    val plt = fig.subplot(0)

    // Render all bin vectors onto the plot
    binVectorMap.foreach {
      case (lang, binVec) =>
        val binV = new DenseVector(binVec)
        val index = DenseVector.range(0, binVec.length, 1)
      ///plt += plot(binV, index)
    }

    fig.refresh()
  }

}