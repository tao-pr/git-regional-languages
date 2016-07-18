package gitlang

object Core extends App {
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.io.StdIn.{ readLine, readInt }
  import org.apache.spark.{ SparkContext, SparkConf }
  import java.io._

  // Initialise a local Spark context
  val conf = new SparkConf().setMaster("local").setAppName("vor")
  val sc = new SparkContext(conf)

  // Reads in the "dist.json" distribution data file
  val distjson = new File("src/main/resources/dist.json")
  val sqlctx = DistDataSource.readJSON(sc, distjson.getAbsolutePath())
  val dists = DistDataSource.getDistributionByLanguage(sqlctx, true)

  // Filter only those languages with distributions
  val dists_ = dists.filter("SIZE(coords)>0")

  // Accumulate the entire universe of the distributions
  val universe = Analysis.accumGlobalDists(sqlctx, dists_)

  // TAODEBUG: Illustrate the universe distribution
  for (xy <- universe) {
    println(xy)
  }

  // TAOTODO: Pass over the distibution data for numerical analysis
}