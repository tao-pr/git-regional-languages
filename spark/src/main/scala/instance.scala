package gitlang

object Core extends App {
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.io.StdIn.{ readLine, readInt }
  import org.apache.spark.SparkContext
  import java.io._

  // Initialise a local Spark context
  val sc = new SparkContext("local", "gitlang")

  // Reads in the "dist.json" distribution data file
  val distjson = new File("src/main/resources/dist.json")
  val sqlctx = DistDataSource.readJSON(sc, distjson.getAbsolutePath())
  val dists = DistDataSource.getDistributionByLanguage(sqlctx, true)

  // Filter only those languages with distributions
  val dists_ = dists.filter("SIZE(coords)>0")

  // Accumulate the entire universe of the distributions
  val universe = Analysis.accumAllDists(sqlctx, dists_)

  // TAOTODO: Pass over the distibution data for numerical analysis
}