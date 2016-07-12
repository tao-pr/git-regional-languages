package gitlang

object Core extends App {
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.io.StdIn.{ readLine, readInt }
  import org.apache.spark.SparkContext
  import java.io._

  // TAOTODO:

  // Initialise a local Spark context
  val sc = new SparkContext("local", "gitlang")

  // Reads in the "dist.json" distribution data file
  val distjson = new File("src/main/resources/dist.json")
  val dataDist = DistDataSource.readJSON(sc, distjson.getAbsolutePath())

  dataDist.show()
}