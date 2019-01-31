package one.codebase

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import scala.util.Try


/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * K-Means is an iterative clustering algorithm and works as follows:
 * K-Means is given a set of data points to be clustered and an initial set of ''K'' cluster
 * centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (''mean'') of all points that have
 * been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.
 * This is the Wikipedia entry for the [[http://en.wikipedia
 * .org/wiki/K-means_clustering K-Means Clustering algorithm]].
 *
 * This implementation works on two-dimensional data points.
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * Input files are plain text files and must be formatted as follows:
 *
 * - Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.
 * For example `"1.2 2.3\n5.3 7.2\n"` gives two data points (x=1.2, y=2.3) and (x=5.3,
 * y=7.2).
 * - Cluster centers are represented by an integer id and a point value.
 * For example `"1 6.2 3.2\n2 2.9 5.7\n"` gives two centers (id=1, x=6.2,
 * y=3.2) and (id=2, x=2.9, y=5.7).
 *

 * This example shows how to use:
 *
 * - Bulk iterations
 * - Broadcast variables in bulk iterations
 * - Scala case classes
 */
object KMeans {

  def main(args: Array[String]) {

    // checking input parameters
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val input = params.get("input", "berlin.csv")
    val iterations = params.getInt("iterations", 10)
    val mnc = params.get("mnc", "").split(",").flatMap { x: String =>
      Try {
        x.toInt
      }.toOption
    }
    var k = params.getInt("k",0)
    val output = params.get("output", "clusters.csv")

    // set up execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val cellTowers = env.readCsvFile[CellTowerData](input, ignoreFirstLine = true)

    val mncFilteredTowers = cellTowers.filter(x => if (mnc.nonEmpty) mnc.contains(x.net) else true)
    val nonLTETowers = mncFilteredTowers.filter(t => t.radio != "LTE")
    val lteTowers = mncFilteredTowers.filter(t => t.radio == "LTE")
    k = if (k==0) lteTowers.count().toInt else k

    val points = nonLTETowers.map(x=> Point(x.lon, x.lat))
    val centroids = lteTowers.distinct(_.cell).first(k).map(x => Centroid(x.cell,x.lon,x.lat))


    val finalCentroids = centroids.iterate(iterations) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }.withForwardedFields("_1; _2")
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }.withForwardedFields("_1")
        .map { x => new Centroid(x._1, x._2.div(x._3)) }.withForwardedFields("_1->id")
      newCentroids
    }

    val clusteredPoints: DataSet[(Int, Point)] =
      points.map(new SelectNearestCenter).withBroadcastSet(finalCentroids, "centroids")

    clusteredPoints.print()
    clusteredPoints.writeAsCsv(output, "\n", ",",writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    env.execute("Scala KMeans Example")

  }


// radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal
// UMTS,262,1,14256,2204760,0,13.381907,52.446789,1034,80,1,1380542105,1491834254,0
// LTE,262,1,1495,26355458,0,13.404399,52.468679,4219,32,1,1380624391,1519322301,0
  case class CellTowerData(radio: String, mcc: Int, net: Int, area: Int, cell: Int, unit: Int,
                           lon: Double, lat: Double, range: Int, samples: Int, changeable: Int,
                           created: Int, updated: Int, averageSignal: Int )


  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
    * Common trait for operations supported by both points and centroids
    * Note: case class inheritance is not allowed in Scala
    */
  trait Coordinate extends Serializable {

    var x: Double
    var y: Double

    def add(other: Coordinate): this.type = {
      x += other.x
      y += other.y
      this
    }

    def div(other: Long): this.type = {
      x /= other
      y /= other
      this
    }

    def euclideanDistance(other: Coordinate): Double =
      Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y))

    def clear(): Unit = {
      x = 0
      y = 0
    }

    override def toString: String =
      s"$x,$y"

  }

  /**
    * A simple two-dimensional point.
    */
  case class Point(var x: Double = 0, var y: Double = 0) extends Coordinate

  /**
    * A simple two-dimensional centroid, basically a point with an ID.
    */
  case class Centroid(var id: Int = 0, var x: Double = 0, var y: Double = 0) extends Coordinate {

    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString: String =
      s"$id ${super.toString}"

  }

  /** Determines the closest cluster center for a data point. */
  @ForwardedFields(Array("*->_2"))
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = _

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid: Centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }

  }
}

