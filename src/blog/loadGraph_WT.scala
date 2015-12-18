/**
 *   Load a graph from the SNAP Wiki-Talk.txt file
 *   http://snap.stanford.edu/data/wiki-Talk.html
 *
 *   wget http://snap.stanford.edu/data/wiki-Talk.txt.gz
 *
 *   17 MB compressed 
 *   67 MB uncompressed 
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 

val width = 100000
val height = 100000

/**
 * 
 * If the graph has not the right schema, we convert it. This layouter needs a well defined set of properties
 * for each node and each link.
 */
def convert( g: Graph[ Any, String ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

	val transformedShuffledNodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] = g.vertices.map {  v =>  
              val random = new scala.util.Random
              ( v._1, ( v._1.toString , random.nextDouble * getWidth, random.nextDouble * getHeight, (0.0,0.0,0.0,0.0) ) ) 
	}

	val transformedEdges: RDD[Edge[Double]] = g.edges.map( e => Edge( e.srcId, e.dstId, e.attr.toDouble ) )
		
        val graphN = Graph(transformedShuffledNodes, transformedEdges, defaultNode)

        graphN
}


 

val fileName = "blog/wiki-Talk.txt"

val s: String = "1.0"
val percentage: Double = 1.0
//val salt: Long = System.currentTimeMillis()
val salt: Long = 1

val edges: RDD[Edge[String]] =
      sc.textFile( fileName ).filter( l => !(l.startsWith("#")) ).sample( false, percentage, salt ).map { line =>
        val fields = line.split("\t")
        Edge( fields(0).toLong, fields(1).toLong, s )
      }

val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

var zE = graph.numEdges
var zV = graph.numVertices

println("num edges = " + zE);
println("num vertices = " + zV);
