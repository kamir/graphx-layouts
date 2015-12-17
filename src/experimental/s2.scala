import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random


        /**
	 * Vector impl. for metadata
	 */
        @SerialVersionUID(101L)
	class Vector(var x: Double = 0.0, var y: Double = 0.0) extends Serializable {

		def +(operand: Vector): Vector = {
			return new Vector(x + operand.x, y + operand.y)
		}

		def -(operand: Vector): Vector = {
			return new Vector(x - operand.x, y - operand.y)
		}

		def *(operand: Vector): Vector = {
			return new Vector(x * operand.x, y * operand.y)
		}

		def *(operand: Double): Vector = {
			return new Vector(x * operand, y * operand)
		}

		def /(operand: Double): Vector = {
			return new Vector(x / operand, y / operand)
		}

		def isNaN: Boolean = x.isNaN || y.isNaN

		def set(x: Double, y: Double): Vector = {
			this.x = x
			this.y = y
			return this
		}

		def clear = {
			x = 0.0
			y = 0.0
		}

		def lenght = math.sqrt(x * x + y * y)

	}

        /**
	 * Vertex metadata
	 */
        @SerialVersionUID(100L)
	class VertexMetadata extends Serializable{
		var pos = new Vector
		var disp = new Vector
	}

val users2: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] =
  sc.parallelize(Array((3L, ("a", 0.0,0.0, (0.0, 0.0, 0.0, 0.0)) ), (7L, ("b", 1.0,1.0, (0.0, 0.0, 0.0, 0.0))),
                       (5L, ("c", 3.0,3.0, (0.0, 0.0, 0.0, 0.0)) ), (2L, ("d", 4.0,4.0, (0.0, 0.0, 0.0, 0.0)))))

val link: RDD[Edge[Double]] =
  sc.parallelize(Array(Edge(3L, 7L, 1.0), Edge(5L, 3L, 1.0),
                       Edge(2L, 5L, 0.5), Edge(5L, 7L, 0.75)))

val defaultNode = ("o", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

var graph: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(users2, link, defaultNode)

val sizeOfGraph = users2.count()

val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " (" + triplet.srcAttr._2 + "," + triplet.srcAttr._3 + "), linkTo( " + triplet.dstAttr._1 + " )=" + triplet.attr )

facts.collect.foreach(println(_))





val width = 1000
val height = 1000

val area = width * height // area of graph

val k = 0.8 * math.sqrt(area / sizeOfGraph) // force constant

var temperature = 0.1 * math.sqrt(area) // current temperature

var currentIteration = 1 // current iteration

val epsilon = 0.0001D // minimal distance
	
def getWidth: Double = width
	
def getHeight: Double = height
	
def getK: Double = k

val iterations = 10


def calcRep( v: (VertexId, (String, Double, Double, (Double,Double,Double,Double))), iteration: Int ) : (VertexId, (String, Double, Double,(Double,Double,Double,Double))) = {
      ( v._1, ( v._2._1, iteration * getWidth, iteration * getHeight, v._2._4 ) ) 
}



        

