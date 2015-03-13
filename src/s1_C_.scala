/**
 *  How to learn GraphX? Use it! Write useful tools!
 *  
 *  Here we go ...
 *  This is a "training script". It calculates the force-directed layout for large graphs.
 *  The algorithm was defined by "Fruchterman Reingold (1991)".
 *  
 *  For more details see: http://en.wikipedia.org/wiki/Force-directed_graph_drawing  
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io._
import sys.process._
import java.util.Calendar

// control the runtime of the layouter
val doShuffle: Boolean = false
val gamma: Double = 0.0  // gravity constant

val iterations = 10

// for partial processing we do sampe the graph
val percentage: Double = 0.125

/**
 *
 * Some inspiration for this project came from: 
 * 
 *    https://github.com/foowie/Graph-Drawing-by-Force-directed-Placement
 * 
 * here I reuse the Vector implementation and the core structure of the
 * algorithm. The goal was not to reinvent the core implementation, but 
 * one for GraphX.
 */

        /**
	 * Vector impl. for metadata
	 */
	 class Vector(var x: Double = 0.0, var y: Double = 0.0) {

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

		def length = math.sqrt(x * x + y * y)

		def asString: String = "( " + x + ", " + y + " )"


	}




val fileNameDebugDump = "/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.debug.dump"

var ci = 0 // count the iterations

val kf: Double = 0.8
var k: Double = 1.0 // will be changed according to area and size of graph

val defaultNode = ("NONODE", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

val epsilon: Double = 0.0001 // minimal distance

val width = 1000
val height = 1000

val area = width * height // area of graph

def getWidth: Double = width
	
def getHeight: Double = height
	
def getK: Double = k

var currentIteration = 1 // current iteration

val cf: Double = 0.1
var temperature = cf * math.sqrt(area) // current temperature

val tab: String = "\t"
val nl: String = "\n"

val sb: StringBuffer = new StringBuffer()

var lt: Double = 0  // keep the last Temperature for debuging

/**
 * This is a handy helper to write into local files.  
 *
 * Found it here:
 *   http://stackoverflow.com/questions/4604237/how-to-write-to-a-file-in-scala
 */
def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

/** 
 * Load the graph from a local file ... 
 * 
 * Requires a files with:
 *   - tab-separated edge-list
 *   - uses a constant link strength, as it only interprestes the first two columns of the file
 *   - lines starting with "#" are ignored, they contain comments
 */
def loadEdges( fn : String ) : Graph[ Any , String ] = {

	val s: String = "1.0"
	
	//val salt: Long = System.currentTimeMillis()
	val salt: Long = 1

	val edges: RDD[Edge[String]] =
      		sc.textFile( fn ).filter( l => !(l.startsWith("#")) ).sample( false, percentage, salt ).map { line =>
        	val fields = line.split( tab )
        	Edge( fields(0).toLong, fields(1).toLong, s )
      	}

	val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

	var zE = graph.numEdges
	var zV = graph.numVertices

	println("num edges = " + zE);
	println("num vertices = " + zV);

	graph	
}



/**
 *  
 *  We implement a force/directed layout. It uses an approach called "simmulated annealing".
 *  This is the functin to "cool the graph" during each iteration.
 */
def cool(iteration: Int) = {
    lt = temperature	
    temperature = (1 - (iteration.toDouble / iterations)) * cf * math.sqrt(area);
    sb.append( ">>> Cooled : " + lt + " => " + temperature + nl )
    temperature
}



def gravity( x1: Double, y1: Double) : (Double, Double) = {	

	val v1 = new Vector( x1, y1 )			
	val v2 = new Vector( 0, 0 )			

	val delta = v2 - v1

	var deltaLength = math.max(epsilon, delta.length)

	val force = gamma / deltaLength
	val disp = delta * force / deltaLength

	( disp.x, disp.y )
//  	( 0.0, 0.0 )  // we turn gravity off here ...	
}

	

/** 
 * Simple inspection of a graph ... 
 */
def inspect( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) = {
	val f: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + " (" + triplet.srcAttr._2 + "," + triplet.srcAttr._3 + "), linkTo( " + triplet.dstAttr._1 + " )=" + triplet.attr )
	f.collect.foreach(println(_))
}

/** 
 * Dump the displacements 
 */
def dumpD( rdd: RDD[(VertexId,(Double, Double))] , label: String ) = {

        println( "!!! WARNING !!! => dumpD() WORKS FOR SMALL GRAPHS ONLY. It uses the RDD.collect() function. " )

	val f: RDD[String] = rdd.map( a => a._1 + tab + a._2._1 + tab + a._2._2 )

	val header: RDD[String] = sc.parallelize( Array("ID" + tab + "dX" + tab + "dY") ) 
	
	val all: RDD[String] = header.union( f )

	printToFile(new File( "/mnt/hgfs/SHARE.VM.MAC/DATA/DEBUG." + label + "." + ci + ".displacement.csv" )) {
		p => all.collect.foreach(p.println(_))
	}

}

/** 
 * Dump the graph as simple edgelist into the local file system. 
 * 
 * This works only for small graphs. In case of larger graphs we can not use the
 *
 *   all. c o l l e c t .foreach(p.println(_))
 *
 * function. Here we will use graph output formats or scalable GraphWriters.
 *
 */
def dump( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ], fn: String ) = {

        println( "!!! WARNING !!! => dump() WORKS FOR SMALL GRAPHS ONLY. It uses the RDD.collect() function. " )

	val f: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + tab + triplet.dstAttr._1 + tab + triplet.attr )

	val header: RDD[String] = sc.parallelize( Array("Source" + tab + "Target" + tab + "Weight") ) 
	
	val all: RDD[String] = header.union( f )

	printToFile(new File( fn + ".triples.csv" )) {
		p => all.collect.foreach(p.println(_))
	}

}

/** 
 * Dump the graph with layout information into the local file system. 
 * 
 * This works only for small graphs. In case of larger graphs we can not use the
 *
 *   allN. c o l l e c t .foreach(p.println(_))
 *
 * function. Here we will use graph output formats or scalable GraphWriters.
 *
 * The pref shuld be "#" if guplot or other tools should read the file.
 * To export the data to Gephi, we MUST remove the "#" from the first line.
 *
 */
def dumpWithLayout( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ], fn: String, pref: String ) = {
 
        println( "!!! WARNING !!! => dumpWithLayout() WORKS FOR SMALL GRAPHS ONLY. It uses the RDD.collect() function. " )

	val fnEdgelist = fn + "_" + ci + "_EL.csv"
	val fnNodelist = fn + "_" + ci + "_NL.csv"

	val headerEL: RDD[String] = sc.parallelize( Array( pref + "Source" + tab + "Target" + tab + "Weight") ) 
	val headerNL: RDD[String] = sc.parallelize( Array( pref + "Id" + tab + "X" + tab + "Y" + tab +  "dX" + tab + "dY" + tab + "k" + tab + "m" + tab + "c") ) 

	// 
	// TODO: Print also k, module, and local clustering coefficient of the node ...
	val e: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + tab + triplet.dstAttr._1 + tab + triplet.attr )
	val n: RDD[String] = g.vertices.map(v => v._2._1 + tab + v._2._2 + tab + v._2._3 + tab + v._2._4._1 + tab + v._2._4._2 + tab + 0 + tab + 0 + tab + 0  )
	
	val allE: RDD[String] = headerEL.union( e )
	val allN: RDD[String] = headerNL.union( n )

	printToFile(new File( fnNodelist )) {
		p => allN.collect.foreach(p.println(_))
	}

	printToFile(new File( fnEdgelist )) {
		p => allE.collect.foreach(p.println(_))
	}
}

/**
 * 
 * If the graph has not the right schema, we convert it. This layouter needs a well defined set of properties
 * for each node and each link.
 */
def convert( g: Graph[ Any, String ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

        sb.append( ">>> Converted the Input" + nl )

	val transformedShuffledNodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] = g.vertices.map {  v =>  
              val random = new scala.util.Random
              ( v._1, ( v._1.toString , random.nextDouble * getWidth, random.nextDouble * getHeight, (0.0,0.0,0.0,0.0) ) ) 
	}

	val transformedEdges: RDD[Edge[Double]] = g.edges.map( e => Edge( e.srcId, e.dstId, e.attr.toDouble ) )
		
        val graphN = Graph(transformedShuffledNodes, transformedEdges, defaultNode)

        graphN
}




/**
 *
 * Here we simply shuffle the vertex position. New koordinates will be random numbers 
 * in the range 0.0 to with for x and 0 to hight of the plotting area for y.
 */
def shuffle( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

        sb.append( ">>> Shuffled the  Graph" + nl )

	val shuffledNodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] = g.vertices.map {  v =>  
              val random = new scala.util.Random
              ( v._1, ( v._2._1, random.nextDouble * getWidth, random.nextDouble * getHeight, v._2._4 ) ) 
	}
		
        val graphN = Graph(shuffledNodes, g.edges, defaultNode)
        graphN
}

/**
 * Return value when is between range (min, max) or min/max
 */
def between(min: Double, value: Double, max: Double): Double = {
	if(value < min)
		return min
	if(value > max)
		return max
	return value
}

/**
 *  
 *  a is the graphs VertexRDDs element without id. It has properties: (label, x, y, (md1, md2, md3, md4)) 
 *  
 *  The final update step is done once per iteration for each vertex. The metadata contains than the total delta regarding the position
 *  the vertex has currently. Both are combined, we apply the temperature dependent scaling function. 
 *  The metadata is reset to zero for the next step.
 */
def updatePos( a: (String, Double, Double, (Double,Double,Double,Double))) : (String, Double, Double,(Double,Double,Double,Double)) = {

        val g = gravity(a._2,a._3)

	val disp: Vector = new Vector( a._4._1, a._4._2 )
	val delta = disp.length 

	val scaleFactor = math.min( temperature, delta ) / delta

	val x = a._2 + scaleFactor * a._4._1 + g._1
	val y = a._3 + scaleFactor * a._4._2 + g._2
	
  	val xf = between( -width/2 , x, width/2  )
   	val yf = between( -height/2, y, height/2 )

	(a._1, xf, yf, (0.0,0.0,0.0,0.0))
}



/**
 *  
 *  a is the graphs VertexRDD with: (label, x, y, (md1, md2, md3, md4)) properties
 *  b is the displacement which is collected as md1 and md2
 *  
 *  md3 and md4 are not used here. Later thet contain information about additional layout influencers, e.g. module-id or attractor location.
 *  
 *  A preUpdate step is done for each influencing factor. Metadata and current position are 
 *  merged in a final update step.
 */
def preUpdatePos(a: (String, Double, Double, (Double,Double,Double,Double)), b: (Double, Double)) : (String, Double, Double, (Double,Double,Double,Double)) = {
	(a._1, a._2, a._3, (b._1,b._2,0.0,0.0))
}


def attractionForce(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )	
		
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v2 - v1
	val l = delta.length

	val deltaLength = math.max(epsilon, l) // avoid x/0
	
	val force = deltaLength * deltaLength / k

	sb.append( "Force: " + force + nl )

	// force along the komponents
	val disp = delta * force / deltaLength

	
	(disp.x, disp.y)		
}


def rep( x1: Double, y1: Double, x2: Double, y2: Double) : (Double, Double) = {	

	val v1 = new Vector( x1, y1 )			
	val v2 = new Vector( x2, y2 )			

	val delta = v2 - v1

	var deltaLength = math.max(epsilon, delta.length)
	val force = k * k / deltaLength
	val disp = delta * force / deltaLength

	( disp.x, disp.y )
	
}







/**
 * Calc Repulsion
 *
 * Repulsion has to be calculated for all pairs of Vertices.
 */
def calcRepulsion( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

//	val pairs: VertexRDD[ ( (String, Double, Double, (Double,Double,Double,Double)) , (String, Double, Double, (Double,Double,Double,Double)) )] = vList.cartesian(vList)

	val pairs = g.vertices.cartesian( g.vertices )

	val dispList: RDD[(VertexId,(Double, Double))] = pairs.map( a => ( a._2._1 , rep( a._1._2._2, a._1._2._3, a._2._2._2, a._2._2._3) ) )

	dumpD( dispList, "REPULSION-PAIRS" )

	val dispListA: RDD[(VertexId,(Double, Double))] = dispList.reduceByKey( (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ) )

	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(dispListA)( (id, a, b) => preUpdatePos(a,b) )

        val graphN = Graph(setC, g.edges, defaultNode)
	dumpWithLayout( graphN, fileNameDebugDump + ".REPULSION.graphN", "#" ) // still has correct IDs
	
        graphN
}


/**
 * Calc Attraction
 * 
 * Attraction is calculated for pairs of nodes.
 *
 * The displacement value is added to the one and subtracted from the other vertex position.
 */
def calcAttraction( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

	val attr1: VertexRDD[(Double, Double)] = g.mapReduceTriplets[(Double, Double)](

  		triplet => {
  			Iterator( (triplet.dstId, ( triplet.srcAttr._2, triplet.srcAttr._3)) )
		},
  		(m1, m2) => attractionForce( (m1,m2) ) // Reduce Function for all contributions from all neighbors
        ) // attraction to one component is now the VertexRDD

	val attr2: VertexRDD[(Double, Double)] = g.mapReduceTriplets[(Double, Double)](

  		triplet => {
  			Iterator( (triplet.srcId, ( triplet.dstAttr._2, triplet.dstAttr._3)) )
		},
  		(m1, m2) => attractionForce( (m1,m2) ) // Reduce Function for all contributions from all neighbors
        ) // inverted attraction to other component is now the VertexRDD

	dumpD( attr1, "ATTR1" )
	dumpD( attr2, "ATTR2" )

	val x: VertexRDD[(Double, Double)] = attr2.union( attr1 )

	val disp1: VertexRDD[(Double, Double)] = x.aggregateUsingIndex(x, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))
	val disp2: VertexRDD[(Double, Double)] = attr2.aggregateUsingIndex(attr2, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))

	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(disp1)( (id, a, b) => preUpdatePos(a,b) )

        val g2 = Graph(setC, g.edges, defaultNode)
	dumpWithLayout( g2, fileNameDebugDump + ".ATTR.g2", "#" )

	val setD: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g2.vertices.innerJoin(disp2)( (id, a, b) => preUpdatePos(a,b) )

        val graphN = Graph(setD, g2.edges, defaultNode)
	dumpWithLayout( graphN, fileNameDebugDump + ".ATTR.graphN", "#" )
        graphN	 

}





/**
 * The Fruchetman Reingold Layout is calculated with n = 10 iterations.
 *
 *
 */
def layoutFDFR( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

        ci = 0

	g.cache()

	println( "> Start the Layout procedure: n=" + iterations + " (nr of iterations)." )

	var gs = g
        if ( doShuffle ) gs = shuffle( g )

 	dumpWithLayout( gs, fileNameDebugDump + ".A", "#" )

	// setup initial temperature
	temperature = 0.1 * math.sqrt(area) // current temperature

        //for(iteration <- 1 to iterations) {
 	for(iteration <- 1 to 1) {

		ci = iteration
	
		println( "> Temperature: (T=" + temperature + ")" )

		// Repulsion is usually for all pairs of vertexes if they are different vertexs ...
                // But for simplification we use only the neighbors.
	  	val gRep = calcRepulsion( gs )
	 	dumpWithLayout( gRep, fileNameDebugDump + ".A", "#" )

		// Attraction is along the links only
	  	val gAttr = calcAttraction( gRep ) 
	 	dumpWithLayout( gAttr, fileNameDebugDump + ".B", "#" )

		// Repulsion and Attraction are in super position as they are overlaing forces
		// def mapValues[VD2](map: (VertexId, VD) => VD2): VertexRDD[VD2]

		//
		// Here we have a proble with the new Graph	
		//
	 	val vNewPositions = gAttr.vertices.mapValues( (id, v) => updatePos( v ) )
	        gs = Graph(vNewPositions, gs.edges, defaultNode)
 		dumpWithLayout( gs, fileNameDebugDump + ".C", "#" )
        
	        // cool
		cool(iteration)
	}

	gs // this was the last stat of our layout
}

/**
 *
 * We create a graph from two layers. One is the stationary link layer and one is the 
 * functional layer, calculated as an "Edit-Activity Correlation" network.
 * 
 * Now we calculate the layout for both link sets based on the sames vertex set.
 * The difference of the locations gives us information if the process is "alligned" with the links
 * or if the functional network differes fundamentally from the underlying structural network.
 *
 */
def createDemoGraph() : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = { 

        sb.append( ">>> Created Graph #House of Nicolaus" + nl )


        //                         name,   x,      y        attr.x attr.z
	val nodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] =
  		  sc.parallelize(Array((1L, ("a", 80.0,70.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (2L, ("b", 600.0,100.0, (0.0, 0.0, 0.0, 0.0))),
        		               (3L, ("c", 30.0,30.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (4L, ("d", 130.0,830.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (5L, ("e", 400.0,400.0, (0.0, 0.0, 0.0, 0.0)))))

	val statLink: RDD[Edge[Double]] =
  		  sc.parallelize(Array(Edge(1L, 2L, 1.0), 
                                       Edge(2L, 3L, 1.0),
                  	 	       Edge(3L, 5L, 1.0), 
                                       Edge(3L, 4L, 1.0),
				       Edge(5L, 4L, 1.0), 
                                       Edge(4L, 1L, 1.0),
				       Edge(4L, 2L, 1.0), 
                                       Edge(1L, 3L, 1.0)))

//val functLink: RDD[Edge[Double]] =
//  sc.parallelize(Array(Edge(3L, 7L, 1.0), Edge(5L, 3L, 1.0),
//                       Edge(3L, 5L, 0.5), Edge(5L, 2L, 0.15),
//                       Edge(3L, 2L, 0.1), Edge(5L, 7L, 0.25),
//                       Edge(2L, 5L, 0.2), Edge(7L, 5L, 0.25),
//                       Edge(2L, 7L, 0.3), Edge(2L, 3L, 0.15)))
//

//var graphF: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, functLink, defaultNode)

	val defaultNode = ("NO", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

	var graphS: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, statLink, defaultNode)

        graphS
}

/**
 *
 * We create a graph from two layers. One is the stationary link layer and one is the 
 * functional layer, calculated as an "Edit-Activity Correlation" network.
 * 
 * Now we calculate the layout for both link sets based on the sames vertex set.
 * The difference of the locations gives us information if the process is "alligned" with the links
 * or if the functional network differes fundamentally from the underlying structural network.
 *
 */
def createDemoGraph2() : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = { 

        sb.append( ">>> Created Graph #2" + nl )

        //                         name,   x,      y        attr.x attr.z
	val nodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] =
  		  sc.parallelize(Array( (3L, ("a", -100.0,-100.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                        (1L, ("b", 100.0,100.0,   (0.0, 0.0, 0.0, 0.0)) ), 
					(2L, ("c", -100.0,100.0,    (0.0, 0.0, 0.0, 0.0)) ), 
                                        (4L, ("d", 100.0,-100.0,     (0.0, 0.0, 0.0, 0.0)) ) 
                                      )
                                ) 

	val statLink: RDD[Edge[Double]] =
  		  sc.parallelize(Array(Edge(1L, 2L, 1.0),Edge(2L, 3L, 1.0),Edge(3L, 4L, 1.0),Edge(4L, 1L, 1.0) ))

//val functLink: RDD[Edge[Double]] =
//  sc.parallelize(Array(Edge(3L, 7L, 1.0), Edge(5L, 3L, 1.0),
//                       Edge(3L, 5L, 0.5), Edge(5L, 2L, 0.15),
//                       Edge(3L, 2L, 0.1), Edge(5L, 7L, 0.25),
//                       Edge(2L, 5L, 0.2), Edge(7L, 5L, 0.25),
//                       Edge(2L, 7L, 0.3), Edge(2L, 3L, 0.15)))
//

//var graphF: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, functLink, defaultNode)

	val defaultNode = ("NO", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

	var graphS: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, statLink, defaultNode)

        graphS
}

/**
 *
 * We create a graph from two layers. One is the stationary link layer and one is the 
 * functional layer, calculated as an "Edit-Activity Correlation" network.
 * 
 * Now we calculate the layout for both link sets based on the sames vertex set.
 * The difference of the locations gives us information if the process is "alligned" with the links
 * or if the functional network differes fundamentally from the underlying structural network.
 *
 */
def createDemoGraph3() : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = { 

        sb.append( ">>> Created Graph #3" + nl )

        //                         name,   x,      y        attr.x attr.z
	val nodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] =
  		  sc.parallelize(Array(  
                                        (1L, ("a", 100.0,100.0,   (0.0, 0.0, 0.0, 0.0)) ), 
					(2L, ("b", -100.0,100.0,    (0.0, 0.0, 0.0, 0.0)) )
                                      )
                                ) 

	val statLink: RDD[Edge[Double]] =
  		  sc.parallelize( Array(Edge(1L, 2L, 1.0) ) )

//val functLink: RDD[Edge[Double]] =
//  sc.parallelize(Array(Edge(3L, 7L, 1.0), Edge(5L, 3L, 1.0),
//                       Edge(3L, 5L, 0.5), Edge(5L, 2L, 0.15),
//                       Edge(3L, 2L, 0.1), Edge(5L, 7L, 0.25),
//                       Edge(2L, 5L, 0.2), Edge(7L, 5L, 0.25),
//                       Edge(2L, 7L, 0.3), Edge(2L, 3L, 0.15)))
//

//var graphF: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, functLink, defaultNode)

	val defaultNode = ("NO", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

	var graphS: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, statLink, defaultNode)

        graphS
}

/**
 *
 * This Snippet is an implementation of the Fruchterman-Reingold Force Directed Graph Layout Algorithm.
 *
 * Layout calculations for large graphs is still a problem. Spark allows us local processing and simple scaling
 * if a cluster is available. 
 *
 * A force directed layout represents to some extend a physical reality of the system. Maybe the restriction to a
 * two dimensional plane is not optimal. But as a first step we use the static link structure of LNNs to calculate
 * the layout. Functional links ar then plotted into this layout.
 * 
 * We investigate now the dependency between distance in the 'static layout' with the link strength of the 'functional network'.
 * Furthermore one can calculate the distance between the nodes in both layouts. It the ration of both distances is 1, they are 
 * not. different. If the distances are very different, one can find node pairs, for which a longer distance exist and such with
 * a shorter distance. Machine learning algorithms can such link properties use for classification models. It would be interesting
 * to find out if this measure has an influence on page sepparation or re linking. 
 * 
 * In this work we measure the force on nodes comming from functional links. The functional-displacement of a node is calculated as the 
 * difference between its location calculated in the static link network and the location found in the functional network.
 * 
 * For a simple interpretation we use the absolute displacement. But one can also think about a vector representation or a complex number
 * which also contains an angle. 
 *
 */

/**
 *
 *  Load the Wiki-Talk Graph
 *
 */
//val fileName = "file:///home/training/graphx-layouts/data/wiki-Talk.txt"
//val fileNameDump = "/mnt/hgfs/SHARE.VM.MAC/DATA/wiki-Talk_" + percentage + ".dump"
//val graphS = loadEdges( fileName )
//val cGraphS = convert( graphS )

/**
 *
 *  Create the DEMO-Graph
 *
 */
val fileNameDump = "/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.dump"
//val cGraphS = createDemoGraph()       // House of Nicolaus
//val cGraphS = createDemoGraph2()   // simple BOX
val cGraphS = createDemoGraph3()   // simple Node Pair

// Just to be sure what was loaded ...
//dump( cGraphS, fileNameDump )
dumpWithLayout( cGraphS, fileNameDump + ".ini2", "" )

val sizeOfGraph = cGraphS.vertices.count()

k = kf * math.sqrt(area / sizeOfGraph) // force constant

val nowStart = Calendar.getInstance().getTime().getTime()

val gLS = layoutFDFR( cGraphS )

val nowStop = Calendar.getInstance().getTime().getTime()

dumpWithLayout( gLS, fileNameDump , "" )

println( "> Runtime: " + ( ( nowStop - nowStart ) / 1000 ) + " s." )

println( "> Size of the graph : " + sizeOfGraph + " nodes." )
println( "> Force constant    : " + k + " a.u." )

println( "> The graph data was prepared." )
println( "> Ready to do a layout." )
println( "> Created EDGE list: " + fileNameDump )

println( "> Last Temperature: " + lt + " a.u." )
println( "> Current Temperature: " + temperature + " a.u." )

// val result = "/home/training/bin/gnuplot /home/training/graphx-layouts/src/plot_DEMO2.sh" !!
// val result = "/home/training/bin/gnuplot /home/training/graphx-layouts/src/plot_DEMO_SINGLE.sh" !!
// println( result )

println( "> Used the script: /home/training/graphx-layouts/src/plot_DEMO_SINGLE.sh" )
println( "> Now please look into the debug view: ./../graphx-layouts/showRelaxationSingle.html" )
println( "> DONE!" )

sb.append( ">>> Ready." )

println( "::SB:: " + nl + sb.toString() )











        

