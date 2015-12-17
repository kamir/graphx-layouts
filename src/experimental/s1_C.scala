/**
 *  How to learn GraphX? Use it! Write a useful tool.
 *  
 *  Here we go! This is a "training script". It calculates the force-directed layout for large graphs.
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
 * Return value when is between range (min, max) or min/max
 */
def between(min: Double, value: Double, max: Double): Double = {
	if(value < min)
		return min
	if(value > max)
		return max
	return value
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


/*******************************************************************************************************************************
 *
 * We create a graph with two layers. One is the stationary link layer and one is the 
 * functional layer, e.g., one which was calculated as an "Edit-Activity Correlation" network.
 * 
 * Now we calculate the layout for both link sets based on the sames vertex set.
 * The difference of the locations gives us information if both processes are "alligned" to each other.
 * this means, if the links of the functional network differ fundamentally from the underlying structural network.
 * Or if high correlation is a consequence of existance of structural links. 
 *
 */
def createDemoGraph1() : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = { 

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
/*******************************************************************************************************************************/


/** 
 * Load the graph from a local file ... 
 * 
 * Requires a files with:
 *   - tab-separated edge-list
 *   - uses a constant link strength, as it only interprestes the first two columns of the file
 *   - lines starting with "#" are ignored, they contain comments
 */
def loadEdges( fn : String ) : Graph[ Any , String ] = {

	//val salt: Long = System.currentTimeMillis()
	val salt: Long = 1

	val s: String = "1.0"

	sb.append( ">>> Load file: " + fn + nl )
	sb.append( ">              salt=" + salt + nl )
	sb.append( ">    default weight=" + s + nl )

	val edges: RDD[Edge[String]] =
      		sc.textFile( fn ).filter( l => !(l.startsWith("#")) ).sample( false, percentage, salt ).map { line =>
        	val fields = line.split( tab )
        	Edge( fields(0).toLong, fields(1).toLong, s )
      	}

	val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

	var zE = graph.numEdges
	var zV = graph.numVertices

	sb.append( ">>> num edges = " + zE);
	sb.append( ">>> num vertices = " + zV);

	graph	
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
def updateForceRepulsion(a: (String, Double, Double, (Double,Double,Double,Double)), b: (Double, Double)) : (String, Double, Double, (Double,Double,Double,Double)) = {
	(a._1, a._2, a._3, (0.0, 0.0, b._1, b._2))
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
def updateForceAttraction(a: (String, Double, Double, (Double,Double,Double,Double)), b: (Double, Double)) : (String, Double, Double, (Double,Double,Double,Double)) = {
	( a._1, a._2, a._3, (b._1, b._2, 0.0, 0.0) )
}

/**************************************************************************************************************************************
 *     Calculation of forces
 **************************************************************************************************************************************/

/**
 *  
 *  In a later version we will enable gravitation centers.
 *
 */
def gravity( x1: Double, y1: Double) : (Double, Double) = {	
/*
	val v1 = new Vector( x1, y1 )			
	val v2 = new Vector( 0, 0 )			

	val delta = v2 - v1

	var deltaLength = math.max(epsilon, delta.length)

	val force = gamma / deltaLength
	val disp = delta * force / deltaLength
*/
//	( disp.x, disp.y )
   	( 0.0, 0.0 )  // we turn gravity off here ...	
}

def attractionForce( b: (String, Double, Double, (Double,Double,Double,Double)), m: ((Double,Double),(Double,Double)) ) : (String, Double, Double, (Double,Double,Double,Double)) = {	

	val deltaX = m._2._1 - m._1._1
	val deltaY = m._2._2 - m._1._2

	val l = math.sqrt( deltaX * deltaX + deltaY * deltaY ) 

	val force = l * l / k

	println( nl + " Attraction Force" )
	println( "l:      " + l )
	println( "F:      " + force )
	println( "k:      " + k )
	println( "dX:     " + deltaX )
	println( "dY:     " + deltaY )
	println( "T:      " + temperature )
	println( "width:  " + width )
	println( "height: " + height )

	// force along the komponents
	val dispX = deltaX * force / l
	val dispY = deltaY * force / l

 	(b._1, b._2, b._3, (b._4._1, b._4._2, b._4._3 + dispX, b._4._4 + dispY))
}

def repulsionForce( x1: Double, y1: Double, x2: Double, y2: Double) : (Double, Double) = {	

	val deltaX = x2 - x1
	val deltaY = y2 - y1

	val l = math.sqrt( deltaX * deltaX + deltaY * deltaY ) 

	var deltaLength = math.max(epsilon, l)

	val force = k * k / l
	val dispX = deltaX * force / l
	val dispY = deltaY * force / l

	println( nl + " Repulsion Force" )
	println( "l:      " + l )
	println( "F:      " + force )
	println( "k:      " + k )
	println( "dX:     " + deltaX )
	println( "dY:     " + deltaY )
	println( "T:      " + temperature )
	println( "width:  " + width )
	println( "height: " + height )

	( dispX, dispY )
	
}

/**
 *  
 *  The "vertex" a is the graphs VertexRDDs element without an id. It has th properties: (label, x, y, (md1, md2, md3, md4)) 
 *  
 *  The final update step is done once per iteration for each vertex. The metadata (m1,m2,m3,m4) contains than total force 
 *  components which accelerate the nodes. We apply the temperature dependent scaling function. 
 *  The metadata is reset to zero for the next step.
 */
def updatePos( a: (String, Double, Double, (Double,Double,Double,Double))) : (String, Double, Double,(Double,Double,Double,Double)) = {

        // val g = gravity(a._2,a._3)
        val g = ( 0.0, 0.0 )

	// partial displacement of a node according to the different forces
	val deltaX = g._1 + a._4._1 + a._4._2
	val deltaY = g._2 + a._4._3 + a._4._4

	val l = math.sqrt( deltaX * deltaX + deltaY + deltaY ) 

	val scaleFactor = math.min( temperature, l ) / l
	val x = a._2 + scaleFactor * deltaX
	val y = a._3 + scaleFactor * deltaY
	
  	val xFinal = between( -width/2 , x, width/2  )
   	val yFinal = between( -height/2, y, height/2 )

  	(a._1, xFinal, yFinal, (0.0,0.0,0.0,0.0))
//	(a._1, x, y, (0.0,0.0,0.0,0.0))
}


/**************************************************************************************************************************************
 *     Apply the force calculation to the nodes and edges, and update the state of the datamodel. 
 **************************************************************************************************************************************/

/**
 * Calc Attraction
 * 
 * Attraction is calculated for pairs of node.
 * The displacement value is added to the one and subtracted from the other vertex position.
 */
def calcAttraction( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {


	//  // Iterative graph-parallel computation ==========================================================
	//  def pregel[A](initialMsg: A, maxIterations: Int, activeDirection: EdgeDirection)(
	//      vprog: (VertexID, VD, A) => VD,
	//      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexID,A)],
	//      mergeMsg: (A, A) => A)
	//    : Graph[VD, ED]


	// Unlike Pregel and instead more like GraphLab messages are computed in parallel as a function of the 
	// edge triplet and the message computation has access to both  the source and destination vertex attributes.

	val attr = g.pregel( ((0.0,0.0),(0.0,0.0)), 1 ) (

	  (id, node, m ) => attractionForce( node, (m._1, m._2) ), // Vertex Program

	  triplet => {  
		Iterator( (triplet.dstId, ( (triplet.srcAttr._2, triplet.srcAttr._3), (triplet.dstAttr._2, triplet.dstAttr._3)) ) )
	  },

//	  (m, n) => ( superPos( m, n ) )// Merge Message superposition of forces
	  (m, n) => ( n )// Merge Message superposition of forces

	)
        //attr
	g	 
}

/**
 * Calc Repulsion
 *
 * Repulsion has to be calculated for all pairs of Vertices. Using the "cartesian" operator is heavy!!!
 */
def calcRepulsion( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

	val pairs = g.vertices.cartesian( g.vertices )

	val repFList: RDD[(VertexId,(Double, Double))] = pairs.map( a => ( a._2._1 , repulsionForce( a._1._2._2, a._1._2._3, a._2._2._2, a._2._2._3) ) )

	dumpD( repFList, "REPULSION-PAIRS-ALL" )  // this looks correct ...

	//
	// It seems to be unnecessary to do an explicit map() in this case !!! Really????
	//
	val repFListAggregated: RDD[(VertexId,(Double, Double))] = repFList.reduceByKey( (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ) )
//	val repFListAggregated: RDD[(VertexId,(Double, Double))] = repFList.map( a => (a._1, (a._2._1,a._2._2) ) ).reduceByKey( (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ) )

	dumpD( repFListAggregated, "REPULSION-PAIRS-AGG" )  // this looks correct ...

	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(repFListAggregated)( (id, a, b) => updateForceRepulsion(a,b) )

        val graphN = Graph(setC, g.edges, defaultNode)
        graphN
}


/**
 * The Fruchertman-Reingold Layout is calculated in several iterations.
 * A cooling schedule controls the placement of nodes. The longer the algorithm runs,
 * the smaller the displacement will be.
 *
 */
def layoutFDFR( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

        ci = 0
	sb.append( ">>> Start FR-Layout procedure: n=" + iterations + " (nr of iterations)." + nl )

	var gs = g
        if ( doShuffle ) gs = shuffle( g )

 	dumpWithLayout( gs, fileNameDebugDump + ".SHUFFLED", "#" )

	// setup initial temperature
	temperature = 0.1 * math.sqrt(area) // current temperature

//        for(iteration <- 1 to iterations) {
 	for(iteration <- 1 to 1) {

		gs.cache()

		ci = iteration // to remember the last iteration after the loop is done
	
		println( "> Temperature: (T=" + temperature + ")" )

		// Repulsion is calcuated for all pairs of vertices.
                // But for simplification one can use only the neighbors within a rang d.
	  	val gRep = calcRepulsion( gs )
	  	// val gRep = calcRepulsion( gs, d )  // NOT IMPLEMENTED !!!
//	 	dumpWithLayout( gRep, fileNameDebugDump, "#" )
	 	dumpWithLayout( gRep, fileNameDebugDump + ".A", "#" )

		// Attraction is calculated along the links only.
	  	val gAttr = calcAttraction( gRep ) 
//	 	dumpWithLayout( gAttr, fileNameDebugDump, "#" )
	 	dumpWithLayout( gAttr, fileNameDebugDump + ".B", "#" )

		// Repulsion and Attraction are forces which both lead to a displacement of the node in super position.
	 	val vNewPositions = gAttr.vertices.mapValues( (id, v) => updatePos( v ) )
	        gs = Graph(vNewPositions, gs.edges, defaultNode)
// 		dumpWithLayout( gs, fileNameDebugDump, "#" )
 		dumpWithLayout( gs, fileNameDebugDump + ".C", "#" )
        
	        // cool
		cool(iteration)
	}

	gs // this was the last stat of our layout
}



/*******************************************************************************************************************************
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

/*******************************************************************************************************************************
 *
 *  Load the Wiki-Talk Graph
 *
 */
//val fileName = "file:///home/training/graphx-layouts/data/wiki-Talk.txt"
//val graphS = loadEdges( fileName )
//val cGraphS = convert( graphS )
//val percentage: Double = 0.005
//sb.append( ">>> percentage:" + percentage )


/*******************************************************************************************************************************
 *
 *  Use one of the DEMO-Graphs
 *
 */
//val cGraphS = createDemoGraph1()   // House of Nicolaus
val cGraphS = createDemoGraph2()   // simple BOX
//val cGraphS = createDemoGraph3()   // simple Node Pair

/*******************************************************************************************************************************
 *
 *  We dump the data from time to time to disc, to debug visually
 *
 */
val fileNameDump = "/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.dump"




/*******************************************************************************************************************************
 *
 * Prepare and start the layout procedure ...
 *
 */

// Just to be sure about what was loaded ...
//dump( cGraphS, fileNameDump )
dumpWithLayout( cGraphS, fileNameDump + ".ini2", "" )

val sizeOfGraph = cGraphS.vertices.count()

k = kf * math.sqrt(area / sizeOfGraph) // force constant

val nowStart = Calendar.getInstance().getTime().getTime()

// val gLS = layoutFDFR( cGraphS )
val gLS = cGraphS

val nowStop = Calendar.getInstance().getTime().getTime()

dumpWithLayout( gLS, fileNameDump , "" )

var rt: String = "> Runtime: " + ( ( nowStop - nowStart ) / 1000 ) + " s." + nl

println( "> Size of the graph : " + sizeOfGraph + " nodes." )
println( "> Force constant    : " + k + " a.u." )

println( "> The graph data was prepared." )
println( "> Ready to do a layout." )
println( "> Created EDGE list: " + fileNameDump )

println( "> Last Temperature: " + lt + " a.u." )
println( "> Current Temperature: " + temperature + " a.u." )

println( "> Used the script: /home/training/graphx-layouts/src/plot_DEMO_SINGLE.sh" )
println( "> Now please look into the debug view: ./../graphx-layouts/showRelaxationSingle.html" )
println( "> DONE!" )

// val result = "/home/training/bin/gnuplot /home/training/graphx-layouts/src/plot_DEMO2.sh" !!
//val result = "/home/training/bin/gnuplot /home/training/graphx-layouts/src/plot_DEMO_SINGLE.sh" !!
//println( result )

sb.append( ">=====================================================" + nl )
sb.append( rt )
sb.append( ">   Done." + nl)
sb.append( ">=====================================================" + nl )

println( nl + nl + sb.toString() )

ci = 1
cool( 1 )
println( repulsionForce( -100, 100, 100, 100) )
println( attractionForce( ("a", -100, 100, (0,0,0,0)), ((-100,100),(100,100)) ) )












        

