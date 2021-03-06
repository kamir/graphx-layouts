/**
 *  How to learn GraphX? Use it! Write useful tools. 
 *  
 *  Here we go ...
 *  This is a "training script". It calculates the graph force-directed layout for large graphs
 *  with simmulated annealing. The algorithm was defined by "Fruchterman Reingold (1991)".
 *  
 *  For more details see: http://en.wikipedia.org/wiki/Force-directed_graph_drawing  
 */
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io._
import sys.process._


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

		def lenght = math.sqrt(x * x + y * y)

	}

val fileNameDebugDump = "/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.debug.dump"

var ci = 0 // count the iterations

var k: Double = 1.0

val defaultNode = ("o", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

val epsilon = 0.0001D // minimal distance

val width = 1000
val height = 1000

val area = width * height // area of graph

def getWidth: Double = width
	
def getHeight: Double = height
	
def getK: Double = k

val iterations = 20

val percentage: Double = 0.125

var currentIteration = 1 // current iteration

var temperature = 0.1 * math.sqrt(area) // current temperature

val tab: String = "\t"

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
    temperature = (1.0 - (iteration.toDouble / iterations)) * 0.1 * math.sqrt(area);
}





	

/** 
 * Simple inspection of a graph ... 
 */
def inspect( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) = {
	val f: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + " (" + triplet.srcAttr._2 + "," + triplet.srcAttr._3 + "), linkTo( " + triplet.dstAttr._1 + " )=" + triplet.attr )
	f.collect.foreach(println(_))
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

	val header: RDD[String] = sc.parallelize( Array("Source" + tab + "Target" + tab + "Strength") ) 
	
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

	val headerEL: RDD[String] = sc.parallelize( Array( pref + "Source" + tab + "Target" + tab + "Strength") ) 
	val headerNL: RDD[String] = sc.parallelize( Array( pref + "Id" + tab + "X" + tab + "Y" + "dX" + tab + "dY" + tab + "k" + tab + "m") ) 

	// 
	// TODO: Print also k and module of the node ...
	val e: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + tab + triplet.dstAttr._1 + tab + triplet.attr )
	val n: RDD[String] = g.vertices.map(v => v._2._1 + tab + v._2._2 + tab + v._2._3 + tab + v._2._4._1 + tab + v._2._4._2 + tab + 0 + tab + 0  )
	
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
 *  a is the graphs VertexRDD with: (label, x, y, (md1, md2, md3, md4)) properties
 *  
 *  The final update step is done once per iteration for each vertex. The metadata contains the total delta of the position
 *  the vertex has the position. Both are combined here and the metadata is reset to zero for the next step.
 */
def updatePos(a: (String, Double, Double, (Double,Double,Double,Double))) : (String, Double, Double,(Double,Double,Double,Double)) = {

	val x = between( 0, a._2 + a._4._1, width)
	val y = between( 0, a._3 + a._4._2, height)

	(a._1, x, y, (0.0,0.0,0.0,0.0))

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

def repulsionForce(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2

	var deltaLength = math.max(epsilon, delta.lenght)
	val force = k * k / deltaLength
	val disp = delta * force / deltaLength
	
	(disp.x, disp.y)		
}


def attractionForce(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2
	val deltaLength = math.max(epsilon, delta.lenght) // avoid x/0
	val force = deltaLength * deltaLength / k

	val disp = delta * force / deltaLength
	
	(disp.x, disp.y)		
}

def attractionForceInverted(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2
	val deltaLength = math.max(epsilon, delta.lenght) // avoid x/0
	val force = deltaLength * deltaLength / k

	val disp = delta * ( -1.0 * force ) / deltaLength
	
	(disp.x, disp.y)		
}


/**
 * Calc Repulsion
 */
def calcRepulsion( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

	val repV: VertexRDD[(Double, Double)] = g.mapReduceTriplets[(Double, Double)](

  		triplet => {
  			Iterator( (triplet.dstId, ( triplet.srcAttr._2, triplet.srcAttr._3)) )
		},
  		(m1, m2) => repulsionForce( (m1,m2) ) // Reduce Function for all contributions from all neighbors
        ) // repulsionV is now the VertexRDD

	
	val disp: VertexRDD[(Double, Double)] = repV.aggregateUsingIndex(repV, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))

	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(disp)( (id, a, b) => preUpdatePos(a,b) )

        val graphN = Graph(setC, g.edges, defaultNode)
        graphN
}


/**
 * Calc Attraction
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
  		(m1, m2) => attractionForceInverted( (m1,m2) ) // Reduce Function for all contributions from all neighbors
        ) // inverted attraction to other component is now the VertexRDD


	val disp1: VertexRDD[(Double, Double)] = attr1.aggregateUsingIndex(attr1, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))
	val disp2: VertexRDD[(Double, Double)] = attr2.aggregateUsingIndex(attr2, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))

	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(disp1)( (id, a, b) => preUpdatePos(a,b) )

        val g2 = Graph(setC, g.edges, defaultNode)

	val setD: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g2.vertices.innerJoin(disp2)( (id, a, b) => preUpdatePos(a,b) )

        val graphN = Graph(setD, g.edges, defaultNode)

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

        var gs = shuffle( g )
	println( "> Shuffled the graph." )

	temperature = 0.1 * math.sqrt(area) // current temperature

	for(iteration <- 1 to iterations) {

		ci = iteration
	
		println( "> Temperature: (T=" + temperature + ")" )

		// Repulsion is usually for all pairs of vertexes if they are different vertexs ...
                // But for simplification we use only the neighbors.
	  	val gRep = calcRepulsion( gs )

		// Attraction is along the links only
	  	val gAttr = calcAttraction( gRep ) 

		// WE CAN DEBUG EACH STEP 
 		dumpWithLayout( gAttr, fileNameDebugDump, "#" )

		// Repulsion and Attraction are in super position as they are overlaing forces
		// def mapValues[VD2](map: (VertexId, VD) => VD2): VertexRDD[VD2]
	 	val vNewPositions = gAttr.vertices.mapValues( (id, v) => updatePos( v ) )
        
                gs = Graph(vNewPositions, g.edges, defaultNode)

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

        //                         name,   x,      y        attr.x attr.z
	val nodes: RDD[(VertexId, (String, Double, Double, (Double,Double,Double,Double)))] =
  		  sc.parallelize(Array((1L, ("a", 80.0,70.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (2L, ("b", 600.0,100.0, (0.0, 0.0, 0.0, 0.0))),
        		               (3L, ("c", 30.0,30.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (4L, ("c", 130.0,830.0, (0.0, 0.0, 0.0, 0.0)) ), 
                                       (5L, ("d", 400.0,400.0, (0.0, 0.0, 0.0, 0.0)))))

	val statLink: RDD[Edge[Double]] =
  		  sc.parallelize(Array(Edge(1L, 2L, 1.0), 
                                       Edge(2L, 3L, 1.0),
                  	 	       Edge(3L, 5L, 1.0), 
                                       Edge(5L, 4L, 1.0),
				       Edge(4L, 1L, 1.0), 
                                       Edge(4L, 2L, 1.0),
				       Edge(1L, 3L, 1.0), 
                                       Edge(4L, 3L, 1.0)))

//val functLink: RDD[Edge[Double]] =
//  sc.parallelize(Array(Edge(3L, 7L, 1.0), Edge(5L, 3L, 1.0),
//                       Edge(3L, 5L, 0.5), Edge(5L, 2L, 0.15),
//                       Edge(3L, 2L, 0.1), Edge(5L, 7L, 0.25),
//                       Edge(2L, 5L, 0.2), Edge(7L, 5L, 0.25),
//                       Edge(2L, 7L, 0.3), Edge(2L, 3L, 0.15)))
//

//var graphF: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double] = Graph(nodes, functLink, defaultNode)

	val defaultNode = ("O", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

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
val cGraphS = createDemoGraph()

// Just to be sure what was loaded ...
//dump( cGraphS, fileNameDump )
dumpWithLayout( cGraphS, fileNameDump + ".ini2", "#" )

val sizeOfGraph = cGraphS.vertices.count()
println( "> Size of the graph : " + sizeOfGraph + " nodes." )

k = 1000 * math.sqrt(area / sizeOfGraph) // force constant

println( "> Force constant    : " + k + " a.u." )
println( "> The graph data was prepared." )
println( "> Ready to do a layout." )

val gLS = layoutFDFR( cGraphS )

dumpWithLayout( gLS, fileNameDump , "" )

println( "> DONE!" )
println( "> Created EDGE list: " + fileNameDump )

val result = "/home/training/bin/gnuplot /home/training/graphx-layouts/src/plot_DEMO2.sh" !!
println(result)












        

