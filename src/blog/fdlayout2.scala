/**
 *  How-To: Layouts for large Graphs using GraphX
 *  
 *  Here we implement the Force-Directed Layout. Plausible positions have do be found
 *  by taking aesthetic criteria into account. The algorithm was defined by 
 *  "Fruchterman Reingold (1991)".
 *  
 *  For more details see: http://en.wikipedia.org/wiki/Force-directed_graph_drawing 
 *
 *  Draft for the BLOG-POST is in Google-Docs.
 *
 *  https://docs.google.com/document/d/1O3XcuKVmxoM_WDNKD0wUck2K3-DfmyBryeJtMlHYVss/edit# 
 */
 
 
 
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io._
import sys.process._
import java.util.Hashtable

/**
 * Some inspiration for this project came from: 
 * 
 *    https://github.com/foowie/Graph-Drawing-by-Force-directed-Placement
 * 
 * here I reuse the Vector implementation. The goal was not to reinvent the 
 * core implementation, but to provide one for GraphX.
 */

    /**
	 * Vector implementation for node metadata (position, displacement)
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
		
		def asString: String = {
			return "x=" + x + " y=" + y + "   l=" + this.lenght
		}

	}

//
// we define a working directory
//
val WD = "."

var REP_SCALE = 0.0001
var ATT_SCALE = 0.0001

val fileNameDebugDump = WD + "/fdl/demo-graph.debug.dump"

//
// count the iterations
//
var ci = 0 

//
// the spring constant
//
var k: Double = 1.0

//
// nodes with no connection are always linked to the origin 
//
val defaultNode = ("o", 0.0, 0.0, (0.0, 0.0, 0.0, 0.0) )

//
// minimal distance
//
val epsilon = 0.0001D

//
// the area to plot in
//
val width = 1000
val height = 1000

val area = width * height // area of graph

def getWidth: Double = width
	
def getHeight: Double = height
	
def getK: Double = k

val iterations = 10

//
// during graph preparation we reduce the size a bit ...
//
val percentage: Double = 1.0
// val percentage: Double = 0.125


var currentIteration = 1 // current iteration

//
// Define the initial temperature
// 
var temperature = 0.1 * math.sqrt(area)  

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
 *   - uses a constant link strength, as it only interprets the first two columns 
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
        
  println( "### Dumplocation : " + fn + ".triples.csv"  )

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
 * If the graph has not the right schema, we convert it. 
 * This layouter needs a well defined set of properties
 * for each node and each link.
 *
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
 *  The final update step is done once per iteration for each vertex. The metadata md1,
 *  md2, md3, and md4 is the total delta of the position of the vertex per iteration. 
 *  Both contributions are combined here and the metadata is reset to zero for the next 
 *  layout step.
 * 
 *  This operation is called on all nodes once per iteration. 
 */
def updatePos(a: (String, Double, Double, (Double,Double,Double,Double))) : (String, Double, Double,(Double,Double,Double,Double)) = {

    // (x,y) is the new position
	val x = between( 0, a._2 + a._4._1, width)
	val y = between( 0, a._3 + a._4._2, height)

    // md3 and md4 are currently not used here.
	(a._1, x, y, (0.0,0.0,0.0,0.0))

}



/**
 *  
 *  a is the graphs VertexRDD with: (label, x, y, (md1, md2, md3, md4)) properties
 *  b is the displacement which is collected as md1 and md2
 *  
 *  md3 and md4 are not used here. Later they can contain information about additional 
 *  layout influencers, e.g. module-id or attractor location.
 *  
 *  A preUpdate step is done for each influencing factor. Metadata and current position 
 *  are merged in a final update step.
 */
def preUpdatePos(a: (String, Double, Double, (Double,Double,Double,Double)), b: (Double, Double)) : (String, Double, Double, (Double,Double,Double,Double)) = {

    // a - existing position 
    // b - update to the current position
    
	(a._1, a._2, a._3, (a._4._1 + b._1, a._4._2 + b._2,0.0,0.0))
}

/**
 *
 *
 *
 */
def repulsionForce(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2

	var deltaLength = math.max(epsilon, delta.lenght)
	val force = k * k / deltaLength
	val disp = delta * force / deltaLength
	
	(disp.x, disp.y)		
}


/**
 *
 *
 *
 */
def attractionForce(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2
	val deltaLength = math.max(epsilon, delta.lenght) // avoid x/0
	val force = deltaLength * deltaLength / k

	val disp = delta * force * ATT_SCALE / deltaLength
	
	(disp.x, disp.y)		
}

def attractionForceInverted(mp: ((Double,Double),(Double,Double)) ) : (Double,Double) = {	

	val v1 = new Vector( mp._1._1, mp._1._2 )			
	val v2 = new Vector( mp._2._1, mp._2._2 )			

	val delta = v1 - v2
	val deltaLength = math.max(epsilon, delta.lenght) // avoid x/0
	val force = deltaLength * deltaLength / k

	val disp = delta * ( -1.0 * force * ATT_SCALE) / deltaLength
	
	(disp.x, disp.y)		
}


/**
 * Calc Repulsion Force for all pairs of Vertexes in the cluster ... no PREGEL
 */
def calcRepulsion( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {


//
//
// THIS MUST BE CHANGED !!!  
//
//	val repV: VertexRDD[(Double, Double)] = g.mapReduceTriplets[(Double, Double)](
//
//  		triplet => {
//  			Iterator( (triplet.dstId, ( triplet.srcAttr._2, triplet.srcAttr._3)) )
//		},
//  		(m1, m2) => repulsionForce( (m1,m2) ) // Reduce Function for all contributions from all neighbors
//        ) // repulsionV is now the VertexRDD
//
//	
//	val disp: VertexRDD[(Double, Double)] = repV.aggregateUsingIndex(repV, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))
//
//	val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(disp)( (id, a, b) => preUpdatePos(a,b) )
//
// def cartesian[U](other: RDD[U])(implicit arg0: ClassTag[U]): RDD[((VertexId, VD), U)]
   
   // all pairs
   //    val pairsV: VertexRDD[(Double, Double)] = g.vertices.cartesian

   // map over all pairs and calc force, which consists of two components, in opposite 
   // direction. Since cartesian contains all pairs we can use the "ID-Orientation" to 
   // define the orientation of the force component. In case of ID_a > ID_b we use +
   // otherwise - as sign.
   
   // This means we emit:   ID, forceOnID( ID, ID2 ) in order to calc the total 
   // contribution to ID from all nodes.


   // Finally, build a new Graph.

    //    val graphN = Graph(setC, g.edges, defaultNode)
    //    graphN
    
    g
}



/**
 * Calc Attraction Force for linked pairs of Vertexes in the cluster sing PREGEL
 */
def calcAttraction( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

/*
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
*/

	  // val disp1: VertexRDD[(Double, Double)] = attr1.aggregateUsingIndex(attr1, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))
	  // val disp2: VertexRDD[(Double, Double)] = attr2.aggregateUsingIndex(attr2, (p1, p2) => ( p1._1 + p2._1 , p1._2 + p2._2 ))

	  // val setC: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g.vertices.innerJoin(disp1)( (id, a, b) => preUpdatePos(a,b) )

      // val g2 = Graph(setC, g.edges, defaultNode)

	  // val setD: VertexRDD[(String, Double, Double, (Double,Double,Double,Double))] = g2.vertices.innerJoin(disp2)( (id, a, b) => preUpdatePos(a,b) )

      //  val graphN = Graph(setD, g.edges, defaultNode)

      //  graphN	 
        
        g

}





/**
 * The Fruchetman Reingold Layout is calculated with n = 10 iterations.
 *
 *
 */
//def layoutFDFR2( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {


//        ci = 0

//	g.cache()

//	println( "> Start the Layout procedure: n=" + iterations + " (nr of iterations)." )

//        var gs = shuffle( g )
//	println( "> Shuffled the graph." )

//	temperature = 0.1 * math.sqrt(area) // current temperature

//	for(iteration <- 1 to iterations) {

//		ci = iteration
	
//		println( "> Temperature: (T=" + temperature + ")" )

		// Repulsion is usually for all pairs of vertexes if they are different vertexs ...
        // But for simplification we use only the neighbors.
	  	// val gRep = calcRepulsion( gs )

		// Attraction is along the links only
	  	// val gAttr = calcAttraction( gRep ) 

		// WE CAN DEBUG EACH STEP 
 		// dumpWithLayout( gAttr, fileNameDebugDump, "#" )

		// Repulsion and Attraction are in super position as they are overlaing forces
		// def mapValues[VD2](map: (VertexId, VD) => VD2): VertexRDD[VD2]
	 	// val vNewPositions = gAttr.vertices.mapValues( (id, v) => updatePos( v ) )
        
        // gs = Graph(vNewPositions, g.edges, defaultNode)

	    // cool
		// cool(iteration)
//	}

//	gs // this is the last state of our layout


//    g	
//}

/**
 * The Fruchterman Reingold Layout is calculated with n = 10 iterations.
 *
 *
 */

def layoutFDFRLocally( g: Graph[ (String, Double, Double, (Double,Double,Double,Double)),Double ], 
                       i: Integer,
                       fn: String ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = {

    println( "### FOLDER: " + fn )
    val file = new File( fn )
    file.mkdirs()
  
    println( "### " + file.exists )


    println( ">>> Layout : layoutFDFRLocally() ..."  )

    val displacements = new java.util.Hashtable[String,Vector]()
    val verts = new java.util.Hashtable[Long,(Long,(String, Double, Double, (Double,Double,Double,Double)))]()
    
   	println( "> Start the Layout procedure: n=" + i + " (nr of iterations)." )

//    var gs = shuffle( g )
    var gs = g
//	  println( "> Shuffled the graph g into gs." )

	  val edges = gs.edges
    val vertices= gs.vertices
    
    var eA = edges.collect
    var vA = vertices.collect
    
	temperature = 0.1 * math.sqrt(area) // current temperature

	for(iteration <- 1 to i) {
    
	  
		ci = iteration
	
		println( "> Temperature: (T=" + temperature + ")" )

		// Repulsion is calculated for all pairs of vertexes if they are 
		// different vertexes with different IDs ...
		  /***
	  	 *      FOR THE PARALLEL VERSION WE USE THIS:
	  	 *  
	  	 * val gRep = calcRepulsion( vertices )
	  	 *
	  	 */
	  	for(i <- 0 until vA.length){
	  	    
	  	    // set vertex.disp to zero
	  	  var vertex_disp = new Vector( 0.0, 0.0 )
	   		val P1 = vA(i)
   	    var contribs = 0
   	    
    		for(j <- 0 until vA.length){
    		  	
    			println("i'th element is: " + P1 + " >>> " + P1.getClass + " >>>> " + P1._2.getClass );
    			
					if ( i != j )  {
    			  contribs = contribs + 1
    				val P2 = vA(j)
    			
    			//println("j'th element is: (VERTEX) " + p2);
				
					val disp = repulsionForce( ( (P1._2._2,P1._2._3),(P2._2._2,P2._2._3)) )
				
					// increase vertice.disp by disp
					vertex_disp = vertex_disp + new Vector( REP_SCALE * disp._1, REP_SCALE * disp._2 ) 
				
					println("    => F_REP( " + i + " " + j + " ): " + disp)
					
				}
			}
			println("    => F_REP_total( " + i + " ): {" + contribs + "} <" + P1 + "> " + vertex_disp.asString)
      val nodeDispl = displacements.get( ""+P1._1 );
			if ( nodeDispl == null ) {
			  displacements.put( ""+P1._1, vertex_disp );
			  verts.put( P1._1, P1 )
			}
			
		}

	
    //
		// Attraction is along the links only, so we iterate on the linklist
		//
	  // val gAttr = calcAttraction( edges ) 
	 	
		for(i <- 0 until eA.length){
				
		    val p1 = eA(i).srcId
		    val p2 = eA(i).dstId

		    val P1 = verts.get( p1 )
		    val P2 = verts.get( p2 )

		    val coords = ( (P1._2._2,P1._2._3) ,(P2._2._2,P2._2._3) )
		    
		    val f1 = attractionForce( coords )
		    val f2 = attractionForceInverted( coords )
		  
		    val vertex_dispF1 = new Vector( f1._1, f1._2 )
		    val vertex_dispF2 = new Vector( f2._1, f2._2 )
		    
    		println("i'th element is: (EDGE) " + eA(i));
		 	  println("    => FORCE 1: ... " + f1);
	     	println("    => FORCE 2: ... " + f2);
	     	

	     	val nodeDispl1 = displacements.get( ""+P1._1 );
	     	val nodeDispl2 = displacements.get( ""+P2._1 );

	     	if ( nodeDispl1 == null ) {
	     	  displacements.put( ""+P1._1, vertex_dispF1 );
	     	}
	     	else {
	     	  displacements.put( ""+P1._1, nodeDispl1 + vertex_dispF1 );
	     	}
	     	
	     	if ( nodeDispl2 == null ) {
	     	  displacements.put( ""+P2._1, vertex_dispF2 );
	     	}
	     	else {
	     	  displacements.put( ""+P2._1, nodeDispl2 + vertex_dispF2 );
	     	}
	     	
			}

		for( i <- 0 until vA.length ){
		  
		  	var p1 = vA(i)
		  	
		  	val f1 = displacements.get( ""+p1._1 )
		  	
		  	println ( "MOVE: " + p1 +  " by " + f1.asString );
        // println ( "    : " + p1.getClass  + "  *** " + vA.getClass);

        val nP = (p1._1,(p1._2._1, p1._2._2 + f1.x, p1._2._3 + f1.y, (0.0,0.0,0.0,0.0)))
    
		  	vA(i) = nP; 
		  	
		}
	     	
	    

		
		// OPTIONALLY WE CAN DEBUG EACH STEP 
 		// dumpWithLayout( gAttr, fileNameDebugDump, "#" )

		// Repulsion and Attraction are in super position 
		// so lets simply add all contributions 	
		
        
	  // cool the system and go on to next step ...
		cool(iteration)
	}

  ///// WRITE THE final layout 
    
  
  val bwN = new BufferedWriter( new FileWriter( fn + "/nodes.csv" ) )
    
  println( vA )

  bwN.write( "label" + "\t" + "posX" + "\t" + "posY" + "\n" )
  for(i <- 0 until vA.length){
    	println("i'th element is: (VERTEX) " + vA(i));
    	val n = vA(i)
    	bwN.write( n._2._1 + "\t" + n._2._2 + "\t" + n._2._3 + "\n" )
	}
	bwN.close()
	
	val bwL = new BufferedWriter( new FileWriter( fn + "/links.csv" ) )
  
	println( eA );
  bwL.write( "SOURCE"  + "\t" + "TARGET" + "\t" + "WEIGHT" + "\n" )
	for(i <- 0 until eA.length){
    	println("i'th element is: (EDGE) " + eA(i));
    	val e = eA(i)
    	bwL.write( e.srcId + "\t" + e.dstId + "\t1.0\n" )
	}
	bwL.close()
	 
	println( "> Final Temperature: (T=" + temperature + ")" )
	
	// finally, we create another graph since we want to continue in Spark ...
	// gs = Graph(vNewPositions, g.edges, defaultNode)


	g // this is the last state of our layout
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
def createDemoGraph( gn: String ) : Graph[ (String, Double, Double, (Double,Double,Double,Double)), Double ] = { 
        
  //                              name,       x,      y,     f.x,   f.y,  att1,att2
	val nodes: RDD[(VertexId, (   String,  Double, Double, (Double,Double,Double,Double)))] =
  		  sc.parallelize(Array((1L, ("a",   180.0,   50.0, (0.0   ,0.0   ,0.0   ,0.0   ))), 
                             (2L, ("b",   600.0,  100.0, (0.0   ,0.0   ,0.0   ,0.0   ))),
        		                 (3L, ("c",    30.0,  130.0, (0.0   ,0.0   ,0.0   ,0.0   ))), 
                             (4L, ("d",   130.0,  830.0, (0.0   ,0.0   ,0.0   ,0.0   ))), 
                             (5L, ("e",   400.0,  400.0, (0.0   ,0.0   ,0.0   ,0.0   )))))

	val statLink: RDD[Edge[Double]] =
  		  sc.parallelize(Array(Edge(1L, 2L, 1.0), 
                             Edge(1L, 3L, 1.0), 
                             Edge(2L, 3L, 1.0),
                  	 	       Edge(3L, 5L, 1.0),
                  	 	       Edge(4L, 1L, 1.0), 
                             Edge(4L, 2L, 1.0),
				                     Edge(4L, 3L, 1.0), 
                             Edge(5L, 4L, 1.0)))

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
//val fileName = "blog/wiki-Talk.txt"
//val fileNameDump = WD + "/fdl/wiki-talk-graph.dump"
//val graphS = loadEdges( fileName )
//val cGraphS = convert( graphS )


/**
 *  Create the DEMO-Graph
 *
 *      simple: 
 *      
 */
val gn = "simple"
val cGraphS = createDemoGraph( gn )
val fileNameDump = WD + "/fdl/demo-graph" + gn + ".dump"

//
// Just to be sure what was loaded ...
//
dump( cGraphS, fileNameDump + gn + ".PREP" )
// dumpWithLayout( cGraphS, fileNameDump + gn + ".PREP", "#" )

val sizeOfGraph = cGraphS.vertices.count()
println( "> Size of the graph  : " + sizeOfGraph + " nodes." )
 
// 
// define the spring constant
// 
k = 0.8 * math.sqrt(area / sizeOfGraph) // force constant

REP_SCALE = 0.0001
ATT_SCALE = 0.00001

println( "> Area               : " + area  )
println( "> Spring constant    : " + k + " a.u." )
println( "> Graph data and area were prepared sucessfully." )
println( "> Ready to do a layout." )

val gLSL = layoutFDFRLocally( cGraphS, 0 , "/GITHUB/ETOSHA.WS/tmp/original_" + gn  )
val gLSL = layoutFDFRLocally( cGraphS, 500 , "/GITHUB/ETOSHA.WS/tmp/fdl_" + gn  )

// val gLS  = layoutFDFR2( cGraphS )

dumpWithLayout( gLSL, fileNameDump , "" )

println( "> DONE!" )
println( "> Created EDGE list: " + fileNameDump )

// val result1 = "gnuplot ./blog/fdlplot.gnuplot" !!

val result2 = "gnuplot ./blog/fdlplot-local.gnuplot" !!

println(result2)












        

