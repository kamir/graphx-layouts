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


def repulsionForce(mp1:(Double,Double), mp2: (Double,Double) ) : (Double,Double) = {	

	val v1 = new Vector( mp1._1, mp1._2 )			
	val v2 = new Vector( mp2._1, mp2._2 )			

	val delta = v1 - v2
		
	return mp1
}

val a: (Double,Double) = (2.0,3.0)
val b: (Double,Double) = (3.0,4.0)

val c= repulsionForce( a,b )

