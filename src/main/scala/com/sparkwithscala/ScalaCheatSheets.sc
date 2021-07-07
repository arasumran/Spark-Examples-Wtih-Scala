// VALUES Immutable constants
val hello:String = "Hello There"
val numberVal:Int = 1
val boolVal:Boolean = true
val characterVal :Character = 'A'



// VARIABLES is mutable
var helya = "Helya"
var helloThere = helya +" There"


// Printing
val PiVal:Double =3.1415
println(f"Pi is : $PiVal%.2f")
println(f"for int vals : $numberVal%02d")


// FUNCTIONS
// format def <funstion name> (parameter name :type, ...) :return type = { }

def squared(x: Int) :Int = {
  x+x
}
def transformIt(x: Int ,f:Int=>Int):Int = {
  f(x)
}


// functino Examples
val result1 = transformIt(10 , x => 10/2) // 5
val result2 = transformIt(2,squared) // 4
val result3 = transformIt(2,x=>{val y = x*2;y*y})


// DATA STRUCTURES


// Tuple is immutable lists

val tupleVal = ("test1","test2",5)
println(tupleVal._1) // this is first one



// MAP : transforms each element of an RDD into one new element

// FLAPMAP : can create many new elements from each one