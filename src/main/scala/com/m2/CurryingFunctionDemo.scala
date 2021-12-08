
// Scala program add two numbers 
//Currying is the process of converting a function with multiple arguments into
// a sequence of functions that take one argument. Each function returns another function
// that consumes the following argument.

object CurryingFunctionDemo 
{ 

    def add2(a: Int) = (b: Int) => a + b; 

  

    // Main function 

    def main(args: Array[String])
    { 

        // Partially Applied function. 

        val sum = add2(29); 

        println(sum(5)); 

    } 
} 
