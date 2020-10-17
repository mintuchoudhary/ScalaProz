
// Scala program add two numbers 
// using Currying function 

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
