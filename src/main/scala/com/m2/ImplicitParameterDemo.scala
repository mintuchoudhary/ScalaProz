/**
implicit parameter are not passed when calling method and are searched in scope, they are declared with implicit keywords, n even method parameter uses implicit. they are like default parameter
*/
object ImplicitParameterDemo { 

      

    def main(args: Array[String])
    { 

        val value = 10

        implicit val multiplier = 3

        def multiply(implicit by: Int) = value * by 

          

        // Implicit parameter will be passed here 

        val result = multiply  

          

        // It will print 30 as a result 

        println(result)  

         //check this
          //println(multiply(10))

    } 
} 
