package edu.ucr.cs.cs167.group16

/**
 * @author ${Group 16 }
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

  // Task #1

  // Task #2

  // Task #3

  // Task #4: Kevin

}
