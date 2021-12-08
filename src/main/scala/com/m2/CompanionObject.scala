package com.m2

/**
 * 1. An object and class can be in same class with same name
 * 2. A companion object and its class can access each other’s private members (fields and methods)
 */
class CompanionObject {
  def hello(){
    println("Hello, this is Companion Class.")
  }
}
object CompanionObject {
  def main(args:Array[String]){
    new CompanionObject().hello()
    println("And this is Companion Object.")
  }

}