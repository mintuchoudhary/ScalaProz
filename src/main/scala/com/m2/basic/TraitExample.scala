package com.m2.basic

/**
 * Traits support multiple inheritance. i.e classes are limited to inherit from a single abstract class
 *   but can inherit from multiple traits.
 * Traits does not contain constructor parameters.
 * Traits are completely interoperable (used in java) only when they do not contain any implementation code.
 */
trait TraitExample {


    // Abstract and non-abstract method
    def portal1
    def tutorial()
    {
      println("Scala tutorial")
    }
}

object Test extends TraitExample {
  def main(args: Array[String]): Unit = {
    tutorial()
  }

  override def  tutorial()
  {
    println("Welcome!! from main")
  }

  override def portal1 :Unit = 111 //implementatioon needed
}