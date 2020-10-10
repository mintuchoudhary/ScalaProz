package com.m2

/**
 * map = tranformation, foreach = action
 */
object ForEachMap {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //bin/winutil.exe

  def main(args: Array[String]): Unit = {
    val intList = List(2, 3, 5, 6, 7)
    intList.map(v => v + 10).foreach(x => print(" " + x))

  }
}
