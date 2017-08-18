package org.minyodev

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Test extends App {

  /*
  * mas simple
val file = bobfs.create("sample.txt")
val writer = bobfs.writer(file)
writer.write("hello bob")
writer.close
val reader = bobfs.reader(bobfs.open("sample.txt"))
reader.getLines.foreach(println)
reader.close
bobfs.listFiles.foreach(println)
  * */
//  val fsp = java.nio.file.spi.FileSystemProvider

  println("hello world")
}
