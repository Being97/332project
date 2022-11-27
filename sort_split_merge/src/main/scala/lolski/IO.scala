package lolski

import java.io._
import java.nio.file.{Paths, Files}

/**
  * Created by lolski on 3/26/16.
  */

object IO {
  object NumGenerator {
    // helpers for generating number
    def shuffledOrder(start: Int, end: Int): Seq[Int] = scala.util.Random.shuffle(ascOrder(start, end))

    def ascOrder(start: Int, end: Int): Seq[Int] = start to end

    def descOrder(start: Int, end: Int): Seq[Int] = end to start by -1
  }

  def writeShuffled(from: Int, to: Int, path: String): Unit = overwrite(path) { in =>
    val numbers = NumGenerator.shuffledOrder(from, to)
    numbers.foreach { e =>
      in.write(s"$e")
      in.newLine()
    }
  }

  def writeSeq(numbers: Seq[String], out: String, shouldOverwrite: Boolean): String = {
    // pick overwrite or append function based on shouldOverwrite flag
    def fn[T]: String => (BufferedWriter => T) => T =
      if (shouldOverwrite) overwrite _ else append _

    fn(out) { writer =>
      numbers foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    }

    out
  }

  // file writing helpers
  def overwrite[T](path: String)(block: BufferedWriter => T): T = {
    Files.deleteIfExists(Paths.get(path))
    append(path) { in =>
      block(in)
    }
  }

  def append[T](path: String)(block: BufferedWriter => T): T = {
    withWriter(new BufferedWriter(new FileWriter(path, true))) { in =>
      block(in)
    }
  }

  def withWriter[T](res: BufferedWriter)(block: BufferedWriter=> T): T = {
    try block(res)
    finally res.close()
  }

  def readLine(path: String, offset: Long): (String, Long) = {
    val access = new RandomAccessFile(new File(path), "r")
    access.seek(offset)
    val str = access.readLine()
    (str, str.getBytes.length)
  }

  def readLines(path: String): (BufferedReader, Iterator[String]) = {
    val reader = new BufferedReader(new FileReader(path))
    (reader, Iterator.continually(reader.readLine()).takeWhile(_!=null))
  }

  def delete(paths: Seq[String]): Unit = paths foreach { p => Files.delete(Paths.get(p))}

  def getCwd = Paths.get("").toAbsolutePath.toString
}