package WorkerTool

import Queue.ScalaQueue

import scala.annotation.tailrec
import scala.collection._
import scala.concurrent.{ExecutionContext, Future}
import scala.math._
import scala.math.Ordering
import scala.language.postfixOps
import scala.util.Random


object WorkerTool{

  // these are use for merge step, make Priority queue for tag
  case class nameId(elem: String, id: Int)
  implicit object TagOrdering extends Ordering[nameId] {
    override def compare(f1: nameId, f2: nameId): Int = {
      val aByte = f1.elem.take(10).map(_.toByte).toList
      val bByte = f2.elem.take(10).map(_.toByte).toList

      def compareTostring(eA: List[Byte], eB: List[Byte]): Int =
        eA match {
          case Nil => 0
          case _ => {
            if (eA.head > eB.head) // A > B
              -1
            else if (eA.head < eB.head) // A < B
              1
            else
              compareTostring(eA.tail, eB.tail)
          }
        }
      compareTostring(aByte, bByte)
    }
  }

  def splitTool(input: String, tmpDir: String, linesPerChunk: Int, myNum : Int): Seq[String] = {
    // split file into n chunks
    val seq = splitStep(input, myNum,linesPerChunk, tmpDir)
    seq
  }

  def sampleSortTool(chunks: Seq[String], inputDir: String, outputDir :String, myNum :Int): String = {

    // sample
    val sample = sampleStep(chunks,inputDir, outputDir, myNum)
    // sort each chunk locally, in parallel
    val sorted = sortStep(chunks, inputDir)
    sample
  }

  def PartitonTool(chunks_sorted: Seq[String], partitonKey: List[String], inTmp:String, outTmp:String, myNum:Int): Seq[String] = {

    val split = PartitionStep(chunks_sorted, partitonKey, inTmp, outTmp, myNum)
    split
  }

  def mergeTool(countList: List[Int], tmp: String, myNum: Int, linesPerChunk : Int, output : String): String ={
    val targetSeq = ChunkPathGenerator(countList: List[Int], tmp: String, myNum: Int)
    val n = max(1, linesPerChunk / targetSeq.size)
    val merge = mergeStep(targetSeq, output, n)
    IO.delete(targetSeq.toList)
    merge
  }

  def mergeTool_for_me(targetSeq : Seq[String], tmp: String, myNum: Int, linesPerChunk: Int, output: String): String = {
    val n = max(1, linesPerChunk / targetSeq.size)
    val merge = mergeStep(targetSeq, output, n)
    IO.delete(targetSeq.toList)
    merge
  }

  def removeChunkTool(target: Seq[String])=
  {
    IO.delete(target.toList)
  }

  def checkIsSortedTool(target: Seq[String]): Boolean =
    {
      def isSorted(l: List[String]): Boolean = {
        val list = l.view
        !list.zip(list.tail).exists { case (x, y) => f10byteCompare(y,x) } //y < x
      }

      target.exists{ in =>
        val (handle, lines) = IO.readLines(in)
        val sorted = isSorted(lines.toList)
        handle.close()
        sorted
      }

    }

  def sampleStep(chunks: Seq[String], inTmp: String, outTmp:String, myNum: Int): String = {

    val count = 10100/chunks.length
    val sampleSeq = chunks flatMap { case in =>
      val (handle, lines) = IO.readLines(s"$inTmp/$in")
      val data = Random.shuffle(lines.toList).take(count)
      handle.close()
      data
    }
    val outpath = s"$outTmp/sample"
    IO.writeSeq(sampleSeq.take(10000).toList, outpath, true)
    outpath
  }

  def splitStep(in: String, myNum : Int, linesPerChunk: Int, tmp: String): Seq[String] = {
    val (handle, lines) = IO.readLines(in)
    val chunked = lines.grouped(linesPerChunk).zipWithIndex

    val chunks = chunked map { case (chunk, id) =>
      val out = s"$tmp/chunk-$myNum-$id" // chunk name
      IO.writeSeq(chunk, out, true)
      out
    } toList

    handle.close()

    chunks
  }

  def sortStep(chunks: Seq[String], inTmp:String): Seq[String] = {
    val async  = chunks map { path => sortChunk(s"$inTmp/$path") }
    async
  }

  def PartitionStep(chunks: Seq[String], sampleKey :List[String], inTmp:String, outTmp:String, myNum:Int) = {

    def compareKey(sampleKey: List[String], elem: String): Int = {
      @tailrec
      def compKeyIter(pos: Int, sampleKey: List[String], elem: String): Int = sampleKey match {
        case Nil => pos
        case head :: tail =>
          if (f10byteCompare(elem, head)) pos //A <B : true
          else compKeyIter(pos +1, tail, elem)
      }

      compKeyIter(0, sampleKey, elem)
    }

    def splitbyKey(in: String, fileN: Int): List[String] = {
      val (handle, lines) = IO.readLines(s"$inTmp/$in")
      val chunk_pos = lines.toList.map{case str=>
        val key =compareKey(sampleKey, str)
        (key, str)
      }
      val chunk_group = chunk_pos.groupBy(_._1)
      val chunks = chunk_group map { case (id, chunk) =>
        val out = s"$outTmp/Chunk-$myNum-$fileN-$id" // chunk name
        IO.writeSeq(chunk.map(_._2), out, shouldOverwrite = true)
        out
      } toList

      handle.close()
      chunks
    }
    chunks.zipWithIndex
      .flatMap {
      case (fname, fileN) =>
        splitbyKey(fname, fileN)
    }
  }

  def f10byteCompare(elemA: String, elemB: String): Boolean = {
    val aByte = elemA.take(10).map(_.toByte).toList
    val bByte = elemB.take(10).map(_.toByte).toList
    @tailrec
    def compareTostring(eA: List[Byte], eB: List[Byte]): Boolean =
      eA match {
        case Nil => true
        case _ => {
          if (eA.head > eB.head) // A > B
            false
          else if (eA.head < eB.head) // A < B
            true
          else
            compareTostring(eA.tail, eB.tail)
        }
      }

    compareTostring(aByte, bByte)
  }

  def sortChunk(path: String): String = {
    val (handle, lines) = IO.readLines(path)
    val it = lines.toArray
    handle.close() // clean up
    IO.writeSeq(it.sortWith(f10byteCompare).map(_.toString), path, true) // 1 pass to write
  }

  // using k-way merging algorithm to merge sorted chunks
  def mergeStep(chunks: Seq[String], out: String, linesPerChunk: Int): String = {
    // initialize variables - priority queue, file readers
    val readers = chunks.zipWithIndex map { case (chunk, id) =>
      val (handle, it) = IO.readLines(chunk)
      // chunk id is used to idenfity which chunk reader to advance after dequeing an element from the queue
      val indexed = it map { e => nameId(e, id) }
      (handle, indexed)
    } toVector
    val totalSize = readers.size
    var closedReaders = mutable.Set[Int]()
    // the priority queue stores (Int, Int), i.e., the actual value as well as the chunk id where it resides in
    // a crucial part of the k-way merge algorithm is to advance the reader of a particular chunk reader
    // after an element has been dequeued from the queue, hence why we store the chunk id along with the actual element
    //val sortedQueue = new ScalaQueue[(Int, Int)](Ordering.by { case (v, i) => -v })
    val sortedQueue = new ScalaQueue[nameId](TagOrdering)
    // ### fix part 1, int, int queue => string, int queue,
    // ### need Tag to int function...

    // initialize the priority queue with the first lines of each chunks
    val lines = readers flatMap { case (_, it) =>
      val tmp = it.take(linesPerChunk)
      tmp toSeq
    }
    // ### fix part 2, v data type is string, so v.toInt => v change


    lines foreach { e => sortedQueue.enqueue(e) }

    // then, iterate over the rest of the chunks and put values into the priority queue,
    // before finally writing the sorted data to 'out'
    IO.overwrite(out) { writer =>
      while (closedReaders.size < totalSize || sortedQueue.notEmpty) {
        // dequeue an element 'v1' and write it to 'out'
        val nameId(e, chunkId) = sortedQueue.dequeue()
        writer.write(s"$e")
        writer.newLine()

        // then read the next line from chunk 'chunkId' put them in the priority queue
        val next = readers(chunkId)._2.toSeq
        next foreach { e => sortedQueue.enqueue(e) }
        // ### fix part 3, v data type is string, we have string compare, so remove this map...


        // check if chunk 'chunkId' has been fully processed
        // if it has, close the reader and store the id in 'closedReaders'
        val it = readers(chunkId)._2
        val handle = readers(chunkId)._1
        if (it.isEmpty) {
          // close reader and put the id in closedReaders set
          val alreadyClosed = closedReaders.contains(chunkId)
          if (!alreadyClosed) {
            handle.close()
            closedReaders += chunkId
          }
          else {
            // do nothing, reader is already closed
          }
        }
      }
    }
    out
  }

  def ChunkPathGenerator(countList: List[Int], tmp: String, myNum: Int): Seq[String] = {
    val Cpath = countList.zipWithIndex.map { case (n, id) =>
      assert(n > 0)
      for (i <- 0 to n - 1 if n > 0) yield s"$tmp/chunk-$id-$i-$myNum"
    }
    Cpath.flatten
  }

  def ChunkforReceiverGenerater(receiver:Int, ChunkN:Int, myNum: Int, tmp:String): Seq[String] =
    {
      (for(i <- 0 to ChunkN -1) yield s"$tmp/chunk-$myNum-$i-$receiver").toSeq
    }

}


