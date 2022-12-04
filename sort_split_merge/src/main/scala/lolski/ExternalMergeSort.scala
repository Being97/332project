package lolski

import lolski.Queue.ScalaQueue
import scala.collection._
import scala.concurrent.{Future, ExecutionContext}
import scala.math._
import scala.math.Ordering
import scala.language.postfixOps


object ExternalMergeSort {

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


  def sort(input: String, tmpDir: String, output: String, linesPerChunk: Int)
      (implicit ec: ExecutionContext): Future[String] = {

    // split file into n chunks
    val chunks = splitStep(input, linesPerChunk, tmpDir)

    // sort each chunk locally, in parallel
    val sortAsync = sortStep(chunks)

    // merge sorted chunks using k-way merge algorithm
    val merge = sortAsync map { sorted =>
      assert(sorted.size != 0)
      val n = max(1, linesPerChunk / sorted.size) // n should be between 1 to linesPerChunk / sorted.size
      mergeStep_tag(sorted, output, n)
    }

    // clean up
    merge onComplete { case _ => IO.delete(chunks.toList) }

    merge
  }


  def splitSortPartitonStep(input: String, tmpDir: String, output: String, linesPerChunk: Int)
      (implicit ec: ExecutionContext): Future[Seq[String]] = {

    // split file into n chunks
    val chunks = splitStep(input, linesPerChunk, tmpDir)

    // sort each chunk locally, in parallel
    val sortAsync = sortStep(chunks)
    sortAsync

    // need to imply partitioning,
  }

  def mergeStep(targetSeq : Seq[String], linesPerChunk : Int, output : String): Future[String] ={
    val n = max(1, linesPerChunk / targetSeq.size)
    val merge = Future(mergeStep_tag(targetSeq, output, n))

    merge onComplete()

  }




  def splitStep(in: String, linesPerChunk: Int, tmp: String): Seq[String] = {
    val (handle, lines) = IO.readLines(in)
    val chunked = lines.grouped(linesPerChunk).zipWithIndex

    val chunks = chunked map { case (chunk, id) =>
      val out = s"$tmp/chunk-$id" // chunk name
      IO.writeSeq(chunk, out, true)
      out
    } toList

    handle.close()

    chunks
  }

  // sort in memory
  // must limit the number of instance being processed concurrently to prevent OutOfMemoryException
/*
  def sortStep(chunks: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = {
    val async  = chunks map { path => Future(sortChunk(path)) }
    val joined = Future.sequence(async)
    joined
  }

  def sortChunk(path: String): String = {
    val (handle, lines) = IO.readLines(path)
    val it = lines.toArray.map(_.toInt)
    handle.close() // clean up
    IO.writeSeq(it.sorted.map(_.toString), path, true) // 1 pass to write
  }
  */

  def sortStep(chunks: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = {
    val async  = chunks map { path => Future(sortChunk(path)) }
    val joined = Future.sequence(async)
    joined
  }
  def sortChunk(path: String): String = {
    def f10byteCompare(elemA:String, elemB:String): Boolean =
    {
      val aByte = elemA.take(10).map(_.toByte).toList
      val bByte = elemB.take(10).map(_.toByte).toList


      
      def compareTostring(eA:List[Byte], eB:List[Byte]): Boolean = 
        eA match {
          case Nil => true
          case _ =>{
          if(eA.head > eB.head) // A > B
            false
          else if (eA.head < eB.head) // A < B
            true
          else
            compareTostring(eA.tail, eB.tail)
        }
      }
      compareTostring(aByte, bByte)
    }
    val (handle, lines) = IO.readLines(path)
    val it = lines.toArray
    handle.close() // clean up
    IO.writeSeq(it.sortWith(f10byteCompare).map(_.toString), path, true) // 1 pass to write
  }

  /*
  // using k-way merging algorithm to merge sorted chunks
  def mergeStep(chunks: Seq[String], out: String, linesPerChunk: Int): String = {
    // initialize variables - priority queue, file readers
    val readers = chunks.zipWithIndex map { case (chunk, id) =>
      val (handle, it) = IO.readLines(chunk)
      // chunk id is used to idenfity which chunk reader to advance after dequeing an element from the queue
      val indexed = it map { e => (e, id) }
      (handle, indexed)
    } toVector
    val totalSize = readers.size
    var closedReaders = mutable.Set[Int]()
    // the priority queue stores (Int, Int), i.e., the actual value as well as the chunk id where it resides in
    // a crucial part of the k-way merge algorithm is to advance the reader of a particular chunk reader
    // after an element has been dequeued from the queue, hence why we store the chunk id along with the actual element
    val sortedQueue = new ScalaQueue[(Int, Int)](Ordering.by { case (v, i) => -v})
    //val sortedQueue2 = new ScalaQueue[nameId](TagOrdering)

    TagOrdering
    // ### fix part 1, int, int queue => string, int queue,
    // ### need Tag to int function...

    // initialize the priority queue with the first lines of each chunks
    val lines = readers flatMap { case (_, it) =>
        val tmp = it.take(linesPerChunk).map { case (v, i) => (v.toInt, i) }
        tmp toSeq
      }
    // ### fix part 2, v data type is string, so v.toInt => v change


    lines foreach { e => sortedQueue.enqueue(e) }

    // then, iterate over the rest of the chunks and put values into the priority queue,
    // before finally writing the sorted data to 'out'
    IO.overwrite(out) { writer =>
      while (closedReaders.size < totalSize || sortedQueue.notEmpty) {
        // dequeue an element 'v1' and write it to 'out'
        val (e, chunkId) = sortedQueue.dequeue()
        writer.write(s"$e")
        writer.newLine()

        // then read the next line from chunk 'chunkId' put them in the priority queue
        val next = readers(chunkId)._2
          .map { case (v, i) => (v.toInt, i) }
          .toSeq
        next foreach { e => sortedQueue.enqueue(e) }
        // ### fix part 3, v data type is string, we have string compare, so remove this map..?


        // check if chunk 'chunkId' has been fully processed
        // if it has, close the reader and store the id in 'closedReaders'
        val it     = readers(chunkId)._2
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
  }*/
  // using k-way merging algorithm to merge sorted chunks
  def mergeStep_tag(chunks: Seq[String], out: String, linesPerChunk: Int): String = {
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

}

