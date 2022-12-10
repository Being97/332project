package WorkerTool

import java.util.concurrent._
import scala.concurrent.{ExecutionContext, Future}
/*
object AppConf {
  // input
  val baseTmp       = s"${IO.getCwd}/files"
  val in            = s"${baseTmp}/intest.txt" // input file (unsorted) // intest : 10000개
  val out           = s"${baseTmp}/outtest.txt" // output file (sorted)

  // sorting params
  val start         = 1
  val stop          = 10004
  val linesPerChunk = 320000 // 32MB
  val parallelism   = 8

}

object Main {

  /*
  val threadPool = Executors.newFixedThreadPool(AppConf.parallelism)
  implicit val parallelSortEC = ExecutionContext.fromExecutor(threadPool)
  */
  def main(args: Array[String]): Unit = {
    println(s"start sorting file ${AppConf.in}...")

    Wok
    val ChunkName =  WorkerTool.splitTool(
      input = AppConf.in,
      tmpDir = AppConf.baseTmp,
      linesPerChunk = AppConf.linesPerChunk,
      myNum = 1
    )
    val key = List(
      " bga<b8 B5",
      "$40NH+%uoP",
      "'M??UWQmB&",
      "+KUX%q`X6p",
      "/.7O7W.lLt"
    )
    val samplePath = WorkerTool.sampleSortTool(
      chunks = ChunkName,
      tmpDir = AppConf.baseTmp,
      partitonKey = key,
      myNum = 1
    )

    val success = WorkerTool.checkIsSortedTool(Chunk_partitioned)
    print(ChunkName)
    print("\n")
    print(Chunk_partitioned)
    print("\n")
    print(success)
    print("\n")
    val remove = WorkerTool.removeChunkTool(ChunkName)

    val countL = List(
      6, 6, 6, 6, 6, 6
    )
    val merge = WorkerTool.mergeTool_for_me(
      Chunk_partitioned,
      tmp = AppConf.baseTmp,
      myNum = 1,
      linesPerChunk = AppConf.linesPerChunk,
      output =AppConf.out
    )
    //val remove_2 = WorkerTool.removeChunkTool(Chunk_partitioned)
    val success2 = WorkerTool.checkIsSortedTool(List(AppConf.out))
    print(success2)
    print("\n")


    println(s"end sorting file ${AppConf.in}...")

  }
}


*/