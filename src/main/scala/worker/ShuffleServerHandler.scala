package worker

import scala.concurrent.ExecutionContext
import scala.collection.mutable.Map

import java.util.logging.Logger
import java.io.{File, IOException}

// import message.shuffleTry.{ShuffleGrpc, ShuffleTryRequest, ShuffleTryDone}
import connection.message._
import WorkerTool._
// import common.{WorkerInfo, FileHandler, loggerLevel}


class ShuffleServerHandler(serverPort: Int, id: Int, partitionDir: String, shuffledDir: String, numWorkers: Int) {
  val logger = Logger.getLogger(classOf[ShuffleServerHandler].getName)  

  var server: ShufflingServer = null

  def serverStart(): Unit = {
    server = new ShufflingServer(ExecutionContext.global, serverPort, id, shuffledDir, numWorkers)
    server.start
  }

  def serverStop(): Unit = {
    if (server != null) {
      server.stop
    }
    server = null
  }

  // to be implemented .. .. ...
  def shuffle(workersIP: Map[Int, String]): Unit = {

    System.out.println(workersIP)

    /* Send partition to other workers */
    for {
      workerId <- (0 until workersIP.size)
    } {
      logger.info(s"[ShuffleServerHandler] Send partition, ${id} => ${workerId}")
      var client: ShufflingClient = null
      try {
        if (workerId != id) {
          val workerIP = workersIP(workerId)
          client = new ShufflingClient(workerIP, 50052, id, partitionDir)
          client.shuffle(workerId)
        }
        else{
          val ListofSelfFiles: List[String] = WorkerTool.getListOfSendingFiles(partitionDir, id).map(_.getName).toList
          WorkerTool.moveFileTool(ListofSelfFiles, partitionDir, shuffledDir)
        }
      } finally {
        if (client != null) {
          client.shutdown
        }
      }
    }
  }
}