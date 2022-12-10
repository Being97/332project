package worker

import scala.concurrent.ExecutionContext
import scala.collection.mutable.Map

import java.util.logging.Logger
import java.io.{File, IOException}

// import message.shuffleTry.{ShuffleGrpc, ShuffleTryRequest, ShuffleTryDone}
import connection.message._
// import common.{WorkerInfo, FileHandler, loggerLevel}


class ShuffleServerHandler(serverPort: Int, id: Int, tempDir: String) {
  val logger = Logger.getLogger(classOf[ShuffleServerHandler].getName)  

  var server: ShufflingServer = null

  def serverStart(): Unit = {
    server = new ShufflingServer(ExecutionContext.global, serverPort, id, tempDir)
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
    /* Send partition to other workers */
    for {
      workerId <- ((id + 1) to workersIP.size) ++ (1 until id)
    } {
      logger.info(s"[ShuffleServerHandler] Try to send partition from ${id} to ${workerId}")
      var client: ShufflingClient = null
      try {
        val workerIP = workersIP(workerId)
        client = new ShufflingClient(workerIP, 50052, id, tempDir)
        client.shuffle(workerId)
      } finally {
        if (client != null) {
          client.shutdown
        }
      }
    }
  }
}