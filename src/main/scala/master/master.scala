package master

import java.util.logging.Logger
import java.net.InetAddress

import io.grpc.{Server, ServerBuilder}
import connection.message.{ConnectionGrpc, ConnectionRequestMsg, ConnectionDoneMsg}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ListBuffer

object Master {
  private val logger = Logger.getLogger(classOf[ConnectionServer].getName)
  private val port = 50051
  
  def main(args: Array[String]): Unit = {
    // args Example = "master 3"
    // master <#ofWorkers>
    val numWorkers = args(0).toInt
    val server = new ConnectionServer(ExecutionContext.global, numWorkers, port)
    logger.info("master IP : " + InetAddress.getLocalHost.getHostAddress + ", num of workers : " + numWorkers)
    server.start()
    server.blockUntilShutdown()
  }

}

