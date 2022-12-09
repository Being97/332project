package worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import java.net.InetAddress

import connection.message.{ConnectionRequestMsg, ConnectionGrpc}
import connection.message.ConnectionGrpc.ConnectionBlockingStub
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

class ConnectionClient(host: String, port: Int){
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  val blockingStub = ConnectionGrpc.blockingStub(channel)
  val logger = Logger.getLogger(classOf[ConnectionClient].getName)
  
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(100, TimeUnit.SECONDS)
  }

  def awaitTermination(): Unit = {
    channel.awaitTermination(100, TimeUnit.SECONDS)
  }

  /** request connection to master. */
  def connectionRequest(workerIP: String): Unit = {
    logger.info("Will try to connect to master")
    val request = ConnectionRequestMsg(workerIP = workerIP)
    try {
      val response = blockingStub.initConnect(request)
      if (response.isConnected)
        logger.info("Connection Successed")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}