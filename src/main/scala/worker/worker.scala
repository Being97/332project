package worker

import java.net.InetAddress
import java.util.logging.Logger

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

object Worker {
  private val logger = Logger.getLogger(classOf[ConnectionClient].getName)
  val usage = "Usage: worker [masterIP]"

  def main(args: Array[String]): Unit = {
    val masterIP = args.headOption.getOrElse("localhost")
    logger.info("masterIP : " + masterIP)
    val client = new ConnectionClient(masterIP, 50051)
    try {
      logger.info("My IP: " + InetAddress.getLocalHost.getHostAddress)
      client.connectionRequest(workerIP = InetAddress.getLocalHost.getHostAddress)
    } finally {
      client.awaitTermination()
      // client.shutdown()
    }
  }
}
