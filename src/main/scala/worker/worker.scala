package worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import java.net.InetAddress

import connection.message.{ConnectionRequestMsg, ConnectionGrpc}
import connection.message.ConnectionGrpc.ConnectionBlockingStub
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

object HelloWorldClient {
  private val logger = Logger.getLogger(classOf[HelloWorldClient].getName)

  def apply(host: String, port: Int): HelloWorldClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    new HelloWorldClient(channel, blockingStub)
  }

  val usage = "Usage: worker [masterIP]"

  def main(args: Array[String]): Unit = {
    val masterIP = args.headOption.getOrElse("localhost")
    logger.info("masterIP : " + masterIP)
    val client = HelloWorldClient(masterIP, 50051)
    try {
      logger.info("My IP: " + InetAddress.getLocalHost.getHostAddress)
      client.connectionRequest(workerIP = InetAddress.getLocalHost.getHostAddress)
    } finally {
      client.awaitTermination()
      // client.shutdown()
    }
  }
}

class HelloWorldClient private(
  private val channel: ManagedChannel,
  private val blockingStub: ConnectionBlockingStub
) {

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(100, TimeUnit.SECONDS)
  }

  def awaitTermination(): Unit = {
    channel.awaitTermination(100, TimeUnit.SECONDS)
  }

  /** request connection to master. */
  def connectionRequest(workerIP: String): Unit = {
    HelloWorldClient.logger.info("Will try to connect to master")
    val request = ConnectionRequestMsg(workerIP = workerIP)
    try {
      val response = blockingStub.sayHello(request)
      if (response.isConnected)
        HelloWorldClient.logger.info("Connection Successed")
    }
    catch {
      case e: StatusRuntimeException =>
        HelloWorldClient.logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}