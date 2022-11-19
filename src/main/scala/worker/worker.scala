package worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import java.net.InetAddress

import connection.message.{ConnectionRequestMsg, ConnectionGrpc}
import connection.message.ConnectionGrpc.ConnectionBlockingStub
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

object HelloWorldClient {
  def apply(host: String, port: Int): HelloWorldClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    new HelloWorldClient(channel, blockingStub)
  }

  val usage = "Usage: worker [masterIP]"

  def main(args: Array[String]): Unit = {
    val masterIP = args.headOption.getOrElse("localhost")
    System.out.println("masterIP : " + masterIP)
    val client = HelloWorldClient(masterIP, 50051)
    try {
      System.out.println("My IP: " + InetAddress.getLocalHost.getHostAddress)
      client.greet("My IP")
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
  private[this] val logger = Logger.getLogger(classOf[HelloWorldClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(100, TimeUnit.SECONDS)
  }

  def awaitTermination(): Unit = {
    channel.awaitTermination(100, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def greet(ip: String): Unit = {
    logger.info("Will try to connect to master")
    val request = ConnectionRequestMsg(workerIP = ip)
    try {
      val response = blockingStub.sayHello(request)
      if (response.isConnected)
        logger.info("Connection Successed")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}