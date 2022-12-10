package worker

import java.net.InetAddress
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Promise, Await}
import scala.concurrent.duration._

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
      
      // Connect
      client.connectionRequest(workerIP = InetAddress.getLocalHost.getHostAddress)

      // Sample
      client.sample

      // Send SampleRequest
      val samplePromise = Promise[Unit]()
      client.sampleDataTransfer(samplePromise)
      Await.ready(samplePromise.future, Duration.Inf)

      // request pivotting
      client.requestPivot


    } finally {
      client.awaitTermination()
      // client.shutdown()
    }
  }
}
