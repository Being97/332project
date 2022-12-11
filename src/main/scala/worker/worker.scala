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
    // argsExample = "slave 141.223.181.67:50051 -I /src/main/resources/input -O /src/main/resources/sorted"
    // worker [server ip:port] -I [input directory] -O [output directory]
    val masterIP = args(0)

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

      // Request pivot
      client.requestPivot

      // Partition
      client.partition

      // Send partitioned
      client.partitioned

      // Start server for shuffle
      client.shuffleServer

      // Request shuffle order
      client.requestShuffle

      // shuffle
      client.shuffle

      // merge sort after shuffle
      client.merge

    } finally {
      client.awaitTermination()
      // client.shutdown()
    }
  }
}
