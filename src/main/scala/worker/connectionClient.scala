package worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import java.net.InetAddress
import java.io.FileInputStream
import java.io.File
import scala.io.Source
import io.grpc.Status
import scala.concurrent.{Promise}

import connection.message._
import connection.message.ConnectionGrpc.ConnectionBlockingStub
import connection.message.ConnectionGrpc.ConnectionStub
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString

class ConnectionClient(host: String, port: Int){
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  val blockingStub = ConnectionGrpc.blockingStub(channel)
  val asyncStub = ConnectionGrpc.stub(channel)
  
  val inputDir = System.getProperty("user.dir") + "/src/main/resources/worker"

  val logger = Logger.getLogger(classOf[ConnectionClient].getName)
  var id: Int = -1

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(100, TimeUnit.SECONDS)
  }

  def awaitTermination(): Unit = {
    channel.awaitTermination(100, TimeUnit.SECONDS)
  }

  def sample(){
    logger.info("sample")
  }

  /** request connection to master. */
  def connectionRequest(workerIP: String): Unit = {
    logger.info("Will try to connect to master")
    val request = ConnectionRequestMsg(workerIP = workerIP)
    try {
      val response = blockingStub.initConnect(request)

      id = response.workerId
      logger.info("My ID : " + id)

      if (response.isConnected)
        logger.info("Connection Successed")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  // transfer sample to master
  def sampleDataTransfer(samplePromise: Promise[Unit]): Unit = {
    logger.info("[sampleDataTransfer] Try to send sample")

    val responseObserver = new StreamObserver[SampleDone]() {
      override def onNext(response: SampleDone): Unit = {
        if (response.successed == true) {
          samplePromise.success()
        }
      }

      override def onError(t: Throwable): Unit = {
        logger.warning("[sampleDataTransfer] Server response Failed")
        samplePromise.failure(new Exception)
      }

      override def onCompleted(): Unit = {
        logger.info("[sampleDataTransfer] Done sending sample")
      }
    }

    val requestObserver = asyncStub.sample(responseObserver)

    try {
      val source = Source.fromFile(inputDir + "/sample")
      for (line <- source.getLines) {
        val request = SampleTransfer(workerId = id, sampledData = ByteString.copyFromUtf8(line+"\r\n"))
        requestObserver.onNext(request)
      }
      source.close
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
    // Mark the end of requests
    requestObserver.onCompleted()
  }
}