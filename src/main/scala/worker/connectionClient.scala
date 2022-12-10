package worker

import WorkerTool._

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import java.net.InetAddress
import java.io.FileInputStream
import java.io.{File, IOException}
import scala.io.Source
import io.grpc.Status
import System.out._

import scala.concurrent.Promise
import connection.message._
import connection.message.ConnectionGrpc.ConnectionBlockingStub
import connection.message.ConnectionGrpc.ConnectionStub
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString

import java.io.{File, PrintWriter}
import scala.io.Source

class ConnectionClient(host: String, port: Int){
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  val blockingStub = ConnectionGrpc.blockingStub(channel)
  val asyncStub = ConnectionGrpc.stub(channel)
  val logger = Logger.getLogger(classOf[ConnectionClient].getName)

  val inputDir = System.getProperty("user.dir") + "/src/main/resources/worker"
  var id: Int = -1

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

  // sample 10000 data line
  final def sample(): Unit = {
    logger.info("[Sample] Sample start")
    val sampleSize = 10000
    try {
      val dir = new File(s"$inputDir/input")

      assert(!dir.listFiles.isEmpty)
/*
      val sampleSource = Source.fromFile(dir.listFiles.toList.head)
      val sampleWriter = new PrintWriter(new File(inputDir + "/sample"))

      for (line <- sampleSource.getLines.take(sampleSize)) {
        sampleWriter.write(line + "\r\n")
      }

      sampleSource.close
      sampleWriter.close
  */

      val samplepath =  WorkerTool.sampleSortTool(
        chunks = dir.list(),
        s"$inputDir/input",
        s"$inputDir/samples",
        myNum = id
      )
      //System.out.println(samplepath)
    } catch {
      case exception: Exception => println(exception)
    }

    logger.info("[Sample] Sample Done")
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
      val source = Source.fromFile(inputDir + "/samples/sample")
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