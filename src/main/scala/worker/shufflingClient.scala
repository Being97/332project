package worker

import com.google.protobuf.ByteString

import message.shuffle.StatusEnum
import message.shuffle.{ShuffleGrpc, ShuffleRequest, ShuffleDone}
import common._

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import java.io.File

import scala.io.Source
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._

import io.grpc.{ManagedChannelBuilder, Status}
import io.grpc.stub.StreamObserver

class ShufflingClient(host: String, port: Int, id: Int, tempDir: String) {
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext.build
  val blockingStub = ShuffleGrpc.blockingStub(channel)
  val asyncStub = ShuffleGrpc.stub(channel)
  val logger: Logger = Logger.getLogger(classOf[ShufflingClient].getName)

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
  
  def getListOfSendingFiles(inputDir: String, receiverID: Int): List[File] = {
    val dir = new File(inputDir)
    if (dir.exists && dir.isDirectory) {
      val fileList = dir.listFiles
      fileList.filter(file => file.isFile && file.getName.startsWith("chunk") &&
      file.getName.split("-")(3).toInt == receiverID).toList
    } else {
      List[File]()
    }
  }

  def shuffle(receiverID: Int): Unit = {
    for {
      file <- getListOfSendingFiles(tempDir, receiverID)
    } {
      val shufflePromise = Promise[Unit]()
      requestShuffle(file, shufflePromise)
      Await.ready(shufflePromise.future, Duration.Inf)
    }
  }

  def requestShuffle(file: File, shufflePromise: Promise[Unit]): Unit = {
    logger.info("[ShufflingClient] Try to send partition")

    val responseObserver = new StreamObserver[ShuffleDone]() {
      override def onNext(response: ShuffleDone): Unit = {
        if (response.status == StatusEnum.SUCCESS) {
          shufflePromise.success()
        }
      }

      override def onError(t: Throwable): Unit = {
        logger.warning(s"[ShufflingClient] Server response Failed: ${Status.fromThrowable(t)}")
        shufflePromise.failure(new Exception)
      }

      override def onCompleted(): Unit = {
        logger.info("[ShufflingClient] Done sending partition")
      }
    }

    val requestObserver = asyncStub.shuffle(responseObserver)

    try {
      val source = Source.fromFile(file)
      val partitionID = file.getName.split("-")(2).toInt
      for (line <- source.getLines) {
        val request = ShuffleRequest(workerID = id, partitionID = partitionID, shuffleData = ByteString.copyFromUtf8(line+"\r\n"))
        requestObserver.onNext(request)
      }
      source.close
    } catch {
      case e: RuntimeException => {
        // Cancel RPC
        requestObserver.onError(e)
        throw e
      }
    }

    // Mark the end of requests
    requestObserver.onCompleted()
  }
}
