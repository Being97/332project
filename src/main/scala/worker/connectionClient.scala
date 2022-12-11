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
import WorkerTool._
import scala.collection.mutable.Map

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

  val workersIP = Map[Int, String]()
  val pivots = Map[Int, String]()

  var shuffleServerHandler: ShuffleServerHandler = null

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
  def sample(): Unit = {
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

  def requestPivot(): Unit = {
    logger.info("[requestPivot] Try to get pivot")

    val pivotResponse = blockingStub.pivot(new PivotRequest(true))
    pivotResponse.status match {
      case StatusEnum.SUCCESS => {

        for (w <- pivotResponse.workerIPList) {
          workersIP(workersIP.size) = w
        }
        
        for (w <- pivotResponse.pivotsList) {
          pivots(pivots.size + 1) = w
        }
      }
      case StatusEnum.FAIL => {
        logger.info("[requestPivot] Pivot failed.")
        throw new Exception
      }
      case _ => {
        /* Wait 5 seconds and retry */
        Thread.sleep(5 * 1000)
        requestPivot
      }
    }
  }

  def partition(): Unit = {
    logger.info("[Partition] Partition start")

    val dir = new File(s"$inputDir/input")

    WorkerTool.PartitonTool(
      chunks_sorted = dir.list(),
      partitonKey = pivots.map{case(n, p) => p}.toList,
      inTmp = inputDir + "/input",
      outTmp = inputDir + "/partition",
      myNum = id
    )

    logger.info("[Partition] Partition done")
  }

  def partitioned(): Unit = {
    logger.info("[Partitioned] Partitioned start")
    val partitionedResponse = blockingStub.partitioned(new PartitionedRequest(id))
    logger.info("[Partitioned] Partitioned done")
  }

  def shuffleServer(): Unit = {
    logger.info("[ShuffleServer] ShuffleServer start")
    val partitionDir = new File(inputDir + "/partition")
    val shuffledDir = new File(inputDir + "/shuffled")
    // if (!tempDir.mkdir) throw new IOException("Could not create temporary directory: " + tempDir.getAbsolutePath)
    assert(partitionDir.isDirectory && shuffledDir.isDirectory)
    shuffleServerHandler = new ShuffleServerHandler(50052, id, partitionDir.getAbsolutePath, shuffledDir.getAbsolutePath, workersIP.toList.length)
    shuffleServerHandler.serverStart
    logger.info("[ShuffleServer] ShuffleServer done")
  }

  def requestShuffle(): Unit = {
    logger.info("[requestShuffle] Waiting shuffle order")

    val shuffleResponse = blockingStub.shuffleTry(new ShuffleTryRequest(true))
    shuffleResponse.status match {
      case StatusEnum.SUCCESS => {
        logger.info("[requestShuffle] Shuffle order fire")
      }
      case StatusEnum.FAIL => {
        logger.info("[requestShuffle] Shuffle failed.")
        throw new Exception
      }
      case _ => {
        /* Wait 5 seconds and retry */
        Thread.sleep(5 * 1000)
        requestShuffle
      }
    }
  }

  def shuffle(): Unit = {
    logger.info("[Shuffle] Shuffle start")
    shuffleServerHandler.shuffle(workersIP)
    logger.info("[Shuffle] Shuffle done")
  }


  def merge(): Unit = {
    logger.info("[Merge] Merge start")
    // shuffleServerHandler.shuffle(workersIP)
    logger.info("[Merge] Merge done")
  }
}