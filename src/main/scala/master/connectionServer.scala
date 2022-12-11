package master

import java.util.logging.Logger
import java.net.InetAddress

import io.grpc.{Server, ServerBuilder}
import io.grpc.stub.StreamObserver
import connection.message._
import io.grpc.Status
import java.io.{OutputStream, FileOutputStream, File}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ListBuffer
import WorkerTool._

class ConnectionServer(executionContext: ExecutionContext, numWorkers: Int, port: Int) { self =>
  private[this] var server: Server = null
  private var workerListBuffer: ListBuffer[String] = new ListBuffer[String]()
  private var workerList: List[String] = null
  private val logger = Logger.getLogger(classOf[ConnectionServer].getName)
  
  val outputDir = System.getProperty("user.dir") + "/src/main/resources/master"
  var state: MASTERSTATE = MASTERREADY
  var sampledWorkerCount: Int = 0
  var partitionedWorkerCount: Int = 0
  var shuffledWorkerCount: Int = 0
  var pivotList: List[String] = null

  def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(ConnectionGrpc.bindService(new ConnectionImpl, executionContext)).build.start
    logger.info("Server started, listening on " + port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  def tryPivot(): Unit = {
    if (sampledWorkerCount == numWorkers) {
      logger.info(s"[tryPivot]: All workers sent sample. Start pivot.")
      pivotList = WorkerTool.makeKeyTool_master(workerList.length, outputDir)
      
      state = PIVOTED
    }
  }
  
  private class ConnectionImpl extends ConnectionGrpc.Connection {
    override def initConnect(req: ConnectionRequestMsg) = {
      System.out.println("Client IP : " + req.workerIP + " Connected")
      workerListBuffer += req.workerIP
      
      if (workerListBuffer.size == numWorkers) {
        workerList = workerListBuffer.toList
        System.out.println(workerList)
        logger.info("all workers connected")
        state = CONNECTED
      }
      
      Future.successful(new ConnectionDoneMsg(isConnected = true, workerId = workerListBuffer.size - 1))
    }

    override def sample(responseObserver: StreamObserver[SampleDone]): StreamObserver[SampleTransfer] = {
      logger.info("[sample]: Worker tries to send sample")
      new StreamObserver[SampleTransfer] {
        var fos: FileOutputStream = null

        override def onNext(request: SampleTransfer): Unit = {
          if (fos == null) {
              val file = new File(outputDir + "/sample" + request.workerId)
              fos = new FileOutputStream(file)
          }

          request.sampledData.writeTo(fos)
          fos.flush()
        }

        override def onError(t: Throwable): Unit = {
          logger.warning("[sample]: Worker failed to send sample")
          throw t
        }

        override def onCompleted(): Unit = {
          logger.info("[sample]: Worker done sending sample")

          fos.close()
          responseObserver.onNext(new SampleDone(successed = true))
          responseObserver.onCompleted

          sampledWorkerCount += 1
          tryPivot
          logger.info("[tryPivot]: Master done sending pivot")
        }
      }
    }

    override def pivot(request: PivotRequest): Future[PivotDone] = state match {
      case PIVOTED => {
        Future.successful(new PivotDone(
          status = StatusEnum.SUCCESS,
          workerIPList = workerList.toSeq,
          pivotsList = pivotList
        ))
      }
      case FAILED => {
        Future.failed(new Exception)
      }
      case _ => {
        Future.successful(new PivotDone(status = StatusEnum.PROGRESS))
      }
    }

    override def partitioned(req: PartitionedRequest) = {
      logger.info("Worker " + req.workerID + " is Partitioned")
      partitionedWorkerCount += 1
      
      if (partitionedWorkerCount == numWorkers) {
        logger.info("all workers Partitioned")
        state = PARTITIONED
      }
      
      Future.successful(new PartitionedDone(isOk = true))
    }  

    override def shuffleTry(request: ShuffleTryRequest): Future[ShuffleTryDone] = state match {
      case PARTITIONED => {
        Future.successful(new ShuffleTryDone(
          status = StatusEnum.SUCCESS,
        ))
      }
      case FAILED => {
        Future.failed(new Exception)
      }
      case _ => {
        Future.successful(new ShuffleTryDone(status = StatusEnum.PROGRESS))
      }
    }

    override def shuffled(req: ShuffledRequest) = {
      logger.info("Worker " + req.workerID + " is shuffled")
      shuffledWorkerCount += 1
      
      if (shuffledWorkerCount == numWorkers) {
        logger.info("all workers shuffled")
        state = SHUFFLED
      }
      
      Future.successful(new ShuffledDone(isOk = true))
    }  

    override def mergeTry(request: MergeTryRequest): Future[MergeTryDone] = state match {
      case SHUFFLED => {
        Future.successful(new MergeTryDone(
          status = StatusEnum.SUCCESS,
        ))
      }
      case FAILED => {
        Future.failed(new Exception)
      }
      case _ => {
        Future.successful(new MergeTryDone(status = StatusEnum.PROGRESS))
      }
    }
  }
  
}
