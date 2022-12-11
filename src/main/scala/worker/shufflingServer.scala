package worker

import message.shuffle.StatusEnum
import message.shuffle.{ShuffleGrpc, ShuffleRequest, ShuffleDone}
import WorkerTool._

import java.util.logging.Logger
import java.util.concurrent.TimeUnit

import io.grpc.{Server, ServerBuilder, Status}
import io.grpc.stub.StreamObserver;

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import java.io.{OutputStream, FileOutputStream, File}

class ShufflingServer(executionContext: ExecutionContext, port: Int, id: Int, shuffledDir: String, numWorkers: Int) { self =>
  var server: Server = null
  val logger: Logger = Logger.getLogger(classOf[ShufflingServer].getName)
  var receivedWorkerCount: Int = 0
  var state: WORKERSERVERSTATE = WORKERSERVERREADY

  def start(): Unit = {
    server = ServerBuilder.forPort(port)
        .addService(ShuffleGrpc.bindService(new ShuffleImpl, executionContext))
        .build
        .start
    logger.info("[ShufflingServer] started, listening on " + port)
    sys.addShutdownHook {
      System.err.println("Shutting down ShufflingServer since JVM is shutting down")
      self.stop()
      System.err.println("ShufflingServer shut down")
    }
  }

  def tryMerge(): Unit = {
    if(receivedWorkerCount == numWorkers){
      logger.info(s"[tryMerge]: All workers sent partition. Start Merge.")
      state = ALLPARTRECEIVED
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

  class ShuffleImpl() extends ShuffleGrpc.Shuffle {
    override def shuffle(responseObserver: StreamObserver[ShuffleDone]): StreamObserver[ShuffleRequest] = {
      new StreamObserver[ShuffleRequest] {
        var fos: FileOutputStream = null
        var workerID: Int = -1
        var partitionID: Int = -1

        override def onNext(request: ShuffleRequest): Unit = {
          workerID = request.workerID
          partitionID = request.partitionID
          if (fos == null) {
            logger.info(s"[ShufflingServer]: receiving shuffleData-$workerID-$partitionID")
            val file = new File(shuffledDir + s"/shuffle-$workerID-$partitionID")
            fos = new FileOutputStream(file)
          }

          request.shuffleData.writeTo(fos)
          fos.flush()
        }

        override def onError(t: Throwable): Unit = {
          logger.warning(s"[ShufflingServer]: Worker $workerID failed to send partition $partitionID: ${Status.fromThrowable(t)}")
          throw t
        }

        override def onCompleted(): Unit = {
          logger.info(s"[ShufflingServer]: Worker $workerID done sending partition $partitionID")

          fos.close()
          responseObserver.onNext(new ShuffleDone(StatusEnum.SUCCESS))
          responseObserver.onCompleted

          receivedWorkerCount += 1
          tryMerge
        }
      }
    }
  }
}
