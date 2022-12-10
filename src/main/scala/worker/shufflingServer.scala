package worker

import message.shuffle.StatusEnum
import message.shuffle.{ShuffleGrpc, ShuffleRequest, ShuffleDone}
import common._

import java.util.logging.Logger
import java.util.concurrent.TimeUnit

import io.grpc.{Server, ServerBuilder, Status}
import io.grpc.stub.StreamObserver;

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class ShufflingServer(executionContext: ExecutionContext, port: Int, id: Int, tempDir: String) { self =>
  var server: Server = null
  val logger: Logger = Logger.getLogger(classOf[ShufflingServer].getName)

  def start(): Unit = {
    server = ServerBuilder.forPort(port)
        .addService(ShuffleGrpc.bindService(new ShuffleImpl, executionContext))
        .build
        .start
    logger.info("[ShufflingServer] started, listening on " + port)
    sys.addShutdownHook {
      System.err.println("Shutting down ShufflingServer since JVM is shutting down")
      self.stop()
      System.err.println("File server shut down")
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
            val file = new File(tempDir + s"shuffle-$workerID-$partitionID")
            fos = new FileOutputStream(file)
          }

          request.data.writeTo(fos)
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
        }
      }
    }
  }
}
