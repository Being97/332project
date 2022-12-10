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

class ConnectionServer(executionContext: ExecutionContext, numWorkers: Int, port: Int) { self =>
  private[this] var server: Server = null
  private var workerListBuffer: ListBuffer[String] = new ListBuffer[String]()
  private var workerList: List[String] = null
  private val logger = Logger.getLogger(classOf[ConnectionServer].getName)

  val outputDir = System.getProperty("user.dir") + "/src/main/resources/master"

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

  private class ConnectionImpl extends ConnectionGrpc.Connection {
    override def initConnect(req: ConnectionRequestMsg) = {
      System.out.println("Client IP : " + req.workerIP + " Connected")
      workerListBuffer += req.workerIP
      
      if (workerListBuffer.size == numWorkers) {
        workerList = workerListBuffer.toList
        System.out.println(workerList)
        logger.info("all workers connected")
      }
      
      Future.successful(new ConnectionDoneMsg(isConnected = true, workerId = workerListBuffer.size - 1))
    }

    override def sample(responseObserver: StreamObserver[SampleDone]): StreamObserver[SampleTransfer] = {
    
      logger.info("[sample]: Worker tries to send sample")
      new StreamObserver[SampleTransfer] {
        var fos: FileOutputStream = null

        override def onNext(request: SampleTransfer): Unit = {

          System.out.println(request.sampledData)
          
          if (fos == null) {
              val file = new File("sample1")
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
        }
      }
    }
  }

}
