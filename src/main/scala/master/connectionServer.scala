package master

import java.util.logging.Logger
import java.net.InetAddress

import io.grpc.{Server, ServerBuilder}
import connection.message.{ConnectionGrpc, ConnectionRequestMsg, ConnectionDoneMsg}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ListBuffer

class ConnectionServer(executionContext: ExecutionContext, numWorkers: Int, port: Int) { self =>
  private[this] var server: Server = null
  private var workerListBuffer: ListBuffer[String] = new ListBuffer[String]()
  private var workerList: List[String] = null
  private val logger = Logger.getLogger(classOf[ConnectionServer].getName)

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
        workerList = workerListBuffer.toList.sorted
        System.out.println(workerList)
        logger.info("all workers connected")
      }
      
      Future.successful(new ConnectionDoneMsg(isConnected = true, workerId = workerListBuffer.size - 1))
    }
  }

}
