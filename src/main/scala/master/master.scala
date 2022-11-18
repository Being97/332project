package master

import java.util.logging.Logger
import java.net.InetAddress

import io.grpc.{Server, ServerBuilder}
import connection.message.{ConnectionGrpc, ConnectionRequestMsg, ConnectionDoneMsg}

import scala.concurrent.{ExecutionContext, Future}

object HelloWorldServer {
  private val logger = Logger.getLogger(classOf[HelloWorldServer].getName)

  def main(args: Array[String]): Unit = {
    val server = new HelloWorldServer(ExecutionContext.global)
    System.out.println(InetAddress.getLocalHost.getHostAddress)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class HelloWorldServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(HelloWorldServer.port).addService(ConnectionGrpc.bindService(new ConnectionImpl, executionContext)).build.start
    HelloWorldServer.logger.info("Server started, listening on " + HelloWorldServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class ConnectionImpl extends ConnectionGrpc.Connection {
    override def sayHello(req: ConnectionRequestMsg) = {
      val reply = ConnectionDoneMsg(isConnected = true)
      System.out.println("Client IP : " + req.workerIP + " Connected")
      Future.successful(reply)
    }
  }

}

