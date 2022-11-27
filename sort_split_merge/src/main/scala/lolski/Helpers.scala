package lolski

import scala.concurrent.{ExecutionContext, Future}

object Helpers {
  object Tests {
    // comparison helpers
    def isAscOrdered(in: Iterator[String]): Boolean = {
      val sliding = in.sliding(2, 1)
      val sorted = sliding forall { case Seq(prev, cur) => prev.toInt < cur.toInt }
      sorted
    }

    def isAscIncrement(in: Iterator[String]): Boolean = {
      val sliding = in.sliding(2, 1)
      val sorted = sliding forall { case Seq(prev, cur) => prev.toInt == (cur.toInt - 1) }
      sorted
    }
  }

  object Timer {
    def elapsed[T](block: => T): (T, Long) = {
      val t1 = System.currentTimeMillis()
      val res = block
      val t2 = System.currentTimeMillis()
      (res, t2 - t1)
    }

    def elapsedAsync[T](block: => Future[T])(implicit ec: ExecutionContext): Future[(T, Long)] = {
      val t1 = System.currentTimeMillis()
      block map { res =>
        val t2 = System.currentTimeMillis()
        (res, t2 - t1)
      }
    }
  }

}
