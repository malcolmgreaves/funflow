package org.alpine.flow

import _root_.io.netty.util.internal.logging.{ Slf4JLoggerFactory, InternalLoggerFactory }
import scala.reflect.{ Manifest, ClassTag }
import org.apache.spark.{ SparkConf, SparkContext }

object LocalData2RDD {

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  @transient private var _sc: SparkContext = null

  def sc: SparkContext = {

    if (_sc == null) {
      SparkUtil.silenceSpark()
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    } else {
      _sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }

    _sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("LocalData2RDD")
        .set("spark.driver.allowMultipleContexts", "true")
    )
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    _sc
  }

  private object SparkUtil {
    def silenceSpark() = {
      setLogLevels(Level.FATAL, Seq("spark", "org.eclipse.jetty", "akka", "log4j"))
    }

    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
      loggers.map {
        loggerName =>
          val logger = Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }
  }
}

class LocalData2RDD extends OperatorNoConf[LocalData, RDDResult[String]] {

  import LocalData2RDD.sc

  protected def applyActual(input: LocalData): RDDResult[String] =
    RDDResult(sc.textFile(input.path.getCanonicalPath))
}

class String2TokensOp extends OperatorNoConf[RDDResult[String], RDDResult[List[String]]] {

  override def applyActual(in: RDDResult[String]): RDDResult[List[String]] =
    RDDResult(in.d.map(_.split(" ").toList))
}

class Tokens2DictOp extends OperatorNoConf[RDDResult[List[String]], WrapResult[Map[String, Int]]] {

  override def applyActual(input: RDDResult[List[String]]): WrapResult[Map[String, Int]] =
    WrapResult(
      input.d.aggregate(Map.empty[String, Int])(
        (m, tokens) =>
          tokens.foldLeft(m)(
            (mm, tok) =>
              mm.get(tok) match {
                case Some(count) => (mm - tok) + (tok -> (count + 1))
                case None        => mm + (tok -> 1)
              }
          ),
        (m1, m2) =>
          m1.foldLeft(m2)({
            case (m, (tok, count)) =>
              m.get(tok) match {
                case Some(pcount) => (m - tok) + (tok -> (pcount + count))
                case None         => m + (tok -> count)
              }
          })
      )
    )
}

abstract class JoinOp[A: ClassTag: Manifest, B: ClassTag: Manifest, C: ClassTag: Manifest]
    extends OperatorNoConf[ResultTuple2[RDDResult[A], RDDResult[B]], RDDResult[C]] {

  override final def applyActual(input: ResultTuple2[RDDResult[A], RDDResult[B]]): RDDResult[C] =
    input._1.d.zip(input._2.d).flatMap(join)

  def join(input: (A, B)): Option[C]
}

class JoinStrEq extends JoinOp[String, String, String] {
  def join(input: (String, String)): Option[String] =
    if (input._1 == input._2)
      Some(input._1)
    else
      None
}
