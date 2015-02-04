package org.alpine.flow

import _root_.io.netty.util.internal.logging.{ Slf4JLoggerFactory, InternalLoggerFactory }
import org.apache.spark.rdd._

import scala.reflect.ClassTag
import org.apache.spark.{ HashPartitioner, SparkConf, SparkContext }
import org.apache.log4j.{ Logger, Level }
import java.util.Random
import org.alpine.flow.LocalData
import scala.Some
import org.alpine.flow.ResultTuple2
import org.alpine.flow.ResultTuple3
import org.alpine.flow.RDDResult
import org.apache.spark.util.Utils

trait OpConf

sealed trait NoOpConf extends OpConf

object NoOpConf {
  val instance: NoOpConf = new NoOpConf {}
}

/**
 * Abstract class describing alpine operators as functions from Result to Result
 * Must be abstract instead of a trait so that we can use ClassTag
 */
abstract class Operator[C <: OpConf: Manifest, -I <: Result: Manifest, +O <: Result: Manifest] extends ((C, I) => O) {

  /** Performs the operation on some input of type I, evaluating to an output of type O. */
  def apply(config: C, input: I): O

  /* A Manifest for the generic configuration of this operator. */
  final val configClass: Manifest[C] = manifest[C]

  /* A Manifest for the generic input of this operator. Used to compare I/O types at runtime. */
  final val inputClass: Manifest[_ >: I] = manifest[I]

  /* A Manifest for the generic output of this operator. Used to compare I/O types at runtime. */
  final val outputClass: Manifest[_ <: O] = manifest[O]
}

abstract class OperatorNoConf[-I <: Result: Manifest, +O <: Result: Manifest] extends Operator[NoOpConf, I, O] {
  final def apply(ignored: NoOpConf, input: I): O = applyActual(input)

  protected def applyActual(input: I): O
}

/** Companion to the Operator trait. Contains implicit conversion from function to Operator */
object Operator {

  implicit def fn2op[C <: OpConf: Manifest, I <: Result: Manifest, O <: Result: Manifest](
    fn: (C, I) => O): Operator[C, I, O] =

    new Operator[C, I, O] {
      override def apply(config: C, input: I): O =
        fn(config, input)
    }

  implicit def fn2opNoConf[I <: Result: Manifest, O <: Result: Manifest](fn: I => O): OperatorNoConf[I, O] =
    new OperatorNoConf[I, O] {
      override protected def applyActual(input: I): O =
        fn(input)
    }

  implicit def fnTuple2[C <: OpConf: Manifest, A <: Result: Manifest, B <: Result: Manifest, Z <: Result: Manifest](
    fn: (C, A, B) => Z): Operator[C, ResultTuple2[A, B], Z] =

    new Operator[C, ResultTuple2[A, B], Z] {
      override def apply(config: C, input: ResultTuple2[A, B]): Z =
        fn(config, input._1, input._2)
    }

  implicit def fnTuple3[CO <: OpConf: Manifest, A <: Result: Manifest, B <: Result: Manifest, C <: Result: Manifest, Z <: Result: Manifest](
    fn: (CO, A, B, C) => Z): Operator[CO, ResultTuple3[A, B, C], Z] =

    new Operator[CO, ResultTuple3[A, B, C], Z] {
      override def apply(config: CO, input: ResultTuple3[A, B, C]): Z =
        fn(config, input._1, input._2, input._3)
    }

}

object LocalData2RDD {

  @transient private var _sc: SparkContext = null

  //  var sc:SparkContext = null

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

    _sc = new SparkContext(new SparkConf().setMaster("local").setAppName("LocalData2RDD"))

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
    RDDResult(input._1.d.zip(input._2.d).flatMap(join))

  def join(input: (A, B)): Option[C]
}

class JoinStrEq extends JoinOp[String, String, String] {
  def join(input: (String, String)): Option[String] =
    if (input._1 == input._2)
      Some(input._1)
    else
      None
}
