package org.alpine.flow

import scala.reflect.Manifest

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

trait OpConf

sealed trait NoOpConf extends OpConf

object NoOpConf extends OpConf {
  val instance: NoOpConf = new NoOpConf {}
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
