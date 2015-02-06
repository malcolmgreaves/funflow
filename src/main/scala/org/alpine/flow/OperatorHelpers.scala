package org.alpine.flow

import scala.reflect.Manifest

final class Tuple2Projection1Op[A <: Result: Manifest] extends OperatorNoConf[ResultTuple2[A, _], A] {
  protected def applyActual(input: ResultTuple2[A, _]): A = input._1
}

final class Tuple2Projection2Op[B <: Result: Manifest] extends OperatorNoConf[ResultTuple2[_, B], B] {
  protected def applyActual(input: ResultTuple2[_, B]): B = input._2
}

final class Tuple3Projection1Op[A <: Result: Manifest] extends OperatorNoConf[ResultTuple3[A, _, _], A] {
  protected def applyActual(input: ResultTuple3[A, _, _]): A = input._1
}

final class Tuple3Projection2Op[B <: Result: Manifest] extends OperatorNoConf[ResultTuple3[_, B, _], B] {
  protected def applyActual(input: ResultTuple3[_, B, _]): B = input._2
}

final class Tuple3Projection3Op[C <: Result: Manifest] extends OperatorNoConf[ResultTuple3[_, _, C], C] {
  protected def applyActual(input: ResultTuple3[_, _, C]): C = input._3
}
