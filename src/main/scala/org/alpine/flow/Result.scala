package org.alpine.flow

import org.apache.hadoop.fs.Path
import scala.reflect.Manifest
import java.io.File

/** Input and output type (class?) for Alpine operators */
abstract class Result extends Serializable

case class ResultTuple2[A <: Result, B <: Result](
  override val _1: A,
  override val _2: B) extends Result with Product2[A, B]

case class ResultTuple3[A <: Result, B <: Result, C <: Result](
  override val _1: A,
  override val _2: B,
  override val _3: C) extends Result with Product3[A, B, C]

object Result {

  implicit def tuple2[A <: Result, B <: Result](input: (A, B)): ResultTuple2[A, B] =
    ResultTuple2(input._1, input._2)

  implicit def tuple3[A <: Result, B <: Result, C <: Result](input: (A, B, C)): ResultTuple3[A, B, C] =
    ResultTuple3(input._1, input._2, input._3)

}

/** node type for workflow */
sealed abstract class Node[+O <: Result: Manifest] {
  def id: String
  def nodeType: Manifest[_ <: O]
}

case class DataNode[+O <: Result: Manifest](override val id: String, data: O) extends Node[O] {
  override val nodeType: Manifest[_ <: O] = manifest[O]
}

/** Node type for non-leaves in workflow */

sealed abstract class OperatorNode[C <: OpConf, -I <: Result: Manifest, +O <: Result: Manifest] extends Node[O] {
  def operator: Operator[C, I, O]
  def config: C
}

case class NoConf[-I <: Result: Manifest, +O <: Result: Manifest](
    override val id: String,
    override val operator: Operator[NoOpConf, I, O]) extends OperatorNode[NoOpConf, I, O] {

  override val config = NoOpConf.instance
  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

case class Common[C <: OpConf, -I <: Result: Manifest, +O <: Result: Manifest](
    override val id: String,
    config: C,
    override val operator: Operator[C, I, O]) extends OperatorNode[C, I, O] {

  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

/** A result from an operator that is stored in memory. I.e. not a dataset or a model, more like a UI chart. */
final case class WrapResult[T](get: T) extends Result

object WrapResult {
  implicit def final2T[T](x: WrapResult[T]): T = x.get
}

/** Trait to describe a result produced by an operator that is itself a dataset */
sealed trait AlpData extends Result

case class LocalData(path: File) extends AlpData
