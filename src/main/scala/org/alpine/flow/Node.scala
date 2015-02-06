package org.alpine.flow

import scala.reflect.Manifest

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