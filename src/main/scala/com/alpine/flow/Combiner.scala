package org.alpine.flow

import scala.util.Try

import scala.reflect.Manifest

sealed abstract class Connection[C <: OpConf: Manifest, -A <: AlpResult: Manifest, +B <: AlpResult: Manifest] {
  /* exactly the number required for destination's input */
  def nodes: Seq[AlpNode[_ <: AlpResult]]

  def replaceParent(oldParent: AlpNode[_ <: AlpResult], newParent: AlpNode[_ <: AlpResult]): Try[Connection[C, A, B]] = Try(

    if (nodes.map(_.id).contains(oldParent.id) && Util.equalManifests(oldParent.nodeType, newParent.nodeType)) {

      val newParents =
        nodes.map(parent =>
          if (parent.id == oldParent.id)
            newParent
          else
            parent
        )
      val sameDestination = destination

      new Connection[C, A, B] {
        override val nodes = newParents
        override val destination = sameDestination
      }

    } else {
      throw new IllegalArgumentException(s"parent replacement must have same type")
    }
  )

  def destination: NodeOperator[C, A, B]

  final def replaceDestination(newDestination: NodeOperator[OpConf, AlpResult, AlpResult]): Try[Connection[C, A, B]] = Try(

    if (Util.equalManifests(newDestination.operator.inputClass, destination.operator.inputClass)
      && Util.equalManifests(newDestination.operator.configClass, destination.operator.configClass)
      && Util.equalManifests(newDestination.operator.outputClass, destination.operator.outputClass)) {

      val newDest = newDestination.asInstanceOf[NodeOperator[C, A, B]]
      val sameNodes = nodes

      new Connection[C, A, B] {
        override val nodes = sameNodes
        override val destination = newDest
      }

    } else {
      throw new IllegalArgumentException(s"destination must have same input and output types")
    }
  )

}

object Connection {

  import Util._

  private def error(n: Any, i: Any): Exception =
    new IllegalArgumentException(s"incorrect Node type for Operator's input.\tNode(s) Type: $n\tOperator Input: $i")

  def forOne[C <: OpConf: Manifest, A <: AlpResult: Manifest, Z <: AlpResult: Manifest](
    node: AlpNode[A],
    destFunc: NodeOperator[C, A, Z]): Try[Connection[C, A, Z]] = Try(

    if (equalManifests(destFunc.operator.inputClass, node.nodeType)) {
      new Connection[C, A, Z] {
        override val nodes = Seq(node)
        override val destination = destFunc
      }
    } else {
      throw error(s"(${node.id})  ${node.nodeType}", s"(${destFunc.id}) ${destFunc.operator.inputClass}")
    }
  )

  def forTwo[C <: OpConf: Manifest, A <: AlpResult: Manifest, B <: AlpResult: Manifest, Z <: AlpResult: Manifest](
    nodez: Seq[AlpNode[_ <: AlpResult]],
    destFunc: NodeOperator[C, AlpResultTuple2[A, B], Z]): Try[Connection[C, AlpResultTuple2[A, B], Z]] = Try(

    if (nodez.size == 2
      && equalManifests(nodez(0).nodeType, manifest[A])
      && equalManifests(nodez(1).nodeType, manifest[B])) {
      new Connection[C, AlpResultTuple2[A, B], Z] {
        override val nodes = nodez
        override val destination = destFunc
      }
    } else {
      throw error(nodez.map(n =>
        s"(${n.id}) ${n.nodeType}").mkString(","), s"(${destFunc.id}) ${destFunc.operator.inputClass}")
    }
  )

  def forThree[CO <: OpConf: Manifest, A <: AlpResult: Manifest, B <: AlpResult: Manifest, C <: AlpResult: Manifest, Z <: AlpResult: Manifest](
    nodez: Seq[AlpNode[_ <: AlpResult]],
    destFunc: NodeOperator[CO, AlpResultTuple3[A, B, C], Z]): Try[Connection[CO, AlpResultTuple3[A, B, C], Z]] = Try(

    if (nodez.size == 3
      && equalManifests(nodez(0).nodeType, manifest[A])
      && equalManifests(nodez(1).nodeType, manifest[B])
      && equalManifests(nodez(2).nodeType, manifest[C])) {
      new Connection[CO, AlpResultTuple3[A, B, C], Z] {
        override val nodes = nodez
        override val destination = destFunc
      }
    } else {
      throw error(nodez.map(n =>
        s"(${n.id}) ${n.nodeType}").mkString(","), s"(${destFunc.id}) ${destFunc.operator.inputClass}")
    }
  )

  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
  //
  //  implicit def two[A,B,Z](nodez:(A,B), destFunc:Common[(A,B), Z]):Connection[(A,B), Z] =
  //    new Connection[(A,B), Z] {
  //      override val nodes = nodez
  //      override val destination = destFunc
  //    }
}
