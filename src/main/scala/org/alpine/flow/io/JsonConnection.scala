package org.alpine.flow.io

import org.alpine.flow._
import scala.util.{ Success, Failure, Try }
import org.alpine.flow.DataNode

case class JsonConnection(nodes: Seq[String], dest: String)

object JsonConnection {

  def toConnection(nodeMap: Map[String, Node[_ <: Result]], jc: JsonConnection): Try[Connection[_ <: OpConf, _ <: Result, _ <: Result]] = Try({

    if (jc.nodes.size == 0 || nodeMap.size == 0) {
      throw new IllegalArgumentException("Connection and node map must be populated with at least two nodes.")
    }

    val parentNodes: Seq[Node[_ <: Result]] = {

      val missingNodes = jc.nodes
        .filter(nodename => !nodeMap.contains(nodename))
        .foldLeft(Seq.empty[String])((a, nodeName) => a :+ nodeName)

      if (!missingNodes.isEmpty) {
        val x = missingNodes.mkString(",")
        throw new IllegalArgumentException(s"missing ${missingNodes.size} parent nodes from the Connection (not in node map): $x")

      } else {
        jc.nodes.map(nodeName => nodeMap(nodeName))
      }
    }

    nodeMap.get(jc.dest) match {

      case Some(destination) =>

        destination match {

          case opNode: OperatorNode[_, _, _] =>

            val connAttempt = opNode.operator.inputClass match {

              case tuple3 if tuple3 <:< manifest[ResultTuple3[Result, Result, Result]] =>
                Connection.forThree(
                  parentNodes,
                  opNode.asInstanceOf[OperatorNode[OpConf, ResultTuple3[Result, Result, Result], Result]]
                )

              case tuple2 if tuple2 <:< manifest[ResultTuple2[Result, Result]] =>
                Connection.forTwo(
                  parentNodes,
                  opNode.asInstanceOf[OperatorNode[OpConf, ResultTuple2[Result, Result], Result]]
                )

              case _ =>
                if (parentNodes.size == 1)
                  Connection.forOne(
                    parentNodes(0).asInstanceOf[Node[Result]],
                    opNode.asInstanceOf[OperatorNode[OpConf, Result, Result]]
                  )
                else
                  throw new IllegalArgumentException(s"Cannot handle operators with input arity > 3: ${opNode.operator.inputClass}")
            }

            connAttempt match {

              case Success(conn) =>
                conn.asInstanceOf[Connection[_ <: OpConf, _ <: Result, _ <: Result]]

              case Failure(e) =>
                throw e
            }

          case DataNode(_, _) =>
            throw new IllegalArgumentException(s"Connection cannot have Data node as destination! destination: ${jc.dest}")
        }

      case None =>
        throw new IllegalArgumentException(s"Connection's destination (${jc.dest}) is not in the node map")
    }
  })

}
