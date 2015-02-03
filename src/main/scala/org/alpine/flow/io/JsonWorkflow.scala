package org.alpine.flow.io

import rapture.json._
import jsonBackends.jackson._
import org.alpine.flow._
import scala.util.{ Success, Failure, Try }

case class JsonWorkflow(flowName: String, nodes: Seq[JsonNode], connections: Seq[JsonConnection])

object JsonWorkflow {

  type JSON = String
  type Name = String

  def fromWorkflow(name: Name, wf: Workflow): Try[Json] = Try({

    //    val jsonNodeList = wf.foldLeft(List.empty[JsonNode])(....)
    //
    //    val connections = wf.leaves.foldLeft(List.empty[JsonConnection])(
    //      (jconn, leaf) => {
    //        val jc = (leaf + wf.predecessorsOf(leaf))
    //        jconn :+ jc
    //
    //        recursively, for each *node in wf.predecessorsOf(leaf)
    //          make a new connection with *node and wf.predecessorsOf(*node)
    //          recurse
    //      }
    //    )
    //
    //    Json.apply(JsonWorkflow(name, jsonNodeList, jsonConnections))
    ???
  })

  def toWorkflow(input: JSON): Try[(Name, Workflow)] = Try({

    // parse the JSON, obtain a JsonWorkflow instance
    val w = Json.parse(input).as[JsonWorkflow]

    // for each JsonNode class (thinly wrapped JSON describing some kind of node),
    // we will parse the JSON inside of the JsonNode
    // From these JsonNodes, we'll construct a mapping from the node's name to the instantiated node object
    val nodeMap = {
      // we collect all parsing & instantiation errors while constructing the map
      val (nm, errors) =
        w.nodes
          .map(jNode => JsonNode.convert(jNode))
          .foldLeft((Map.empty[String, Node[_ <: Result]], List.empty[Throwable]))({
            case ((m, err), conversionAttempt) =>
              conversionAttempt match {

                case Success((name, jnodeKind)) =>
                  if (m.contains(name))
                    (
                      m,
                      err :+ new IllegalArgumentException(s"Nodes in a Workflow cannot have the same name. Found duplicate for $name")
                    )
                  else
                    JsonNodeKind.toNode(name, jnodeKind) match {

                      case Success(realNode) =>
                        (m + (name -> realNode), err)

                      case Failure(e) =>
                        (m, err :+ e)
                    }

                case Failure(e) =>
                  (m, err :+ e)
              }
          })

      if (!errors.isEmpty) {
        val e = errors.mkString("\n")
        throw new IllegalArgumentException(s"${errors.size} errors in parsing and conversion JSON nodes:\n$e")
      }
      nm
    }

    // Parse the JSON for each workflow connection and instantiate into a Connection object.
    // Collect all of these into a Set.
    val connections = {
      // we collect all parsing & instantiation errors while constructing the set
      val (conns, errors) = w.connections
        .map(jConn => JsonConnection.toConnection(nodeMap, jConn))
        .foldLeft((Set.empty[Connection[_ <: OpConf, _ <: Result, _ <: Result]], List.empty[Throwable]))({
          case ((connSet, errs), connAttempt) =>
            connAttempt match {

              case Success(connection) =>
                (connSet + connection, errs)

              case Failure(e) =>
                (connSet, errs :+ e)
            }
        })

      if (!errors.isEmpty) {
        val e = errors.mkString("\n")
        throw new IllegalArgumentException(s"Found ${errors.size} errors in parsing JSON connections:\n$e")
      }
      conns
    }

    // Instantiate the workflow with this Node and Connection information
    (
      w.flowName,
      Workflow.create(
        nodeMap.foldLeft(Set.empty[Node[_ <: Result]])({
          case (nodeSet, (_, node)) => nodeSet + node
        }),
        connections
      ).get
    )
  })
}
