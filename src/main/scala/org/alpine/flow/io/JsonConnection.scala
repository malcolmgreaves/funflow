package org.alpine.flow.io

import rapture.json._
import jsonBackends.jackson._
import org.alpine.flow._
import scala.util.{ Success, Failure, Try }
import org.alpine.flow.Data
import java.io.File

import scala.reflect.Manifest

case class JsonNode(name: String, kind: String, nodeInfo: Json)

object JsonNode {

  val configurableOperator = "configurableOperator"
  val pureOperator = "pureOperator"
  val data = "data"

  type Name = String
  type JSON = String

  def toNode(s: JSON): Try[AlpNode[_ <: AlpResult]] =
    parse(s).flatMap({
      case (name, jNode) => JsonNodeKind.toNode(name, jNode)
    })

  def toNodeMap(strs: Seq[JSON]): Try[Map[String, AlpNode[_ <: AlpResult]]] = Try(
    strs.map(s =>
      toNode(s) match {
        case Success(node) =>
          node
        case Failure(e) =>
          throw e
      }
    ).foldLeft(Map.empty[String, AlpNode[_ <: AlpResult]])(
      (m, node) =>
        if (m.contains(node.id)) {
          throw new IllegalArgumentException(s"Cannot have duplicate nodes in the map. Duplicate node name/id: ${node.id}")
        } else {
          m + (node.id -> node)
        }
    )
  )

  def toNodeMap(jsonNodeList: JSON): Try[Map[String, AlpNode[_ <: AlpResult]]] =
    Try(Json.parse(jsonNodeList).as[List[String]]).flatMap(toNodeMap)

  def parse(s: JSON): Try[(Name, JsonNodeKind)] =
    convert(Json.parse(s).as[JsonNode])

  def convert(jn: JsonNode): Try[(Name, JsonNodeKind)] =
    Try((
      jn.name,

      if (jn.kind == configurableOperator)
        jn.nodeInfo.as[JsonOpConfNode]

      else if (jn.kind == pureOperator)
        jn.nodeInfo.as[JsonOpNoConfNode]

      else if (jn.kind == data)
        jn.nodeInfo.as[JsonDataNode]

      else
        throw new IllegalArgumentException(s"unrecognized node kind: ${jn.kind}")
    ))
}

/* Either an operator or a data source */

sealed abstract class JsonNodeKind

case class JsonDataNode(dataType: String, dataLoc: String, special: Option[String]) extends JsonNodeKind

case class JsonOpConfNode(operator: String, confMaker: String, configuration: String) extends JsonNodeKind

case class JsonOpNoConfNode(operator: String) extends JsonNodeKind

object JsonNodeKind {

  type Name = String

  def toNode(name: Name, jn: JsonNodeKind): Try[AlpNode[_ <: AlpResult]] =
    jn match {
      case opConf: JsonOpConfNode     => JsonOpConfNode.toNode(name, opConf)
      case opNoConf: JsonOpNoConfNode => JsonOpNoConf.toNode(name, opNoConf)
      case data: JsonDataNode         => JsonDataNode.toNode(name, data)
    }
}

object JsonOpNoConf {

  type Name = String

  def toNode(name: Name, jn: JsonOpNoConfNode): Try[NoConf[_ <: AlpResult, _ <: AlpResult]] = Try({

    val operator = Class.forName(jn.operator).newInstance.asInstanceOf[AlpOperatorNoConf[AlpResult, AlpResult]]

    NoConf(
      name,
      operator.asInstanceOf[AlpOperatorNoConf[_ <: AlpResult, _ <: AlpResult]]
    )
  })

}

abstract class ConfMaker[C <: OpConf: Manifest] {
  type JSON = String
  def make(s: JSON): Try[C]
  final val configClass = manifest[C]
}

object JsonOpConfNode {

  import org.alpine.flow.Util._

  type Name = String

  def toNode(name: Name, jn: JsonOpConfNode): Try[Common[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]] = Try({

    val operator = Class.forName(jn.operator).newInstance.asInstanceOf[AlpOperator[OpConf, AlpResult, AlpResult]]

    if (equalManifests(operator.configClass, manifest[NoOpConf])) {
      throw new IllegalArgumentException(s"Expecting configurable operator!")
    }

    val confMaker = Class.forName(jn.confMaker).newInstance.asInstanceOf[ConfMaker[_ <: OpConf]]

    if (!equalManifests(operator.configClass, confMaker.configClass)) {
      throw new IllegalArgumentException(
        "Expecting configuration maker and operator's config classes to match, " +
          s"instead have operator's config: ${operator.configClass} and conf maker: ${confMaker.configClass}}"
      )
    }

    confMaker.make(jn.configuration) match {

      case Success(configuration) =>
        Common(
          name,
          configuration,
          operator.asInstanceOf[AlpOperator[OpConf, _ <: AlpResult, _ <: AlpResult]]
        ).asInstanceOf[Common[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]]

      case Failure(e) =>
        throw e
    }
  })

}

/*
        {
            "name" : "node1",
            "kind" : "data"
            "nodeInfo" : {
                "dataType" : "HDFS",
                "dataLoc" : "hdfs:///path/to/file",
            }
        }
 */

object JsonDataNode {

  val DB = "DB"
  val HDFS = "HDFS"
  val RDD = "RDD"
  val Local = "Local"

  type Name = String

  //case class Data[O <: AlpResult: Manifest](override val id: String, data: O) extends AlpNode[O]
  def toNode[D <: Data[_ <: AlpResult]](name: Name, jn: JsonDataNode): Try[Data[_ <: AlpResult]] = Try(
    jn.dataType match {
      case DB   => ???
      case HDFS => ???
      case RDD => jn.special match {
        case Some(format) => ???
        case None         => ???
      }
      case Local =>
        Data(name, LocalData(new File(jn.dataLoc)))

      case _ =>
        throw new IllegalArgumentException(s"Unknown Data Type for JsonDataNode: ${jn.dataType} @ location: ${jn.dataLoc}")
    }
  )

}

case class JsonConnection(nodes: Seq[String], dest: String)

object JsonConnection {

  def toConnection(nodeMap: Map[String, AlpNode[_ <: AlpResult]], jc: JsonConnection): Try[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]] = Try({

    if (jc.nodes.size == 0 || nodeMap.size == 0) {
      throw new IllegalArgumentException("Connection and node map must be populated with at least two nodes.")
    }

    val parentNodes: Seq[AlpNode[_ <: AlpResult]] = {

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

          case opNode: NodeOperator[_, _, _] =>

            val connAttempt = opNode.operator.inputClass match {

              case tuple3 if tuple3 <:< manifest[AlpResultTuple3[AlpResult, AlpResult, AlpResult]] =>
                Connection.forThree(
                  parentNodes,
                  opNode.asInstanceOf[NodeOperator[OpConf, AlpResultTuple3[AlpResult, AlpResult, AlpResult], AlpResult]]
                )

              case tuple2 if tuple2 <:< manifest[AlpResultTuple2[AlpResult, AlpResult]] =>
                Connection.forTwo(
                  parentNodes,
                  opNode.asInstanceOf[NodeOperator[OpConf, AlpResultTuple2[AlpResult, AlpResult], AlpResult]]
                )

              case _ =>
                if (parentNodes.size == 1)
                  Connection.forOne(
                    parentNodes(0).asInstanceOf[AlpNode[AlpResult]],
                    opNode.asInstanceOf[NodeOperator[OpConf, AlpResult, AlpResult]]
                  )
                else
                  throw new IllegalArgumentException(s"Cannot handle operators with input arity > 3: ${opNode.operator.inputClass}")
            }

            connAttempt match {

              case Success(conn) =>
                conn.asInstanceOf[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]]

              case Failure(e) =>
                throw e
            }

          case Data(_, _) =>
            throw new IllegalArgumentException(s"Connection cannot have Data node as destination! destination: ${jc.dest}")
        }

      case None =>
        throw new IllegalArgumentException(s"Connection's destination (${jc.dest}) is not in the node map")
    }
  })

}

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
          .foldLeft((Map.empty[String, AlpNode[_ <: AlpResult]], List.empty[Throwable]))({
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
        .foldLeft((Set.empty[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]], List.empty[Throwable]))({
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
        nodeMap.foldLeft(Set.empty[AlpNode[_ <: AlpResult]])({
          case (nodeSet, (_, node)) => nodeSet + node
        }),
        connections
      ).get
    )
  })
}
