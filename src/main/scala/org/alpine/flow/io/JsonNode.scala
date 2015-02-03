package org.alpine.flow.io

import org.alpine.flow._
import rapture.json._
import jsonBackends.jackson._
import scala.util.Try
import org.alpine.flow.NoConf
import scala.util.Failure
import scala.util.Success
import org.alpine.flow.Common
import java.io.File
import scala.reflect.Manifest

case class JsonNode(name: String, kind: String, nodeInfo: Json)

object JsonNode {

  val configurableOperator = "configurableOperator"
  val pureOperator = "pureOperator"
  val data = "data"

  type Name = String
  type JSON = String

  def toNode(s: JSON): Try[Node[_ <: Result]] =
    parse(s).flatMap({
      case (name, jNode) => JsonNodeKind.toNode(name, jNode)
    })

  def toNodeMap(strs: Seq[JSON]): Try[Map[String, Node[_ <: Result]]] = Try(
    strs.map(s =>
      toNode(s) match {
        case Success(node) =>
          node
        case Failure(e) =>
          throw e
      }
    ).foldLeft(Map.empty[String, Node[_ <: Result]])(
      (m, node) =>
        if (m.contains(node.id)) {
          throw new IllegalArgumentException(s"Cannot have duplicate nodes in the map. Duplicate node name/id: ${node.id}")
        } else {
          m + (node.id -> node)
        }
    )
  )

  def toNodeMap(jsonNodeList: JSON): Try[Map[String, Node[_ <: Result]]] =
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

  def toNode(name: Name, jn: JsonNodeKind): Try[Node[_ <: Result]] =
    jn match {
      case opConf: JsonOpConfNode     => JsonOpConfNode.toNode(name, opConf)
      case opNoConf: JsonOpNoConfNode => JsonOpNoConf.toNode(name, opNoConf)
      case data: JsonDataNode         => JsonDataNode.toNode(name, data)
    }
}

object JsonOpNoConf {

  type Name = String

  def toNode(name: Name, jn: JsonOpNoConfNode): Try[NoConf[_ <: Result, _ <: Result]] = Try({

    val operator = Class.forName(jn
      .operator).newInstance.asInstanceOf[OperatorNoConf[Result, Result]]

    NoConf(
      name,
      operator.asInstanceOf[OperatorNoConf[_ <: Result, _ <: Result]]
    )
  })

}

object JsonOpConfNode {

  import org.alpine.flow.Util._

  type Name = String

  def toNode(name: Name, jn: JsonOpConfNode): Try[Common[_ <: OpConf, _ <: Result, _ <: Result]] = Try({

    val operator = Class.forName(jn.operator).newInstance.asInstanceOf[Operator[OpConf, Result, Result]]

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
          operator.asInstanceOf[Operator[OpConf, _ <: Result, _ <: Result]]
        ).asInstanceOf[Common[_ <: OpConf, _ <: Result, _ <: Result]]

      case Failure(e) =>
        throw e
    }
  })

}

abstract class ConfMaker[C <: OpConf: Manifest] {
  type JSON = String
  def make(s: JSON): Try[C]
  final val configClass = manifest[C]
}

object JsonDataNode {

  val DB = "DB"
  val HDFS = "HDFS"
  val RDD = "RDD"
  val Local = "Local"

  type Name = String

  //case class Data[O <: Result: Manifest](override val id: String, data: O) extends Node[O]
  def toNode[D <: DataNode[_ <: Result]](name: Name, jn: JsonDataNode): Try[DataNode[_ <: Result]] = Try(
    jn.dataType match {
      case DB   => ???
      case HDFS => ???
      case RDD => jn.special match {
        case Some(format) => ???
        case None         => ???
      }
      case Local =>
        DataNode(name, LocalData(new File(jn.dataLoc)))

      case _ =>
        throw new IllegalArgumentException(s"Unknown Data Type for JsonDataNode: ${jn.dataType} @ location: ${jn.dataLoc}")
    }
  )

}
