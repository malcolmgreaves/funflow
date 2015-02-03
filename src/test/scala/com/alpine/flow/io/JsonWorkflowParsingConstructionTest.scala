package org.alpine.flow.io

import org.scalatest.FunSuite
import rapture.json._
import jsonBackends.jackson._
import scala.util.{ Failure, Success, Try }
import org.alpine.flow.{ AlpNode, AlpResult }
import org.apache.spark.sql.catalyst.errors
import org.apache.spark.sql.catalyst

class JsonWorkflowParsingConstructionTest extends FunSuite {

  import JsonWorkflowParsingConstructionTest._

  // NODE PARSING & INSTANTIATION TESTS

  test("JSON: parsing of multiple Nodes") {
    val errors = nodesStr.zip(nodesJson).foldLeft(List.empty[String])({
      case (errs, (str, jsonNodeKind)) =>
        Try(Json.parse(str).as[JsonNode]) match {
          case Success(jsonNode) =>
            JsonNode.convert(jsonNode) match {
              case Success((_, jnkind)) =>
                if (jnkind == jsonNodeKind) {
                  errs
                } else {
                  errs :+ s"Successful parse and conversion, but did not equal intended JsonNodeKind: parsed: $jnkind expecting: $jsonNodeKind"
                }
              case Failure(e) =>
                errs :+ s"Successful parse, but couldn't convert $str to JsonNodeKind: $e"
            }
          case Failure(_) =>
            errs :+ s"Couldn't parse $str"
        }
    })

    assert(errors.isEmpty, s"${errors.size} errors:\n" + errors.mkString("\t"))
  }

  test("Instantiation: from JSON to Nodes") {
    val errors = nodesStr.foldLeft(List.empty[String])(
      (errs, str) =>
        Try(JsonNode.toNode(str)) match {
          case Success(_) =>
            errs
          case Failure(e) =>
            errs :+ s"Couldn't parse $str : $e"
        }
    )

    assert(errors.isEmpty, s"${errors.size} errors:\n" + errors.mkString("\t"))
  }

  // CONNECTION PARSING & INSTANTIATION TESTS

  test("JSON: parsing multiple Connections") {
    val errors = connectionsStr.zip(connectionsJson).foldLeft(List.empty[String])({
      case (errs, (str, json)) =>
        Try(Json.parse(str).as[JsonConnection]) match {
          case Success(jConn) =>
            if (jConn == json) {
              errs
            } else {
              errs :+ s"Successful parse, but did not equal correct object: JSON: $str \tobject: $json"
            }
          case Failure(e) =>
            errs :+ s"Couldn't parse $str : $e"
        }
    })

    assert(errors.isEmpty, s"${errors.size} errors:\n" + errors.mkString("\t"))
  }

  test("Instantiation: from JSON to Connections") {

    val nodeMap: Map[String, AlpNode[_ <: AlpResult]] =
      JsonNode.toNodeMap(nodesStr) match {
        case Success(nm) =>
          nm
        case Failure(e) =>
          fail(s"failed to create node mapping: $e")
      }

    val errors = connectionsStr.foldLeft(List.empty[String])(
      (errs, str) =>
        Try(Json.parse(str).as[JsonConnection]).flatMap(x => JsonConnection.toConnection(nodeMap, x)) match {
          case Success(_) =>
            errs
          case Failure(e) =>
            errs :+ s"Couldn't parse $str : $e"
        }
    )

    assert(errors.isEmpty, s"${errors.size} errors:\n" + errors.mkString("\t"))
  }

  // WORKFLOW PARSING & INSTANTIATION TESTS

  test("JSON: parsing Workflow") {
    Try(Json.parse(workflowStr).as[JsonWorkflow]) match {
      case Failure(e) =>
        fail(s"Could not parse JSON workflow: $e")
      case Success(wf) =>
        if (wf != workflowJson)
          fail(s"Parsed workflow, but did not equal correct result. Parsed: $wf Actual: $workflowJson")
    }
  }

  test("Instantiation: from JSON to Workflow") {
    JsonWorkflow.toWorkflow(workflowStr) match {
      case Success(_) =>
        Unit
      case Failure(e) =>
        fail(s"could not parse & instantiate workflow: $e")
    }
  }

}

object JsonWorkflowParsingConstructionTest {

  // NODE DATA

  val nodesStr = Seq(
    """{
      |     "name": "project README",
      |     "kind":"data",
      |     "nodeInfo": {
      |       "dataType":"Local",
      |       "dataLoc":"./README.md"
      |     }
      |   }""",
    """{
      |     "name": "to RDD",
      |     "kind" : "pureOperator",
      |     "nodeInfo" : {
      |       "operator":"org.alpine.flow.LocalData2RDD"
      |     }
      |   }""",
    """{
      |     "name": "tokenizer",
      |     "kind" : "pureOperator",
      |     "nodeInfo" : {
      |       "operator":"org.alpine.flow.String2TokensOp"
      |     }
      |   }""",
    """{
      |     "name": "dictionaryMaker",
      |     "kind" : "pureOperator",
      |     "nodeInfo" : {
      |       "operator":"org.alpine.flow.Tokens2DictOp"
      |     }
      |}"""
  ).map(_.stripMargin.trim)

  val nodesJson = Seq(
    JsonDataNode("Local", "./README.md", None),
    JsonOpNoConfNode("org.alpine.flow.LocalData2RDD"),
    JsonOpNoConfNode("org.alpine.flow.String2TokensOp"),
    JsonOpNoConfNode("org.alpine.flow.Tokens2DictOp")
  )

  // CONNECTION DATA

  val connectionsStr = Seq(
    """        {
      |          "nodes": ["project README"],
      |          "dest":"to RDD"
      |        }""",
    """        {
      |            "nodes" : [ "to RDD" ],
      |            "dest" : "tokenizer"
      |        }""",
    """        {
      |            "nodes" : [ "tokenizer" ],
      |            "dest" : "dictionaryMaker"
      |        }"""
  ).map(_.stripMargin.trim)

  val connectionsJson = Seq(
    JsonConnection(Seq("project README"), "to RDD"),
    JsonConnection(Seq("to RDD"), "tokenizer"),
    JsonConnection(Seq("tokenizer"), "dictionaryMaker")
  )

  def nodeMap: Map[String, AlpNode[_ <: AlpResult]] =
    JsonNode.toNodeMap(nodesStr) match {
      case Success(nm) =>
        nm
      case Failure(e) =>
        throw new IllegalStateException(s"failed to create node mapping: $e")
    }

  // WORKFLOW DATA

  val workflowStr =
    """{
      |    "flowName": "tokenization workflow",
      |    "nodes": [
      |        {
      |            "name": "project README",
      |            "kind":"data",
      |            "nodeInfo": {
      |                "dataType":"Local",
      |                "dataLoc":"./README.md"
      |            }
      |        },
      |        {
      |            "name": "to RDD",
      |            "kind" : "pureOperator",
      |            "nodeInfo" : {
      |                "operator":"org.alpine.flow.LocalData2RDD"
      |            }
      |        },
      |        {
      |            "name": "tokenizer",
      |            "kind" : "pureOperator",
      |            "nodeInfo" : {
      |                "operator":"org.alpine.flow.String2TokensOp"
      |            }
      |        },
      |        {
      |            "name": "dictionaryMaker",
      |            "kind" : "pureOperator",
      |            "nodeInfo" : {
      |                "operator":"org.alpine.flow.Tokens2DictOp"
      |            }
      |        }
      |    ],
      |    "connections" : [
      |        {
      |          "nodes": ["project README"],
      |          "dest":"to RDD"
      |        },
      |        {
      |            "nodes" : [ "to RDD" ],
      |            "dest" : "tokenizer"
      |        },
      |        {
      |            "nodes" : [ "tokenizer" ],
      |            "dest" : "dictionaryMaker"
      |        }
      |    ]
      |
      |}""".stripMargin.trim

  lazy val workflowJson = JsonWorkflow(
    "tokenization workflow",
    nodesStr.map(s => Json.parse(s).as[JsonNode]),
    connectionsJson
  )

}