package org.alpine.flow

import org.scalatest.FunSuite

import rapture.json._
import jsonBackends.jackson._

import scala.util.{ Try, Failure, Success }
import org.alpine.flow.io.{ JsonConnection, JsonWorkflowParsingConstructionTest, JsonNode }
import rapture.json.Json

class ConnectionTest extends FunSuite {

  import JsonWorkflowParsingConstructionTest._

  test("Test Destination Replacement") {

    Try(Json.parse(connectionsStr(0)).as[JsonConnection]).flatMap(x => JsonConnection.toConnection(nodeMap, x)) match {

      case Success(conn) =>
        val newDestination = {
          def op(ignore: NoOpConf, l: LocalData): RDDAlpResult[String] =
            throw new RuntimeException("just type checking, no invoking")
          NoConf[LocalData, RDDAlpResult[String]](conn.destination.id, AlpOperator.fn2op(op))
        }
        conn.replaceDestination(newDestination.asInstanceOf[NodeOperator[OpConf, AlpResult, AlpResult]])

      case Failure(e) =>
        fail(s"Couldn't parse $connectionsStr(0) : $e")
    }
  }

  test("Test Parent Replacement") {

    Try(Json.parse(connectionsStr(0)).as[JsonConnection]).flatMap(x => JsonConnection.toConnection(nodeMap, x)) match {

      case Success(conn) =>
        val newParent = JsonNode.toNode(nodesStr(0)).get
        conn.replaceParent(conn.nodes(0), newParent)

      case Failure(e) =>
        fail(s"Couldn't parse $connectionsStr(0) : $e")
    }
  }

}