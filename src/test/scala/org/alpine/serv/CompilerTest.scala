package org.alpine.serv

import org.alpine.flow._
import org.scalatest.FunSuite
import org.alpine.flow.io.{ JsonNode, JsonConnection }
import org.alpine.flow.NoConf

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import scala.util.{ Failure, Success }

class CompilerTest extends FunSuite {

  import org.alpine.flow.io.JsonWorkflowParsingConstructionTest._

  test("simple compile") {

    val funcStr = "(a:AlpFinalResult[Map[String,Int]]) => {println(a); a}"
    val compiler = new Compiler(None)
    val clazz = compiler.compile(s"import org.alpine.flow._; $funcStr")
    val x = clazz.newInstance()
    println(s"loaded class: $x")
    val a = FinalResult(Map("hello" -> 1))
    val prex = x.asInstanceOf[() => Any]
    println(s"cased class, prex: $prex")
    val res = prex.apply()
    println(s"prex.apply(): $res")
    val xer = res.asInstanceOf[(FinalResult[Map[String, Int]] => FinalResult[Map[String, Int]])]
    xer.apply(a)

  }

  test("advanced compile: new operator + integration into workflow") {

    val funcStr = """(a:AlpFinalResult[Map[String,Int]]) => {println("hello world!!!"); a}"""
    val compiler = new Compiler(None)
    val clazz = compiler.compile(s"import org.alpine.flow._; $funcStr")
    val func = clazz.newInstance().asInstanceOf[() => Any].apply()
      .asInstanceOf[(FinalResult[Map[String, Int]] => FinalResult[Map[String, Int]])]
    val newNode =
      NoConf(
        "dynamic yo!",
        Operator.fn2opNoConf(func)
      ).asInstanceOf[NodeOperator[NoOpConf, Result, FinalResult[Map[String, Int]]]]

    val connections: Seq[Connection[_ <: OpConf, _ <: Result, _ <: Result]] = {
      val nm = nodeMap // + (newNode.id -> newNode)
      connectionsJson.map(s => JsonConnection.toConnection(nm, s).get)
    }

    val newConnection: Connection[_ <: OpConf, _ <: Result, _ <: Result] =
      Connection.forOne(connections(connections.size - 1).destination, newNode) match {
        case Success(c) =>
          c
        case Failure(e) =>
          fail(s"Failed to create connection with new dynamically generated node: $e")
      }

    val nodes = nodesStr.map(s => JsonNode.toNode(s).get)

    val wf = Workflow.create(
      (nodes :+ newNode).toSet,
      (connections :+ newConnection).toSet
    ).get

    import scala.concurrent.ExecutionContext.Implicits.global
    val rf = ResultsFlow(wf)

    val futureResult = rf.execute(newNode).get._1

    val res = Await.result(futureResult, 10 minutes)
    println(s"result: $res")
  }

  ignore("ultra advanced: new Result type operators integration at runtime"){
    val newNode1 = {
      val compiler = new Compiler(None)
      val funcStr1 =
        """
          | case class NewAlpResult(m:Map[String,Int]) extends AlpResult
          | (a:AlpFinalResult[Map[String,Int]]) => {println("hello world!!!"); NewAlpResult(a)}""".stripMargin.trim
      val clazz = compiler.compile(s"import org.alpine.flow._; $funcStr1")
      val func = clazz.newInstance().asInstanceOf[() => Any].apply()
        .asInstanceOf[(FinalResult[Map[String, Int]] => _ <: Result)]
      NoConf(
        "1 dynamic yo!",
        Operator.fn2opNoConf(func)
      ).asInstanceOf[NodeOperator[NoOpConf, Result, Result]]
    }

    val newNode2 = {
      val compiler = new Compiler(None)
      val funcStr2 =
        """
          | case class NewAlpResult(m:Map[String,Int]) extends AlpResult
          | (a:NewAlpResult) => {println("whoa! this is pretty cool!"); a}
        """.stripMargin.trim
      val clazz = compiler.compile(s"import org.alpine.flow._; $funcStr2")
      val func = clazz.newInstance().asInstanceOf[() => Any].apply()
        .asInstanceOf[(Result => _ <: Result)]
      NoConf(
        "2 dynamic yo!",
        Operator.fn2opNoConf(func)
      ).asInstanceOf[NodeOperator[NoOpConf, Result, Result]]
    }

    val connections: Seq[Connection[_ <: OpConf, _ <: Result, _ <: Result]] = {
      val nm = nodeMap // + (newNode.id -> newNode)
      connectionsJson.map(s => JsonConnection.toConnection(nm, s).get)
    }

    val newConnection1: Connection[_ <: OpConf, _ <: Result, _ <: Result] =
      Connection.forOne(connections(connections.size - 1).destination, newNode1) match {
        case Success(c) =>
          c
        case Failure(e) =>
          throw new Exception(s"Failed to create connection with new dynamically generated node: $e")
      }

    val newConnection2: Connection[_ <: OpConf, _ <: Result, _ <: Result] =
      Connection.forOne(newNode1, newNode2) match {
        case Success(c) =>
          c
        case Failure(e) =>
          throw new Exception(s"Failed to create connection with new dynamically generated node: $e")
      }

    val nodes = nodesStr.map(s => JsonNode.toNode(s).get)

    val wf = Workflow.create(
      ((nodes :+ newNode1) :+ newNode2).toSet,
      ((connections :+ newConnection1) :+ newConnection2).toSet
    ).get

    import scala.concurrent.ExecutionContext.Implicits.global
    val rf = ResultsFlow(wf)

    val futureResult = rf.execute(newNode2).get._1

    val res = Await.result(futureResult, 10 minutes)
    println(s"result: $res")
  }

}
