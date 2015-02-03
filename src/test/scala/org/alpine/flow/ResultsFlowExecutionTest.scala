package org.alpine.flow

import org.scalatest.FunSuite
import scala.util.{ Failure, Success }
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import org.alpine.flow.io.{ JsonWorkflowParsingConstructionTest, JsonWorkflow }

class ResultsFlowExecutionTest extends FunSuite {

  import scala.concurrent.ExecutionContext.Implicits.global

  test("ResultsFlow rest: asyc. node execution") {

    LocalData2RDD.sc.stop()
    LocalData2RDD.sc

    // parse the JSON and instantiate the test workflow
    JsonWorkflow.toWorkflow(JsonWorkflowParsingConstructionTest.workflowStr) match {

      case Success((_, workflow)) => {

        val (resultsFlow, futureResults, errors) =
          // create the results flow and then execute all nodes, which is achieveable by executing all leaf nodes
          workflow.leaves.foldLeft((ResultsFlow(workflow), Set.empty[Future[_]], Seq.empty[String]))({
            case ((rf, results, err), leaf) =>
              rf.execute(leaf) match {

                case Success((futureResult, updatedResultsFlow)) =>
                  (updatedResultsFlow, results + futureResult, err)

                case Failure(e) =>
                  (rf, results, err :+ s"Could not execute leaf Node: $leaf : due to failure: $e")
              }
          })

        if (!errors.isEmpty) {
          val e = errors.mkString("\n")
          fail(s"Failed execution, ${errors.size} errors:\n$e")
        }

        futureResults.foreach(f => {
          val result = Await.result(f, 1 day)
          println(s"Result: $result")
        })

        //        val futureErrors = mutable.ListBuffer.empty[String]
        //
        //        futureResults.map(_.onComplete({
        //          case Success(res) =>
        //            println(s"success! $res")
        //
        //          case Failure(e) =>
        //            futureErrors.append(s"computation failed: $e")
        //        }))
        //
        //        if(!futureErrors.isEmpty){
        //          val e = futureErrors.mkString("\n")
        //          fail(s"${futureErrors.size} Errors computing results:\n$e")
        //        }

        val secondExecution = workflow.leaves
          .flatMap(leaf => resultsFlow.execute(leaf).toOption)
          .map(_._1)
          .toSet

        assert(
          futureResults == secondExecution,
          s"results don't match. Got these futures the first time: $futureResults and these the second time $secondExecution"
        )
      }

      case Failure(e) =>
        fail(s"could not instantiate workflow from JSON: $e")
    }
  }

}