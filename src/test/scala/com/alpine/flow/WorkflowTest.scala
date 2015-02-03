package org.alpine.flow

import org.scalatest.FunSuite

import scala.util.{ Failure, Success }
import org.alpine.flow.io.{ JsonWorkflowParsingConstructionTest, JsonWorkflow, JsonNode }

class WorkflowTest extends FunSuite {

  import JsonWorkflowParsingConstructionTest._

  test("Correct computation of Workflow's leaves (nodes with no children)") {

    JsonWorkflow.toWorkflow(workflowStr) match {

      case Success((_, wf)) =>
        val l = wf.leaves.mkString(",")
        assert(wf.leaves.size == 1,
          s"workflow doesn't have correct # of leaves, expecting 1, actual: ${wf.leaves.size} : $l")

        val id = "dictionaryMaker"
        assert(wf.leaves.toSeq(0).id == id,
          s"expecting Node ID to be $id, not ${wf.leaves.toSeq(0).id}")

      case Failure(e) =>
        fail(s"Failed to parse & instantiate Workflow: $e")
    }
  }

  test("Replace Node in Workflow") {

    JsonWorkflow.toWorkflow(workflowStr) match {

      case Success((_, wf)) =>
        JsonNode.toNode(nodesStr(nodesStr.size - 1)) match {

          case Success(newN) =>
            val n = wf.leaves.toSeq(0)
            wf.replace(n, newN) match {
              case Success(_) =>
                Unit
              case Failure(e) =>
                fail(s"Failed to replace node $n with $newN in workflow: $e")
            }

          case Failure(e) =>
            fail(s"Failed to create node for replacement: $e")
        }

      case Failure(e) =>
        fail(s"Failed to parse & instantiate Workflow: $e")
    }
  }

}