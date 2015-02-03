package org.alpine.flow

import scala.concurrent.{ ExecutionContext, Future }
import scala.Some
import scala.util.{ Failure, Success, Try }

import scala.reflect.Manifest

/**
 * A workflow along with a caching mechanism that recalls the results for every executed node.
 *
 */
final case class ResultsFlow(
    wf: Workflow,
    nodesWithResults: TypedMap = TypedMap.empty)(implicit val executionContext: ExecutionContext) {

  def execute[T <: Result: Manifest](n: AlpNode[T]): Try[(Future[T], ResultsFlow)] =
    if (wf.contains(n)) {

      val nodeKey = mkKey(n)

      nodesWithResults.get(nodeKey) match {

        case Some(r) =>
          Success(r, this)

        case None =>
          Try(execute_h(n)).map(result =>
            (result, this.copy(nodesWithResults = this.nodesWithResults + (nodeKey, result)))
          )
      }
    } else {
      Failure(nodeNotFound(n))
    }

  private def nodeNotFound(n: AlpNode[_ <: Result]) =
    new IllegalArgumentException(s"node $n not found in workflow")

  /** WARNING: Throws IllegalStateException if there is a non-leaf node with no children in the Workflow! */
  private def execute_h[B <: Result: Manifest](n: AlpNode[B]): Future[B] =
    nodesWithResults.get(mkKey(n)) match {

      case Some(r) =>
        r

      case None =>
        n match {

          // node is a data source
          case Data(_, data) =>
            Future.successful(data)

          // node is an operator
          case other: NodeOperator[OpConf, Result, B] => {

            // extract the operator from the node
            val operator: Result => B =
              other.operator.curried(other.config)

            wf.predecessorsOf(n) match {

              case Some(parentConnection) =>
                // execute each child node in parallel, obtaining a Future[Seq[_]] representing their evaluations
                Future.sequence(parentConnection.nodes.map(nde => execute_h(nde)))
                  // then combine these results into an input that our operator can accept
                  .map(results =>
                    parentConnection.destination.operator.inputClass match {
                      case tuple3 if tuple3 <:< manifest[ResultTuple3[_, _, _]] =>
                        ResultTuple3(results(0), results(1), results(2))

                      case tuple2 if tuple2 <:< manifest[ResultTuple2[_, _]] =>
                        ResultTuple2(results(0), results(1))

                      case _ =>
                        results(0)
                    })
                  // then take this combined input and perform the node's operation op, evaluating to a type Future[O]
                  .map(operator)

              case None =>
                throw new IllegalStateException(s"Illegal Workflow construction, no parents of operator node: $n")
            }
          }
        }
    }

  def clearCachedResults(ns: AlpNode[T forSome { type T <: Result }]*): ResultsFlow =
    this.copy(nodesWithResults = ns.foldLeft(this.nodesWithResults)((mm, n) => mm - mkKey(n)))

  @inline private[flow] def mkKey[T <: Result](n: AlpNode[T]): MapKey[AlpNode[T], Future[T]] =
    MapKey(n)
}
