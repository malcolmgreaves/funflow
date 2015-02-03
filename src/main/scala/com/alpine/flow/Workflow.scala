package org.alpine.flow

import scala.util.{ Success, Failure, Try }

import scala.reflect.Manifest

sealed trait Workflow {

  def contains(n: AlpNode[_ <: AlpResult]): Boolean

  def predecessorsOf[C <: OpConf: Manifest, I <: AlpResult: Manifest, O <: AlpResult: Manifest](n: AlpNode[O]): Option[Connection[C, I, O]]

  def foreach[U](f: AlpNode[_ <: AlpResult] => U): Unit

  def leaves: Set[AlpNode[_ <: AlpResult]]

  def foldLeft[U](zero: U)(combOp: (U, AlpNode[_ <: AlpResult]) => U): U

  def replace(inWorkflow: AlpNode[_ <: AlpResult], newNode: AlpNode[_ <: AlpResult]): Try[Workflow]

}

object Workflow {

  def create(
    nodes: Set[AlpNode[_ <: AlpResult]],
    connections: Set[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]]): Try[Workflow] = Try({

    // make sure that all of the nodes specified in each connection are contained in the nodeset
    // incldues Connection.nodes as well as Connection.destination
    {
      val nodeIDs = nodes.map(_.id).toSet
      if (!connections.forall(con => con.nodes.map(_.id).forall(nID => nodeIDs.contains(nID)) && nodeIDs.contains(con.destination.id)))
        throw new IllegalArgumentException(s"All nodes defined in the connections must be defined in the input nodeset!")
    }

    // get the workflow into "execution order"
    // TODO doc
    val child2parents =
      connections.foldLeft(TypedMap.empty)({
        case (cmap, conn) => cmap + (mkKeyConnection(conn.destination), conn)
      }).asInstanceOf[TypedMapImpl]

    /*

    leaves are destinations of connections that do not exist in any parent nodes of any connection

    +

    the nodes that do not occur in any connection

     */

    val leafNodes =
      if (connections.size == 0) {
        nodes

      } else {

        val parents = connections.foldLeft(Map.empty[String, AlpNode[_ <: AlpResult]])(
          (parents, conn) => conn.nodes.foldLeft(parents)(
            (p, n) =>
              if (p.contains(n.id))
                p
              else
                p + (n.id -> n)
          )
        )

        val leavesOfPaths =
          connections
            // get all of the destinations that occur in the workflow
            .foldLeft(Map.empty[String, AlpNode[_ <: AlpResult]])(
              (dests, conn) =>
                if (dests.contains(conn.destination.id))
                  dests
                else
                  dests + (conn.destination.id -> conn.destination)
            )
            .toSeq
            // grab only the ones that are never a parent of any connection
            .filter({
              case (id, destination) => !parents.contains(id)
            })
            .map(_._2)
            .toSet

        val forest =
          nodes
            // if it is not a destination, then it might be in the forest
            .filter(n => child2parents.get(mkKeyConnection(n)).isEmpty)
            // if it is not a parent as well, then it in the forest
            .filter(n => connections.forall(conn => conn.nodes.forall(cn => cn.id != n.id)))
            .toSet

        leavesOfPaths ++ forest
      }

    new Workflow {

      private val nodeIDs = nodes.map(_.id)

      override def predecessorsOf[C <: OpConf: Manifest, I <: AlpResult: Manifest, O <: AlpResult: Manifest](n: AlpNode[O]): Option[Connection[C, I, O]] =
        child2parents.get(mkKeyConnection[C, I, O](n))

      override def contains(n: AlpNode[_ <: AlpResult]): Boolean =
        nodeIDs.contains(n.id)

      override def foreach[U](f: AlpNode[_ <: AlpResult] => U): Unit =
        nodes.foreach(f)

      override val leaves: Set[AlpNode[_ <: AlpResult]] =
        leafNodes

      override def foldLeft[U](zero: U)(combOp: (U, AlpNode[_ <: AlpResult]) => U): U =
        nodes.foldLeft(zero)(combOp)

      override def replace(inWorkflow: AlpNode[_ <: AlpResult], newNode: AlpNode[_ <: AlpResult]): Try[Workflow] = Try({

        if (contains(inWorkflow) && Util.equalManifests(inWorkflow.nodeType, newNode.nodeType)) {

          val newNodes = nodes.filter(n => n.id != inWorkflow.id) + newNode

          predecessorsOf(inWorkflow) match {

            case Some(conn) =>
              conn.replaceDestination(newNode.asInstanceOf[NodeOperator[OpConf, AlpResult, AlpResult]]) match {

                case Success(newConn) =>

                  val newConnections =
                    (connections.filter(conn => conn.destination.id != newNode.id) + newConn)
                      .foldLeft(Set.empty[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]])({
                        case (news, connect) => {
                          connect.replaceParent(inWorkflow, newNode) match {
                            case Success(replaced) =>
                              news + replaced
                            case Failure(_) =>
                              news
                          }
                        }
                      })

                  Workflow.create(newNodes, newConnections).get

                case Failure(e) =>
                  throw e
              }

            case None =>
              val newConnections =
                connections.filter(conn => conn.destination.id != newNode.id)
                  .foldLeft(Set.empty[Connection[_ <: OpConf, _ <: AlpResult, _ <: AlpResult]])({
                    case (news, connect) => {
                      connect.replaceParent(inWorkflow, newNode) match {
                        case Success(replaced) =>
                          news + replaced
                        case Failure(_) =>
                          news
                      }
                    }
                  })

              Workflow.create(newNodes, newConnections).get
          }

        } else {
          throw new IllegalArgumentException(s"node $inWorkflow is not in the workflow")
        }
      })

    }

  })

  @inline private def mkKeyConnection[C <: OpConf: Manifest, I <: AlpResult: Manifest, O <: AlpResult: Manifest](n: AlpNode[O]): MapKey[AlpNode[O], Connection[C, I, O]] =
    MapKey(n)

}
