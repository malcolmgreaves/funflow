package org.alpine.flow

//import java.util.concurrent.{ Future, TimeUnit }
//
//class CombinerInt extends Combiner[Int] {
//  override def combine(newThing: Future[Int]): Combiner[Int] = ???
//
//  override def get:
//}

import org.scalatest.FunSuite
//
//class AlpFlowResultTest extends FunSuite {
//
//  import org.alpine.flow.AlpFlowResultTest._
//
//  test("test AlpWorkflow") {
//    val result = elNode.apply.get()
//    println(s"result: $result")
//  }
//
//}
//
//object AlpFlowResultTest {
//
//  import org.alpine.flow.AlpOperator._
//
//  val elOperator = (p: AlpPigScript) => p
//
//  val script = AlpPigScript(
//    IndexedSeq("load from Russia with love"),
//    IndexedSeq("action script line", "from russia", "without love!")
//  )
//
//  class LLeaf(override val input: AlpPigScript, override val op: AlpOperator[AlpPigScript, AlpPigScript]) extends Leaf[AlpPigScript, AlpPigScript](input, op) {
//    override def mkFuture(x: => AlpPigScript) = new Future[AlpPigScript] {
//
//      override def isCancelled: Boolean = false
//
//      override def get(): AlpPigScript = x
//
//      override def get(l: Long, timeUnit: TimeUnit): AlpPigScript = x
//
//      override def cancel(b: Boolean): Boolean = false
//
//      override def isDone: Boolean = true
//    }
//  }
//
//  val script1 = new LLeaf(script.copy(linesAfterLoad = IndexedSeq("change 1")), elOperator)
//  val script2 = new LLeaf(script.copy(linesAfterLoad = IndexedSeq("change 2")), elOperator)
//  val script3 = new LLeaf(script.copy(linesAfterLoad = IndexedSeq("change 3")), elOperator)
//
//  class LCommon(override val children: Seq[AlpNode[AlpPigScript]], override val op: AlpOperator[AlpPigScript, AlpPigScript], override val comb: Combiner[AlpPigScript]) extends Common[AlpPigScript, AlpPigScript](children, op, comb) {
//    override def mkFuture(x: => AlpPigScript) = new Future[AlpPigScript] {
//
//      override def isCancelled: Boolean = false
//
//      override def get(): AlpPigScript = x
//
//      override def get(l: Long, timeUnit: TimeUnit): AlpPigScript = x
//
//      override def cancel(b: Boolean): Boolean = false
//
//      override def isDone: Boolean = true
//    }
//  }
//
//  val elNode = new LCommon(
//    children = Seq(script1, script2, script3),
//    op = elOperator,
//    comb = new Combiner[AlpPigScript] {
//
//      override def combine(aggregate: AlpPigScript, newThing: Future[AlpPigScript]): AlpPigScript = {
//        aggregate.combine(newThing.get())
//      }
//
//      override def zero: AlpPigScript = {
//        val e = IndexedSeq.empty[String]
//        AlpPigScript(e, e)
//      }
//    }
//  )
//
//}