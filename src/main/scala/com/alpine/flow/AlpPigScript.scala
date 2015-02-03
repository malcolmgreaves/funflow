package org.alpine.flow

/** Is a Pig script */
sealed case class AlpPigScript(
    loadScripts: IndexedSeq[AlpPigScript.LoadLine],
    linesAfterLoad: IndexedSeq[AlpPigScript.ScriptLine]) extends AlpScript {

  import AlpPigScript._

  override lazy val script: String = {
    (if (loadScripts.size > 0) loadScripts.map(_.toString).mkString("\n") + "\n" else "") +
      linesAfterLoad.map(_.toString).mkString("\n")
  }

  def combine(other: AlpPigScript): AlpPigScript = {
    AlpPigScript(
      takeCommonInSequence(loadScripts, other.loadScripts),
      takeCommonInSequence(linesAfterLoad, other.linesAfterLoad)
    )
  }
}

object AlpPigScript {

  trait ScriptLine extends {
    def varName: String
    def action: String
    override final def toString = s"$varName = $action"
    override def equals(x: Any): Boolean = x match {
      case a: ScriptLine => varName == a.varName && action == a.action
      case _             => false
    }
  }

  trait LoadLine extends ScriptLine {
    def filepath: String
    def format: String
    override final def action = s"LOAD $filepath AS $format"
    override def equals(x: Any): Boolean = x match {
      case a: LoadLine => varName == a.varName && filepath == a.filepath && format == a.format
      case _           => false
    }
  }

  def takeCommonInSequence[T](l1: IndexedSeq[T], l2: IndexedSeq[T]): IndexedSeq[T] = {
    (l1 ++ l2).foldLeft((IndexedSeq.empty[T], l1.toSet ++ l2.toSet))({
      case ((newSequence, remainingInCommon), element) => if (remainingInCommon.contains(element)) {
        (newSequence :+ element, remainingInCommon - element)
      } else {
        (newSequence, remainingInCommon)
      }
    })._1
  }
}
