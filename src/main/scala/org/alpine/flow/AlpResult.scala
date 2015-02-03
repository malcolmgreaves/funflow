package org.alpine.flow

import org.apache.hadoop.fs.Path
import scala.reflect.Manifest
import java.io.File

/** Input and output type (class?) for Alpine operators */
abstract class AlpResult extends Serializable

case class AlpResultTuple2[A <: AlpResult, B <: AlpResult](
  override val _1: A,
  override val _2: B) extends AlpResult with Product2[A, B]

case class AlpResultTuple3[A <: AlpResult, B <: AlpResult, C <: AlpResult](
  override val _1: A,
  override val _2: B,
  override val _3: C) extends AlpResult with Product3[A, B, C]

object AlpResult {

  implicit def tuple2[A <: AlpResult, B <: AlpResult](input: (A, B)): AlpResultTuple2[A, B] =
    AlpResultTuple2(input._1, input._2)

  implicit def tuple3[A <: AlpResult, B <: AlpResult, C <: AlpResult](input: (A, B, C)): AlpResultTuple3[A, B, C] =
    AlpResultTuple3(input._1, input._2, input._3)

}

/** node type for workflow */
sealed abstract class AlpNode[+O <: AlpResult: Manifest] {

  def id: String

  val nodeType: Manifest[_ <: O]
}

case class Data[+O <: AlpResult: Manifest](override val id: String, data: O) extends AlpNode[O] {
  override val nodeType: Manifest[_ <: O] = manifest[O]
}

/** Node type for non-leaves in workflow */

sealed abstract class NodeOperator[C <: OpConf, -I <: AlpResult: Manifest, +O <: AlpResult: Manifest] extends AlpNode[O] {
  def operator: AlpOperator[C, I, O]
  def config: C
}

case class NoConf[-I <: AlpResult: Manifest, +O <: AlpResult: Manifest](
    override val id: String,
    override val operator: AlpOperator[NoOpConf, I, O]) extends NodeOperator[NoOpConf, I, O] {

  override val config = NoOpConf.instance
  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

case class Common[C <: OpConf, -I <: AlpResult: Manifest, +O <: AlpResult: Manifest](
    override val id: String,
    config: C,
    override val operator: AlpOperator[C, I, O]) extends NodeOperator[C, I, O] {

  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

/** A model trained by an operator */
abstract class AlpModel extends AlpResult

/** A result from an operator that is stored in memory. I.e. not a dataset or a model, more like a UI chart. */
final case class AlpFinalResult[T](get: T) extends AlpResult

object AlpFinalResult {
  implicit def final2T[T](x: AlpFinalResult[T]): T = x.get
}

/** Trait to describe a result produced by an operator that is itself a dataset */
sealed trait AlpData extends AlpResult

trait StructuredData extends AlpData {
  def colInfo: ColumnInfo
}

case class LocalData(path: File) extends AlpData

/** Describes the columns of a dataset. */
trait ColumnInfo {

  /** column type should eventually be ColDesc, which has name + type info */
  type Column = String

  /** The columns in-sequence */
  def columns: Traversable[Column]

  /** The # of columns */
  def nColumns: Long

  /** the index of a column */
  def indexOf(c: Column): Option[Long]

  /** the column associated to an index */
  def columnAt(i: Long): Option[Column]
}

/** Name and complete path to an HDFS dataset */
case class HadoopFile(fiName: String, fiDir: Path, hostport: (String, Int) = ("", 8080)) {
  val pathComplete = new Path(fiDir, fiName)
}

/** Column information for a single HDFS file as dataset */
case class AlpSingleHDFSData(colInfo: ColumnInfo, fi: HadoopFile) extends AlpData

/**
 * Column information for a dataset spread about multiple HDFS files. Shards are assumed to contain data in
 * sequential order. I.e. fis(k) ==> fis(k+1) is a continuous part of the original data.
 */
case class AlpMultiHDFSData(colInfo: ColumnInfo, fis: IndexedSeq[HadoopFile]) extends AlpData

/**
 * Represents a script that could be executed at any moment (access w/ script:T) and can be combined
 * with another script sub-type.
 */
trait AlpScript extends AlpResult {
  def script: String
}
