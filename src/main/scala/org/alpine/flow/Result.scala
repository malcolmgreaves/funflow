package org.alpine.flow

import org.apache.hadoop.fs.Path
import scala.reflect.Manifest
import java.io.File

/** Input and output type (class?) for Alpine operators */
abstract class Result extends Serializable

case class ResultTuple2[A <: Result, B <: Result](
  override val _1: A,
  override val _2: B) extends Result with Product2[A, B]

case class ResultTuple3[A <: Result, B <: Result, C <: Result](
  override val _1: A,
  override val _2: B,
  override val _3: C) extends Result with Product3[A, B, C]

object Result {

  implicit def tuple2[A <: Result, B <: Result](input: (A, B)): ResultTuple2[A, B] =
    ResultTuple2(input._1, input._2)

  implicit def tuple3[A <: Result, B <: Result, C <: Result](input: (A, B, C)): ResultTuple3[A, B, C] =
    ResultTuple3(input._1, input._2, input._3)

}

/** node type for workflow */
sealed abstract class AlpNode[+O <: Result: Manifest] {

  def id: String

  val nodeType: Manifest[_ <: O]
}

case class Data[+O <: Result: Manifest](override val id: String, data: O) extends AlpNode[O] {
  override val nodeType: Manifest[_ <: O] = manifest[O]
}

/** Node type for non-leaves in workflow */

sealed abstract class NodeOperator[C <: OpConf, -I <: Result: Manifest, +O <: Result: Manifest] extends AlpNode[O] {
  def operator: Operator[C, I, O]
  def config: C
}

case class NoConf[-I <: Result: Manifest, +O <: Result: Manifest](
    override val id: String,
    override val operator: Operator[NoOpConf, I, O]) extends NodeOperator[NoOpConf, I, O] {

  override val config = NoOpConf.instance
  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

case class Common[C <: OpConf, -I <: Result: Manifest, +O <: Result: Manifest](
    override val id: String,
    config: C,
    override val operator: Operator[C, I, O]) extends NodeOperator[C, I, O] {

  override val nodeType: Manifest[_ <: O] = operator.outputClass
}

/** A model trained by an operator */
abstract class Model extends Result

/** A result from an operator that is stored in memory. I.e. not a dataset or a model, more like a UI chart. */
final case class FinalResult[T](get: T) extends Result

object FinalResult {
  implicit def final2T[T](x: FinalResult[T]): T = x.get
}

/** Trait to describe a result produced by an operator that is itself a dataset */
sealed trait Data extends Result

trait StructuredData extends Data {
  def colInfo: ColumnInfo
}

case class LocalData(path: File) extends Data

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
case class SingleHDFSData(colInfo: ColumnInfo, fi: HadoopFile) extends Data

/**
 * Column information for a dataset spread about multiple HDFS files. Shards are assumed to contain data in
 * sequential order. I.e. fis(k) ==> fis(k+1) is a continuous part of the original data.
 */
case class MultiHDFSData(colInfo: ColumnInfo, fis: IndexedSeq[HadoopFile]) extends Data

/**
 * Represents a script that could be executed at any moment (access w/ script:T) and can be combined
 * with another script sub-type.
 */
trait Script extends Result {
  def script: String
}
