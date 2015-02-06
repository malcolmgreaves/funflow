package org.alpine.flow

import scala.reflect.ClassTag
import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partitioner, HashPartitioner }

/** Input and output type (class?) for Alpine operators */
sealed abstract class Result extends Serializable

sealed trait DataResult extends Result

final case class DBData(schema:String, table:String) extends DataResult

final case class LocalData(path: File) extends DataResult

sealed trait ScriptResult extends Result {
  def script: String
}

final case class SQLScript(script: String) extends ScriptResult

final case class PigScript(script: String) extends ScriptResult

/** A result from an operator that is stored in memory. I.e. not a dataset or a model, more like a UI chart. */
final case class WrapResult[T](get: T) extends Result

/** Wrapper for a Spark RDD. All methods are equivalent to the RDD ones. */
final case class RDDResult[T](d: RDD[T]) extends DataResult {

  def map[U: ClassTag](f: T => U): RDDResult[U] =
    RDDResult(d.map(f))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDDResult[U] =
    RDDResult(d.flatMap(f))

  def filter(f: T => Boolean): RDDResult[T] =
    RDDResult(d.filter(f))

  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDDResult[T] =
    RDDResult(d.distinct(numPartitions)(ord))

  def distinct(): RDDResult[T] =
    distinct(d.partitions.size)

  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDDResult[(K, Iterable[T])] =
    groupBy(f, new HashPartitioner(numPartitions))

  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDDResult[(K, Iterable[T])] =
    RDDResult(d.groupBy(f, p)(kt, ord))

  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDDResult[U] =
    RDDResult(d.mapPartitions(f, preservesPartitioning))

  def zip[U: ClassTag](other: RDDResult[U]): RDDResult[(T, U)] =
    RDDResult(d.zip(other.d))

  def foreach(f: T => Unit) =
    d.foreach(f)

  def foreachPartition(f: Iterator[T] => Unit) =
    d.foreachPartition(f)

  def reduce(f: (T, T) => T): T =
    d.reduce(f)

  def fold(zeroValue: T)(op: (T, T) => T): T =
    d.fold(zeroValue)(op)

  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U =
    d.aggregate(zeroValue)(seqOp, combOp)
}

object RDDResult {

  implicit def rdd2rddResult[T](r: RDD[T]): RDDResult[T] =
    RDDResult(r)

}

//
// Tuples
//

final case class ResultTuple2[A <: Result, B <: Result](
  override val _1: A,
  override val _2: B) extends Result with Product2[A, B]

final case class ResultTuple3[A <: Result, B <: Result, C <: Result](
  override val _1: A,
  override val _2: B,
  override val _3: C) extends Result with Product3[A, B, C]

object Result {

  implicit def tuple2[A <: Result, B <: Result](input: (A, B)): ResultTuple2[A, B] =
    ResultTuple2(input._1, input._2)

  implicit def tuple3[A <: Result, B <: Result, C <: Result](input: (A, B, C)): ResultTuple3[A, B, C] =
    ResultTuple3(input._1, input._2, input._3)

}