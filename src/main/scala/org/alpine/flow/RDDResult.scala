package org.alpine.flow

import org.apache.spark.rdd._

import scala.reflect.ClassTag
import org.apache.spark.{ Partitioner, HashPartitioner }

/** Wrapper for a Spark RDD. All methods are equivalent to the RDD ones. */
final case class RDDResult[T](d: RDD[T]) extends Result {

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