package org.alpine.flow

case class MapKey[K, +V](key: K)

trait TypedMap {

  /** Gets the value for the key as Some(value) if it exists or None if it doesn't exist */
  def get[K, V](key: MapKey[K, V]): Option[V]

  final def contains[K, V](key: MapKey[K, V]): Boolean =
    get(key).isDefined

  /**
   * Adds the key and value associations for two TypedMaps.
   *
   * If both maps contain a key k, then "this" mapping is used, rather than "that" mapping
   */
  def ++(that: TypedMap): TypedMap

  /**
   * Adds the key and value.
   *
   * Ensures that the resulting TypedMap has the input key -> value mapping. Any previosuly mapped value for
   * the input key is discarded.
   */
  def +[K, V](key: MapKey[K, V], value: V): TypedMap

  /**
   * Removes the key from the map.
   *
   * The removal results in a TypedMap that does not have the key anymore. Any previosuly mapped value for the input
   * key is discarded.
   */
  def -[K, V](key: MapKey[K, V]): TypedMap
}

object TypedMap {
  val empty: TypedMap = TypedMapImpl()
}

case class TypedMapImpl(m: Map[MapKey[_, _], Any] = Map.empty[MapKey[_, _], Any]) extends TypedMap {

  override def get[K, V](key: MapKey[K, V]): Option[V] =
    m.get(key).flatMap(v => Some(v.asInstanceOf[V]))

  override def +[K, V](key: MapKey[K, V], value: V): TypedMap = {
    TypedMapImpl(
      m.get(key) match {
        case Some(_) => (m - key) + (key -> value)
        case None    => m + (key -> value)
      }
    )
  }

  override def -[K, V](key: MapKey[K, V]): TypedMap =
    TypedMapImpl(m - key)

  override def ++(that: TypedMap): TypedMap = {
    that match {
      case t: TypedMapImpl => TypedMapImpl(m ++ t.m)
    }
  }
}
