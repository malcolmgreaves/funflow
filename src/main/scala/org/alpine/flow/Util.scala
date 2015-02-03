package org.alpine.flow

object Util {

  /* Evaluates to true iff m1 =:= m2 (aka m1 and m2 are Manifest objects for the same type). */
  def equalManifests(m1: Manifest[_], m2: Manifest[_]): Boolean =
    m1 <:< m2 && m2 <:< m1

}