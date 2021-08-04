package com.yotpo.metorikku.code.steps

case class InputMatcher[K](ks: K*) {
  def unapplySeq[V](m: Map[K, V]): Option[Seq[V]] = if (ks.forall(m.contains)) Some(ks.map(m)) else None
}
