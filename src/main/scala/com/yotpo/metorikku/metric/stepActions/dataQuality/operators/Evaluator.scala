package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

case class Evaluator() {

    def dqAssertion[N<%Ordered[N]](operator: String, evaluatee: N): N => Boolean = operator match {
        case "==" => {_ == evaluatee}
        case "!=" => {_ != evaluatee}
        case ">=" => {_ >= evaluatee}
        case ">" => {_ > evaluatee}
        case "<=" => {_ <= evaluatee}
        case "<" => {_ < evaluatee}
  }
}
