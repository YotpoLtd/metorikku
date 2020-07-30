package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

case class Evaluator() {

    def sizeAssertion(operator: String, evaluatee: Long): Long => Boolean = operator match {
        case "==" => {_ == evaluatee}
        case "!=" => {_ != evaluatee}
        case ">=" => {_ >= evaluatee}
        case ">" => {_ > evaluatee}
        case "<=" => {_ <= evaluatee}
        case "<" => {_ < evaluatee}
  }

    def uniquenessAssertion(operator: String, evaluatee: Double): Double => Boolean = operator match {
      case "==" => {_ == evaluatee}
      case "!=" => {_ != evaluatee}
      case ">=" => {_ >= evaluatee}
      case ">" => {_ > evaluatee}
      case "<=" => {_ <= evaluatee}
      case "<" => {_ < evaluatee}
    }

}
