package com.intenthq.pucket.util

import org.specs2.Specification
import ExceptionUtil._

import scalaz.syntax.either._

class ExceptionUtilSpec extends Specification {

  def is =
    s2"""
        Exception util must
          throw an exception when called with a failing type $failingType
          throw an exception when called implicitly with a failing type $failingTypeImplicit
          return the right hand side when called with a success type $successType
          return the right hand side when called implicitly with a successful type $successTypeImplicit
      """

  def failingType = ExceptionUtil.doThrow[Unit](new RuntimeException().left) must throwA[RuntimeException]
  def failingTypeImplicit = new RuntimeException().left.throwException must throwA[RuntimeException]

  def successType = ExceptionUtil.doThrow(0.right) === 0
  def successTypeImplicit = 0.right[Throwable].throwException === 0

}
