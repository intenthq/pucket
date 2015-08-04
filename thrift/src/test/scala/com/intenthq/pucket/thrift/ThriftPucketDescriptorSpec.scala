package com.intenthq.pucket.thrift

import com.intenthq.pucket.thrift.ThriftTestUtils._
import com.intenthq.pucket.test.model.ThriftTest
import org.json4s.ParserUtil._
import org.scalacheck.{Gen, Prop}
import org.specs2.matcher.DisjunctionMatchers
import org.specs2.{ScalaCheck, Specification}

class ThriftPucketDescriptorSpec extends Specification with DisjunctionMatchers with ScalaCheck {
  import ThriftPucketDescriptorSpec._

  def is =
    s2"""
       The Pucket Descriptor
         Can serialise and deserialise from JSON $fromJson
         Fails to deserialise a non-json string $badString
         Fails when schema class isn't in the class path $schemaClassNotFound
         Fails when schema class is not the expected type $schemaClassIncorrect
      """

  def fromJson =
    Prop.forAll(descriptorGen) { d =>
      ThriftPucketDescriptor[ThriftTest](classOf[ThriftTest], d.toString) must be_\/-.like {
        case a =>
          (a.schemaClass === d.schemaClass) and
          (a.compression === d.compression) and
          (a.partitioner.map(_.getClass) === d.partitioner.map(_.getClass))
      }
    }

  def badString =
    Prop.forAll(randomString) { s =>
      ThriftPucketDescriptor[ThriftTest](classOf[ThriftTest], s) must be_-\/[Throwable].like {
        case a => a must beAnInstanceOf[ParseException]
      }
    }

  def schemaClassNotFound =
    ThriftPucketDescriptor[ThriftTest](classOf[ThriftTest], badSchemaClass) must be_-\/.like {
      case a => a must beAnInstanceOf[ClassNotFoundException]
    }

  def schemaClassIncorrect =
    ThriftPucketDescriptor[ThriftTest](classOf[ThriftTest], incorrectSchemaClass) must be_-\/.like {
      case a => a must beAnInstanceOf[RuntimeException]
    }
}

object ThriftPucketDescriptorSpec {
  val rng = scala.util.Random
  val badSchemaClass = json("legend.nic.Cage")
  val incorrectSchemaClass = json("com.intenthq.pucket.test.model.ThriftTest2")
  def json(clazz: String) = s"""{"thriftSchemaClass": "$clazz", "compression": "SNAPPY"}"""
  def randomString: Gen[String] = rng.nextString(50)
}
