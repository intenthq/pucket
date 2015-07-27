package com.intenthq.thrift

import com.intenthq.pucket.thrift.ThriftPucketDescriptor
import com.intenthq.test.model.Test
import com.intenthq.thrift.ThriftTestUtils._
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
      ThriftPucketDescriptor[Test](classOf[Test], d.toString) must be_\/-.like {
        case a =>
          (a.schemaClass === d.schemaClass) and
          (a.compression === d.compression) and
          (a.partitioner === d.partitioner)
      }
    }

  def badString =
    Prop.forAll(randomString) { s =>
      ThriftPucketDescriptor[Test](classOf[Test], s) must be_-\/[Throwable].like {
        case a => a must beAnInstanceOf[ParseException]
      }
    }

  def schemaClassNotFound =
    ThriftPucketDescriptor[Test](classOf[Test], badSchemaClass) must be_-\/.like {
      case a => a must beAnInstanceOf[ClassNotFoundException]
    }

  def schemaClassIncorrect =
    ThriftPucketDescriptor[Test](classOf[Test], incorrectSchemaClass) must be_-\/.like {
      case a => a must beAnInstanceOf[RuntimeException]
    }
}

object ThriftPucketDescriptorSpec {
  val rng = scala.util.Random
  val badSchemaClass = json("legend.nic.Cage")
  val incorrectSchemaClass = json("com.intenthq.test.model.Test2")
  def json(clazz: String) = s"""{"schemaClass": "$clazz", "compression": "SNAPPY"}"""
  def randomString: Gen[String] = rng.nextString(50)
}
