package com.intenthq.pucket.avro

import com.intenthq.pucket.avro.test.{AvroTest, AvroTest2}
import org.json4s.ParserUtil.ParseException
import org.scalacheck.{Prop, Gen}
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DisjunctionMatchers


class AvroPucketDescriptorSpec extends Specification with DisjunctionMatchers with ScalaCheck {
  import AvroTestUtils._
  import AvroPucketDescriptorSpec._

  def is =
    s2"""
       The Pucket Descriptor
         Can serialise and deserialise from JSON $fromJson
         Fails to deserialise a non-json string $badString
         Fails when avro schema isn't valid $badSchema
         Fails when schema is not for the expected type $schemaClassIncorrect
      """

  def fromJson =
    Prop.forAll(descriptorGen) { d =>
      AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema, d.toString) must be_\/-.like {
        case a =>
          (a.schema === d.schema) and
          (a.compression === d.compression) and
          (a.partitioner === d.partitioner)
      }
    }
  
  def badString =
    Prop.forAll(randomString) { s =>
      AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema, s) must be_-\/[Throwable].like {
        case a => a must beAnInstanceOf[ParseException]
      }
    }

  def badSchema =
    AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema, badSchemaString) must be_-\/.like {
      case a => a must beAnInstanceOf[ParseException]
    }

  def schemaClassIncorrect =
    AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema, incorrectSchema) must be_-\/.like {
      case a => a must beAnInstanceOf[RuntimeException]
    }
}

object AvroPucketDescriptorSpec {
  val rng = scala.util.Random
  val badSchemaString = json("Nic Cage is the best actor of all time")
  val incorrectSchema = json(AvroTest2.getClassSchema.toString.replace(""""""", """\""""))
  def json(clazz: String) = s"""{"avroSchema": "$clazz", "compression": "SNAPPY"}"""
  def randomString: Gen[String] = rng.nextString(50)
}
