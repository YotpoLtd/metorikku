package com.yotpo.metorikku.input.readers.kafka.deserialize

import java.nio.ByteBuffer

import com.databricks.spark.avro.SchemaConverters.IncompatibleSchemaException
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class AvroToRowSchemaConverter extends Serializable {

  def createConverterToSQL(avroSchema: Schema, sparkSchema: DataType): (GenericRecord) => Row =
    convertToSql(avroSchema, sparkSchema).asInstanceOf[(GenericRecord) => Row]

  // scalastyle:off
  /**
    * Returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
    *                         by user.
    * @param targetSqlType    Target catalyst sql type after the conversion.
    * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    *         taken from private method private[avro] def createConverterToSQL:
    *         https://github.com/databricks/spark-avro/blob/master/src/main/scala/com/databricks/spark/avro/SchemaConverters.scala
    */
  def convertToSql(sourceAvroSchema: Schema,
                   targetSqlType: DataType): Any = {
    def createConverter(avroSchema: Schema,
                        sqlType: DataType, path: List[String]): AnyRef => AnyRef = {
      val avroType = avroSchema.getType
      (sqlType, avroType) match {
        // Avro strings are in Utf8, so we have to call toString on them
        case (StringType, STRING) | (StringType, ENUM) => {
          (item: AnyRef) => if (item == null) null else item.toString
        }
        case (IntegerType, INT) | (BooleanType, BOOLEAN) | (DoubleType, DOUBLE) |
             (FloatType, FLOAT) | (LongType, LONG) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              item
            }
        case (BinaryType, FIXED) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              item.asInstanceOf[Fixed].bytes().clone()
            }
        case (BinaryType, BYTES) =>
          (item: AnyRef) =>
            if (item == null) {
              null
            } else {
              // Byte arrays are reused by avro, so we have to make a copy of them.
              val byteBuffer = item.asInstanceOf[ByteBuffer]
              val bytes = new Array[Byte](byteBuffer.remaining)
              byteBuffer.get(bytes)
              bytes
            }

        case (struct: StructType, RECORD) => {
          val length = struct.fields.length
          val converters = new Array[AnyRef => AnyRef](length)
          val avroFieldIndexes = new Array[Int](length)
          var i = 0
          while (i < length) {
            val sqlField = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = createConverter(avroField.schema(), sqlField.dataType, path :+ sqlField.name)
              converters(i) = converter
              avroFieldIndexes(i) = avroField.pos()
            } else if (!sqlField.nullable) {
              throw new IncompatibleSchemaException(
                s"Cannot find non-nullable field ${sqlField.name} at path ${path.mkString(".")} " +
                  "in Avro schema\n" +
                  s"Source Avro schema: $sourceAvroSchema.\n" +
                  s"Target Catalyst type: $targetSqlType")
            }
            i += 1
          }

          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              val record = item.asInstanceOf[GenericRecord]
              val result = new Array[Any](length)
              var i = 0
              while (i < converters.length) {
                if (converters(i) != null) {
                  val converter = converters(i)
                  result(i) = converter(record.get(avroFieldIndexes(i)))
                }
                i += 1
              }
              new GenericRow(result)
            }
          }
        }
        case (arrayType: ArrayType, ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType,
            path)
          val allowsNull = arrayType.containsNull
          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
                if (element == null && !allowsNull) {
                  throw new RuntimeException(s"Array value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
                } else {
                  elementConverter(element)
                }
              }
            }
          }
        case (mapType: MapType, MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          val allowsNull = mapType.valueContainsNull
          (item: AnyRef) => {
            if (item == null) {
              null
            } else {
              item.asInstanceOf[java.util.Map[AnyRef, AnyRef]].asScala.map { x =>
                if (x._2 == null && !allowsNull) {
                  throw new RuntimeException(s"Map value at path ${path.mkString(".")} is not " +
                    "allowed to be null")
                } else {
                  (x._1.toString, valueConverter(x._2))
                }
              }.toMap
            }
          }
        case (sqlType, UNION) => {
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (remainingUnionTypes.size == 1) {
              createConverter(remainingUnionTypes.head, sqlType, path)
            } else {
              createConverter(Schema.createUnion(remainingUnionTypes.asJava), sqlType, path)
            }
          } else avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) => createConverter(avroSchema.getTypes.get(0), sqlType, path)
            case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sqlType == LongType =>
              (item: AnyRef) => {
                item match {
                  case null => null
                  case l: java.lang.Long => l
                  case i: java.lang.Integer => new java.lang.Long(i.longValue())
                }
              }
            case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && sqlType == DoubleType =>
              (item: AnyRef) => {
                item match {
                  case null => null
                  case d: java.lang.Double => d
                  case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                }
              }
            case other =>
              sqlType match {
                case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                  if (t.fields.length == 0) {
                    (item: AnyRef) => new GenericRow(null)
                  }
                  else {
                    val fieldConverters = t.fields.zip(avroSchema.getTypes.asScala).map {
                      case (field, schema) =>
                        createConverter(schema, field.dataType, path :+ field.name)
                    }
                    (item: AnyRef) =>
                      if (item == null) {
                        null
                      } else {
                        val i = GenericData.get().resolveUnion(avroSchema, item)
                        val converted = new Array[Any](fieldConverters.length)
                        converted(i) = fieldConverters(i)(item)
                        new GenericRow(converted)
                      }
                  }


                case _ => throw new IncompatibleSchemaException(
                  s"Cannot convert Avro schema to catalyst type because schema at path " +
                    s"${path.mkString(".")} is not compatible " +
                    s"(avroType = $other, sqlType = $sqlType). \n" +
                    s"Source Avro schema: $sourceAvroSchema.\n" +
                    s"Target Catalyst type: $targetSqlType")
              }
          }
        }
        case (left, right) =>
          throw new IncompatibleSchemaException(
            s"Cannot convert Avro schema to catalyst type because schema at path " +
              s"${path.mkString(".")} is not compatible (avroType = $left, sqlType = $right). \n" +
              s"Source Avro schema: $sourceAvroSchema.\n" +
              s"Target Catalyst type: $targetSqlType")
      }
    }

    createConverter(sourceAvroSchema, targetSqlType, List.empty[String])
  }

  // scalastyle:on

}
