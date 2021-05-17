# Detection of malformed records with Apache Spark

Data in the CSV files is stored in a text form with no information on types. When you read data from CSV files with Apache Spark and you want to have properly typed data, you can tell Spark to infer types automatically. The `inferSchema` option can be used for this: 

```scala
def load(dataLocation: String)(implicit spark: SparkSession): DataFrame = {
  spark.read
    .option("inferSchema", "true")
    .csv(dataLocation)
}
```

Unfortunately, schema inference does not always produce good results, especially when there are columns that contain only null values or when the dataset is too small and does not have enough values to correctly infer types.

Alternatively, you can provide a schema when loading data from CSV. Sometimes there is a possibility that the data does not match the schema. The most common case is that the number of columns in the DataFrame does not match the number of schema fields. This is not difficult to verify. But what if only some of the types in the schema do not correspond to the values in the DataFrame? We can easily check this when the DataFrame contains only a few columns and when the entire column cannot be cast to the type defined in the schema. However, there may be cases when only a subset of the records are affected by the problem. Of course we can use the built-in Spark functionality and load data in different available modes: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`. The first mode is used by default and only silently replaces values that do not match the target type with zeros. Another option is to use `DROPMALFORMED`. In this case, all records that are incompatible with the schema are silently deleted. The last option is `FAILFAST`, which causes the job to fail if the schema is incompatible. Unfortunately, if the job fails, it gives no indication of the column that caused the failure.

Hence, there are two problems that these modes cannot solve: give us information about malformed rows and tell us exactly which columns are incompatible with the schema. The first problem is easy to solve. We can only use the following approach:


```scala
def findMalformedRecords(dataLocation: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
  val fullDf = spark.read
    .csv(dataLocation)
    
  val validDf = spark.read
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .csv(dataLocation)
    
  fullDf.except(validDf)
}
```

With this function we can get a DataFrame with all the malformed records. This code is very simple, but it works quite slowly and, more importantly, it cannot tell us which columns contain invalid data. To solve this problem, I decided to write a Spark job. The code is available below:

```scala
package io.github.dyaraev.example.spark

import SchemaValidator.{FieldInfo, SchemaValidationResult}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

object SchemaValidator {

  private val DefaultTypedFieldSuffix = "_TYPED"
  private val DefaultMalformedFieldsColumn = "malformed_fields"

  def apply(): SchemaValidator = {
    new SchemaValidator(DefaultTypedFieldSuffix, DefaultMalformedFieldsColumn)
  }

  def apply(typedFieldSuffix: String, malformedFieldsColumn: String): SchemaValidator = {
    new SchemaValidator(typedFieldSuffix, malformedFieldsColumn)
  }

  case class FieldInfo(untypedName: String, typedName: String, dataType: DataType)

  case class TestRecord(c1: String, c2: String, c3: String, c4: String, c5: String)

  case class ValidationResult private(malformedDf: DataFrame, malformedFields: Seq[String])
}

class SchemaValidator(typedFieldSuffix: String, malformedFieldsColumn: String) extends Serializable {

  def findMalformed(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[ValidationResult] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(
        s"Unable to map ${schema.fields.length} schema fields to dataframe containing ${df.schema.fields.length} columns"
      )
    }

    val malformedDf = analyzeData(df, prepareFieldsInfo(schema)).filter(size(col(malformedFieldsColumn)) > 0)
    val malformedFieldsSet = collectMalformedColumns(malformedDf).toSet
    val malformedFields = schema.names.filter(malformedFieldsSet.contains) // preserve columns ordering
    ValidationResult(selectAffectedColumns(malformedDf, malformedFields), malformedFields)

    val malformedColumns = collectMalformedColumns(malformedDf)
    ValidationResult(selectAffectedColumns(malformedDf, malformedColumns), malformedColumns)
  }

  private def collectMalformedColumns(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._
    malformedDf
      .select(explode(col(malformedFieldsColumn)))
      .distinct()
      .as[String]
      .collect()
  }

  private def analyzeData(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    val typedDf = addTypedColumns(df, fieldsInfo)
    typedDf.map(compareFields(fieldsInfo))(createRowEncoder(typedDf.schema))
  }

  private def selectAffectedColumns(malformedDf: DataFrame, malformedColumns: Seq[String]): DataFrame = {
    val affectedColumns = malformedColumns.flatMap(column => Seq(column, column + typedFieldSuffix))
    malformedDf.selectExpr(affectedColumns :+ malformedFieldsColumn: _*)
  }

  private def compareFields(fieldsInfo: Seq[FieldInfo])(row: Row): Row = {
    val corruptedColumns = fieldsInfo.flatMap { fieldInfo =>
      val untypedFieldIndex = row.fieldIndex(fieldInfo.untypedName)
      val typedFieldIndex = row.fieldIndex(fieldInfo.typedName)
      if (row.isNullAt(untypedFieldIndex) == row.isNullAt(typedFieldIndex)) {
        None
      } else {
        Some(fieldInfo.untypedName)
      }
    }
    Row.fromSeq(row.toSeq :+ corruptedColumns)
  }

  private def createRowEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder.apply(StructType(schema.fields :+ StructField(malformedFieldsColumn, ArrayType(StringType))))
  }

  private def prepareFieldsInfo(schema: StructType): Seq[FieldInfo] = {
    schema.fields.map(f => FieldInfo(f.name, f.name + typedFieldSuffix, f.dataType))
  }

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) =>
        df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }
}

```

The logic of the job is simple. It loads the data as plain strings and then adds a typed column for each of the existing columns. The types are taken from the provided schema, and the built-in `cast` function is used for creating typed columns. So we have pairs of columns. It's worth noting that any values in a column that are incompatible with the type are replaced with null values. Now all we have to do is check which values have been replaced with nulls. 

Let's now test the validator. As input data we are going to use the following dataset.

```
+----+---+---+---+---+
|c1  |c2 |c3 |c4 |c5 |
+----+---+---+---+---+
|null|E  |S  |A  |B  |
|null|E  |S  |C  |D  |
|null|E  |J  |E  |F  |
|null|K  |K  |G  |H  |
|null|O  |K  |A1 |B1 |
|/   |E  |Q  |C1 |B1 |
|null|1  |8  |11 |12 |
|null|2  |9  |22 |23 |
|null|3  |0  |33 |34 |
|null|4  |5  |44 |45 |
+----+---+---+---+---+
```

The schema is represented by five fields:

```
root
 |-- col1: long (nullable = true)
 |-- col2: long (nullable = true)
 |-- col3: long (nullable = true)
 |-- col4: string (nullable = true)
 |-- col5: string (nullable = true)
```

Not all of the rows in the dataset can be mapped to the schema. This is how the result DataFrame looks like:

```
+----+----------+----+----------+----+----------+------------------+
|col1|col1_TYPED|col2|col2_TYPED|col3|col3_TYPED|malformed_fields  |
+----+----------+----+----------+----+----------+------------------+
|null|null      |E   |null      |S   |null      |[col2, col3]      |
|null|null      |E   |null      |S   |null      |[col2, col3]      |
|null|null      |E   |null      |J   |null      |[col2, col3]      |
|null|null      |K   |null      |K   |null      |[col2, col3]      |
|null|null      |O   |null      |K   |null      |[col2, col3]      |
|/   |null      |E   |null      |Q   |null      |[col1, col2, col3]|
+----+----------+----+----------+----+----------+------------------+
```

As you can see, there are pairs of columns where every other column has the suffix `_TYPED`. The prefix can be changed if there is a chance that one of the columns in the original DataFrame could have a name ending with that suffix. The last column contains a list of bad columns for each row. 

On the real data the code above works pretty slowly. The DAG shows that two additional steps are required to deserialize and serialize values in `typedDf.map(...)`. Ok, how can we optimize it? First of all, it's always better to use the built-in Spark functions, which allow the Catalyst engine to optimize job execution. Let's check how we can rewrite it using these functions:

```scala
package io.github.dyaraev.example.spark

import SchemaValidator.{FieldInfo, ValidationResult}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

class SchemaValidator(typedFieldSuffix: String, malformedFieldsColumn: String) extends Serializable {

  def findMalformed(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[ValidationResult] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(
        s"Unable to map ${schema.fields.length} schema fields to dataframe containing ${df.schema.fields.length} columns"
      )
    }

    val malformedDf = analyzeData(df, prepareFieldsInfo(schema)).filter(size(col(malformedFieldsColumn)) > 0)
    val malformedFieldsSet = collectMalformedColumns(malformedDf).toSet
    val malformedFields = schema.names.filter(malformedFieldsSet.contains) // preserve columns ordering
    ValidationResult(selectAffectedColumns(malformedDf, malformedFields), malformedFields)
  }

  private def collectMalformedColumns(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._
    malformedDf
      .select(explode(col(malformedFieldsColumn)))
      .distinct()
      .as[String]
      .collect()
  }

  private def analyzeData(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    addTypedColumns(df, fieldsInfo).withColumn(malformedFieldsColumn, compareFields(fieldsInfo))
  }

  private def selectAffectedColumns(malformedDf: DataFrame, malformedColumns: Seq[String]): DataFrame = {
    val affectedColumns = malformedColumns.flatMap(column => Seq(column, column + typedFieldSuffix))
    malformedDf.selectExpr(affectedColumns :+ malformedFieldsColumn: _*)
  }

  private def compareFields(fieldsInfo: Seq[FieldInfo]): Column = {
    val expressions = fieldsInfo.map {
      case FieldInfo(untypedName, typedName, _) =>
        when(col(untypedName).isNull =!= col(typedName).isNull, lit(untypedName)).otherwise(lit(""))
    }
    array_remove(array(expressions: _*), "")
  }

  private def prepareFieldsInfo(schema: StructType): Seq[FieldInfo] = {
    schema.fields.map(f => FieldInfo(f.name, f.name + typedFieldSuffix, f.dataType))
  }

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) =>
        df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }
}
```

This version works at least twice as fast. Of course, the job is not fully optimized, and it is probably worth repartitioning the data or persisting intermediate results. It was not our goal to apply all of these optimizations, but to test the idea of how we can get information about malformed records/columns in a DataFrame. The code was executed with Apache Spark 2.4.7.  
