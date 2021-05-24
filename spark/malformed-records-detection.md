# Detection of malformed records with Apache Spark

## Some title

Apache Spark supports reading data from a variety of formats. Some of them already contain information about the schema and in particular data types. However delimiter separated formats such as CSV don't have any information about the fields except maybe field names which can be included in the header. Nevertheless when we read the data we want to be sure that its structure matches to what we expect, so we can process it correspondingly. There are two ways how we can convert untyped dataset, which is represented by string values, to a typed structure: we can ask Spark to infer the schema or we can provide a schema explicitly. Unfortunately, in some cases our datasets may contain malformed records. As a result, using automatic type inference or providing a schema is not always enough for getting a clean dataset we can work with. Now, let's take a look what problems we can expect working with such datasets and how we detect malformed records.

## Automatic type inference

When you read data from CSV files with Apache Spark and you want to have properly typed data, you can tell Spark to infer types automatically. The `inferSchema` option can be used for this: 

```scala
package examples

import org.apache.spark.sql.{DataFrame, SparkSession}

object SchemaValidator {

  def load(dataLocation: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .csv(dataLocation)
  }
}
```

Unfortunately, schema inference does not always produce good results, especially when there are columns that contain only null values or when the dataset is too small and does not have enough values to correctly infer types. Let's check an example. In a perfect world we would have a valid dataset and the correct schema would be inferred by Spark but now we are interested in corner cases and how to deal with them. That is why our CSV has several different problems, such as different number of fields in rows, columns without values, wrong types:

```csv
,1,,E,1,A,B
,,,E,2,1,D
,,,E2,5,2,F
,,,K1,1,3,H
,,,O,2,4,B1
,,/,E,2,B,B1
,,/,N,4,6,B1,B2
,,/,N,1,7
,2,,1,8,1,12
,,,2,9,2,23
,3,,3,0,C,34
,,,4,5,4,45
,,,4,5,5
,,,4,5,6,C4,68
```

This is a table representation of the dataset:

```
|   |  1|   |  E|  1|  A|  B|
|   |   |   |  E|  2|  1|  D|
|   |   |   | E2|  5|  2|  F|
|   |   |   | K1|  1|  3|  H|
|   |   |   |  O|  2|  4| B1|
|   |   |  /|  E|  2|  B| B1|
|   |   |  /|  N|  4|  6| B1| B2|
|   |   |  /|  N|  1|  7|
|   |  2|   |  1|  8|  1| 12|
|   |   |   |  2|  9|  2| 23|
|   |  3|   |  3|  0|  C| 34|
|   |   |   |  4|  5|  4| 45|
|   |   |   |  4|  5|  5|
|   |   |   |  4|  5|  6| C4| 68|
```

Now we need to define a schema:

```
root
 |-- _c0: integer (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: long (nullable = true)
 |-- _c3: long (nullable = true)
 |-- _c4: long (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
```

As we see, the columns `_c2` and `_c3` contain wrong values. There also two rows that contain less fields and two rows that contain more fields than specified in the schema. We can check how Spark will infer schema for this dataset.

```
root
 |-- _c0: string (nullable = true)
 |-- _c1: integer (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
```

Ok, what is wrong in the inferred schema:
* `_c0` - string instead of integer because there is no information for type inference
* `_c1` - integer instead of string because information for type inference is not enough
* `_c2`, `_c3` - string instead of long because the malformed records prevent Spark from inferring the correct type
* `_c4` - integer instead long because information for type inference is not enough

There is also a possibility that Spark identifies number of column incorrectly because it does not know how many columns we expect to have in the dataset and simply looks at the first rows. If we want to load data in the `DROPMALFORMED` and we have a wrong number of columns in the inferred schema, we most probably will loose all the valid records. Now is the time to remember what modes we have in Spark and how the dataset will be loaded in each of the modes.

`PERMISSIVE` mode:

|_c0 |_c1 |_c2 |_c3|_c4|_c5|_c6 |
|----|----|----|---|---|---|----|
|_null_|1   |_null_|E  |1  |A  |B   |
|_null_|_null_|_null_|E  |2  |1  |D   |
|_null_|_null_|_null_|E2 |5  |2  |F   |
|_null_|_null_|_null_|K1 |1  |3  |H   |
|_null_|_null_|_null_|O  |2  |4  |B1  |
|_null_|_null_|/   |E  |2  |B  |B1  |
|_null_|_null_|/   |N  |4  |6  |B1  |
|_null_|_null_|/   |N  |1  |7  |_null_|
|_null_|2   |_null_|1  |8  |1  |12  |
|_null_|_null_|_null_|2  |9  |2  |23  |
|_null_|3   |_null_|3  |0  |C  |34  |
|_null_|_null_|_null_|4  |5  |4  |45  |
|_null_|_null_|_null_|4  |5  |5  |_null_|
|_null_|_null_|_null_|4  |5  |6  |C4  |

`DROPMALFORMED` mode:

|_c0 |_c1 |_c2 |_c3|_c4|_c5|_c6|
|----|----|----|---|---|---|---|
|_null_|1   |_null_|E  |1  |A  |B  |
|_null_|_null_|_null_|E  |2  |1  |D  |
|_null_|_null_|_null_|E2 |5  |2  |F  |
|_null_|_null_|_null_|K1 |1  |3  |H  |
|_null_|_null_|_null_|O  |2  |4  |B1 |
|_null_|_null_|/   |E  |2  |B  |B1 |
|_null_|2   |_null_|1  |8  |1  |12 |
|_null_|_null_|_null_|2  |9  |2  |23 |
|_null_|3   |_null_|3  |0  |C  |34 |
|_null_|_null_|_null_|4  |5  |4  |45 |

Of course in the `FAILFAST` mode we don't get any result, only the following exception:

```
org.apache.spark.SparkException: Malformed records are detected in record parsing. Parse Mode: FAILFAST..
```

The outcome is simple. Schema inference doesn't work well on malformed datasets. If we expect malformed datasets it is always better to explicitly provide a schema. 

## Explicit schema provision in Apache Spark 

Alternatively, you can provide a schema when loading data from CSV. Sometimes there is a possibility that the data does not match the schema. The most common case is that the number of columns in the DataFrame does not match the number of schema fields. This is not difficult to verify. But what if only some of the types in the schema do not correspond to the values in the DataFrame? We can easily check this when the DataFrame contains only a few columns and when the entire column cannot be cast to the type defined in the schema. However, there may be cases when only a subset of the records are affected by the problem. Of course we can use the built-in Spark functionality and load data in different available modes: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`. The first mode is used by default and only silently replaces values that do not match the target type with zeros. Another option is to use `DROPMALFORMED`. In this case, all records that are incompatible with the schema are silently deleted. The last option is `FAILFAST`, which causes the job to fail if the schema is incompatible. Unfortunately, if the job fails, it gives no indication of the column that caused the failure.

_**IMPORTANT (from the Apache Spark documentation):**_
> In version 2.3 and earlier, CSV rows are considered as malformed if at least one column value in the row is malformed. CSV parser dropped such rows in the `DROPMALFORMED` mode or outputs an error in the `FAILFAST` mode. Since Spark 2.4, CSV row is considered as malformed only when it contains malformed column values requested from CSV datasource, other values can be ignored. As an example, CSV file contains the “id,name” header and one row “1234”. In Spark 2.4, selection of the id column consists of a row with one column value 1234 but in Spark 2.3 and earlier it is empty in the `DROPMALFORMED` mode. To restore the previous behavior, set `spark.sql.csv.parser.columnPruning.enabled` to `false`.

>In Spark version 2.4 and below, CSV datasource converts a malformed CSV string to a row with all nulls in the `PERMISSIVE` mode. In Spark 3.0, the returned row can contain non-null fields if some of CSV column values were parsed and converted to desired types successfully.

Hence, there are two problems that these modes cannot solve: give us information about malformed rows and tell us exactly which columns are incompatible with the schema using the following approach:

**[TBD]**

With this function we can get a DataFrame with all the malformed records. This code is very simple, but it works quite slowly and, more importantly, it cannot tell us which columns contain invalid data. In some cases, incorrect results are also possible, for example, if the precision of floating point numbers changes as a result of applying the schema. After that, the string stored in the intermediate dataset will differ from the original one. To solve these problems, we can write another Spark job. The code is available below:

```scala
package examples

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

class SchemaValidator(typedColumnSuffix: String, malformedFieldsColumn: String) extends Serializable {

  private case class FieldInfo(untypedName: String, typedName: String, dataType: DataType)

  def findMalformed(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[DataFrame] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(
        s"Unable to map ${schema.fields.length} schema fields to ${df.schema.fields.length} DataFrame columns"
      )
    }

    val malformedDf = analyzeData(df, schema).filter(size(col(malformedFieldsColumn)) > 0)
    val malformedFields = collectMalformedFields(malformedDf)
    selectAffectedFields(malformedDf, malformedFields)
  }

  private def collectMalformedFields(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._
    val malformedFields = malformedDf
      .select(explode(col(malformedFieldsColumn)))
      .distinct()
      .as[String]
      .collect()
      .toSet

    malformedDf.schema.names.filter(malformedFields.contains) // preserve columns order
  }

  private def analyzeData(df: DataFrame, schema: StructType): DataFrame = {
    val fieldsInfo = schema.fields.map(f => FieldInfo(f.name, f.name + typedColumnSuffix, f.dataType))
    val typedDf = addTypedColumns(df, fieldsInfo)
    val newSchema = StructType(typedDf.schema.fields :+ StructField(malformedFieldsColumn, ArrayType(StringType)))
    typedDf.map(compareFields(fieldsInfo))(RowEncoder.apply(newSchema))
  }

  private def selectAffectedFields(malformedDf: DataFrame, malformedColumns: Seq[String]): DataFrame = {
    val affectedColumns = malformedColumns.flatMap(c => Seq(c, c + typedColumnSuffix))
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

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) =>
        df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }
}
```

The logic of the job is simple. It loads the data as plain strings and then adds a typed column for each of the existing columns. The types are taken from the provided schema, and the built-in `cast` function is used for creating typed columns. So we have pairs of columns. It's worth noting that any values in a column that are incompatible with the type are replaced with null values. Now all we have to do is check which values have been replaced with nulls. 

Let's now test the validator. As input data we are going to use the following dataset:

```
+----+---+---+---+---+
| _c0|_c1|_c2|_c3|_c4|
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

Obviously, not all of the rows in the dataset can be mapped to the schema. This is how the result DataFrame looks like:

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

As you can see, there are pairs of columns where every other column has the suffix `_TYPED`. The suffix can be changed using a constructor parameter if there is a chance that one of the columns in the original DataFrame could have a name ending with that suffix. The last column contains a list of bad columns for each row. Its name can also be changed.

This approach also has some limitations. At first missing fields are ignored. The data must be loaded in `PERMISSIVE` mode. As a result, if a row contains fewer fields, missing fields with null values are added to the end of the row.Additional fields are also not taken into account. If some rows contain more fields than the schema, those fields are simply ignored. 

On the real data the code above works pretty slowly. The DAG shows that two additional steps are required to deserialize and serialize values in `typedDf.map(...)`. Ok, how can we optimize it? First of all, it's always better to use the built-in Spark functions, which allow the Catalyst engine to optimize job execution. Let's check how we can rewrite it using these functions:

```scala
package examples

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class SchemaValidator(typedColumnSuffix: String, malformedFieldsColumn: String) extends Serializable {

  private case class FieldInfo(untypedName: String, typedName: String, dataType: DataType)

  def findMalformed(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[DataFrame] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(
        s"Unable to map ${schema.fields.length} schema fields to ${df.schema.fields.length} DataFrame columns"
      )
    }

    val malformedDf = analyzeData(df, schema).filter(size(col(malformedFieldsColumn)) > 0)
    val malformedFields = collectMalformedFields(malformedDf)
    selectAffectedFields(malformedDf, malformedFields)
  }

  private def collectMalformedFields(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._
    val malformedFields = malformedDf
      .select(explode(col(malformedFieldsColumn)))
      .distinct()
      .as[String]
      .collect()
      .toSet

    malformedDf.schema.names.filter(malformedFields.contains) // preserve columns ordering
  }

  private def analyzeData(df: DataFrame, schema: StructType): DataFrame = {
    val fieldsInfo = schema.fields.map(f => FieldInfo(f.name, f.name + typedColumnSuffix, f.dataType))
    addTypedColumns(df, fieldsInfo).withColumn(malformedFieldsColumn, compareFields(fieldsInfo))
  }

  private def selectAffectedFields(malformedDf: DataFrame, malformedColumns: Seq[String]): DataFrame = {
    val affectedColumns = malformedColumns.flatMap(c => Seq(c, c + typedColumnSuffix))
    malformedDf.selectExpr(affectedColumns :+ malformedFieldsColumn: _*)
  }

  private def compareFields(fieldsInfo: Seq[FieldInfo]): Column = {
    val expressions = fieldsInfo.map {
      case FieldInfo(untypedName, typedName, _) =>
        when(col(untypedName).isNull =!= col(typedName).isNull, lit(untypedName)).otherwise(lit(""))
    }
    array_remove(array(expressions: _*), "")
  }

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) =>
        df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }
}
```

This version works at least twice as fast. Of course, the job is not fully optimized, and it is probably worth repartitioning the data or persisting intermediate results. It was not our goal to apply all of these optimizations, but to test the idea of how we can get information about malformed records/columns in a DataFrame. The code was executed with Apache Spark 2.4.7 and 3.1.1.  


