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
object SchemaValidator {

  private val TypedColumnSuffix = "_TYPED"
  private val ValidationInfoColumn = "validation_info"

  case class FieldInfo(untypedName: String, typedName: String, dataType: DataType)

  case class SchemaValidationResult(df: DataFrame, malformedColumns: Seq[String])

  def apply(): SchemaValidator = SchemaValidator(TypedColumnSuffix, ValidationInfoColumn)
}

case class SchemaValidator(typedColumnSuffix: String, validationInfoColumn: String) {

  def findMalformedRecords(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[SchemaValidationResult] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(s"Unable to map ${schema.fields.length} schema fields to dataframe containing ${df.schema.fields.length} columns")
    }

    val combinedFieldsInfo = combineFieldsInfo(schema)
    val combinedDf = addTypedColumns(df, combinedFieldsInfo)

    val malformedDf = combinedDf
      .map(compareFields(combinedFieldsInfo))(createRowEncoder(combinedDf.schema))
      .filter(size(col(validationInfoColumn)) > 0)

    val malformedColumns = collectMalformedColumns(malformedDf)

    SchemaValidationResult(
      malformedDf.selectExpr(malformedColumns.flatMap(c => Seq(c, c + typedColumnSuffix)) :+ validationInfoColumn: _*),
      malformedColumns
    )
  }

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) => df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }

  private def collectMalformedColumns(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._

    malformedDf
      .select(explode(col(validationInfoColumn)).as(validationInfoColumn))
      .distinct()
      .as[String]
      .collect()
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
    RowEncoder.apply(StructType(schema.fields :+ StructField(validationInfoColumn, ArrayType(StringType))))
  }

  private def combineFieldsInfo(schema: StructType): Seq[FieldInfo] = {
    schema.fields.map(field => FieldInfo(field.name, field.name + typedColumnSuffix, field.dataType))
  }
}
```

The logic of the job is simple. It loads the data as plain strings and then adds a typed column for each of the existing columns. The types are taken from the provided schema, and the built-in `cast` function is used for creating typed columns. So we have pairs of columns. It's worth noting that any values in a column that are incompatible with the type are replaced with null values. Now all we have to do is check which values have been replaced with nulls. This is how the result DataFrame looks like:

```
+----+----------+----+----------+----+----------+------------------+
|col1|col1_TYPED|col2|col2_TYPED|col3|col3_TYPED|validation_info   |
+----+----------+----+----------+----+----------+------------------+
|null|null      |E   |null      |S   |null      |[col2, col3]      |
|null|null      |E   |null      |S   |null      |[col2, col3]      |
|null|null      |E   |null      |J   |null      |[col2, col3]      |
|null|null      |K   |null      |K   |null      |[col2, col3]      |
|null|null      |O   |null      |K   |null      |[col2, col3]      |
|/   |null      |E   |null      |Q   |null      |[col1, col2, col3]|
+----+----------+----+----------+----+----------+------------------+
```

The code above works pretty slowly. The DAG shows that two additional steps are required to deserialize and serialize values in `combinedDf.map(...)`. Ok how can we optimize it? First of all, it's always better to use the built-in Spark functions, which allow the Catalyst engine to optimize job execution. Let's check how we can rewrite it using these functions:

```scala
object SchemaValidator {

  private val TypedColumnSuffix = "_TYPED"
  private val ValidationInfoColumn = "validation_info"

  case class FieldInfo(untypedName: String, typedName: String, dataType: DataType)

  case class SchemaValidationResult(df: DataFrame, malformedColumns: Seq[String])

  def apply(): SchemaValidator = SchemaValidator(TypedColumnSuffix, ValidationInfoColumn)
}

case class SchemaValidator(typedColumnSuffix: String, validationInfoColumn: String) {

  def findMalformedRecords(df: DataFrame, schema: StructType)(implicit spark: SparkSession): Try[SchemaValidationResult] = Try {
    if (df.schema.fields.length != schema.fields.length) {
      throw new RuntimeException(s"Unable to map ${schema.fields.length} schema fields to dataframe containing ${df.schema.fields.length} columns")
    }

    val combinedFieldsInfo = combineFieldsInfo(schema)
    
    val malformedDf = addTypedColumns(df, combinedFieldsInfo)
      .withColumn(validationInfoColumn, comparedFields(combinedFieldsInfo))
      .filter(size(col(validationInfoColumn)) > 0)

    val malformedColumns = collectMalformedColumns(malformedDf)

    SchemaValidationResult(
      malformedDf.selectExpr(malformedColumns.flatMap(c => Seq(c, c + typedColumnSuffix)) :+ validationInfoColumn: _*),
      malformedColumns
    )
  }  

  private def collectMalformedColumns(malformedDf: DataFrame)(implicit spark: SparkSession): Seq[String] = {
    import spark.implicits._

    malformedDf
      .select(explode(col(validationInfoColumn)).as(validationInfoColumn))
      .distinct()
      .as[String]
      .collect()
  }

  private def addTypedColumns(df: DataFrame, fieldsInfo: Seq[FieldInfo]): DataFrame = {
    fieldsInfo.foldLeft(df.toDF(fieldsInfo.map(_.untypedName): _*)) {
      case (df, FieldInfo(untypedName, typedName, dataType)) => df.withColumn(typedName, col(untypedName).cast(dataType))
    }
  }  

  private def compareFields(fieldsInfo: Seq[FieldInfo]): Column = {
    val expressions = fieldsInfo.map {
      case FieldInfo(untypedName, typedName, _) => when(col(untypedName).isNull =!= col(typedName).isNull, lit(untypedName)).otherwise(lit(""))
    }
    array_remove(functions.array(expressions: _*), "")
  }

  private def combineFieldsInfo(schema: StructType): Seq[FieldInfo] = {
    schema.fields.map(field => FieldInfo(field.name, field.name + typedColumnSuffix, field.dataType))
  }
}
```

This version works at least twice as fast. Of course, the job is not fully optimized, and it is probably worth repartitioning the data or persisting intermediate results. It was not our goal to apply all of these optimizations, but to test the idea of how we can get information about malformed records/columns in a DataFrame. The code was executed with Apache Spark 2.4.7.  
