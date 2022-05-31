import com.sun.jmx.mbeanserver.Util.cast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions._

import scala.xml.dtd.ContentModelParser.S

object Main extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("nasaAPP")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val nasa_file = "C:/Users/marcos.corona/Downloads/access.log"

  val df = (spark.read.format("text")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(nasa_file))

  df.show(10, truncate = false)

  //vamos a definir un esquema para tenerlo más organizado.
  //para entender un poco mejor el uso de regex he consultado la siguiente página:
  //https://www.robertoballester.com/pequeno-manual-sobre-expresiones-regulares-regex/
  //usamos s para encontrar los espacios en blanco.
  //
  val nasa_df = df.select(regexp_extract(col("value"), """(\S+)""", 1).alias("host"),
    regexp_extract(col("value"), """(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("timestamp"),
    regexp_extract(col("value"), """"(\S+) (\S+)\s*(\S+)?\s*"""", 1).alias("method"),
    regexp_extract(col("value"), """"(\S+) (\S+)\s*(\S+)?\s*"""", 2).alias("endpoint"),
    regexp_extract(col("value"), """"(\S+) (\S+)\s*(\S+)?\s*"""", 3).alias("protocol"),
    regexp_extract(col("value"), """\s(\d{3})\s""", 1).cast("integer").alias("status"),
  regexp_extract(col("value"), """\s(\S+)$""", 1).cast("integer").alias("content_size"))
  nasa_df.printSchema()

  //nasa_df.show(10)

  //ahora vamos a limpiar un poco el dataframe:

  val nasa_clean_df = nasa_df.withColumn("host", when(col("host") === "", "Sin especificar").otherwise(col("host")))
  .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss"))
  .withColumn("method", when(col("method") === "", "Sin especificar").otherwise(col("method")))
  .withColumn("endpoint", when(col("endpoint") === "", "Sin especificar").otherwise(col("endpoint")))
  .withColumn("protocol", when(col("protocol") === "", "Sin especificar").otherwise(col("protocol")))

  nasa_clean_df.printSchema()
  //nasa_clean_df.show(5, truncate = true)

  //nos lo guardamos para usar mas adelante en mi caso lo guardare como json y parquet

  /*nasa_clean_df.write.format("json")
  .mode("overwrite")
  .option("compression", "snappy")
  .save("C://Json/nasa.json")

  nasa_clean_df.write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("C://Parquet/nasa.parquet")*/

  //vamos a coger el df limpio desde el parquet guardado:
  val nasa =  spark.read.format("parquet").load("C://Parquet/nasa.parquet")

  nasa.show(5)

  //- ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos
  nasa.select(col("protocol"))
    .distinct()
    .show()

  //- ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
  //para ver cuál es el más común.
  nasa.select(col("status"))
    .groupBy(col("status"))
  .agg(count(col("status")).alias("total"))
  .orderBy(col("total").desc)
  .show()

  //- ¿Y los métodos de petición (verbos) más utilizados?
  nasa.select(col("method"))
    .groupBy(col("method"))
    .agg(count(col("method")).alias("total"))
    .orderBy(col("total").desc)
    .show()

  //- ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
  nasa.select(col("endpoint"),col("content_size"))
  .orderBy(col("content_size").desc)
  .show(1, truncate=false)

  //- Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
  //decir, el recurso con más registros en nuestro log
  nasa.select(col("endpoint")).groupBy(col("endpoint"))
  .agg(count(col("endpoint")).alias("total"))
  .orderBy(col("total").desc)
  .show(1,truncate = false)

  //- ¿Qué días la web recibió más tráfico?

  nasa.select(col("timestamp"))
    .groupBy(col("timestamp")
      .cast("date")
      .alias("Timestamp"))
  .agg(count("*").alias("Trafico"))
  .orderBy(col("Trafico").desc)
  .show(1)

//- ¿Cuáles son los hosts son los más frecuentes?
  nasa.select(col("host")).groupBy(col("host"))
  .agg(count("*").alias("Visitas"))
  .orderBy(desc(col("Visitas")))
  .show(10)



}
