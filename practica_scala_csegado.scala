// Databricks notebook source
import org.apache.spark.sql.SparkSession
val sc = spark.sparkContext



// COMMAND ----------

val dfreport = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/section4/data/world_happiness_report.csv")
display(dfreport)

// COMMAND ----------

dfreport.printSchema

// COMMAND ----------

val df2021 = spark.read.option("header", "true").csv("dbfs:/FileStore/section4/data/world_happiness_report_2021.csv")
display(df2021)


// COMMAND ----------

df2021.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 1 
// MAGIC
// MAGIC ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score”
// MAGIC mayor número más feliz es el país)
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions.{col, max}
// cambiar el tipo de dato de Ladder score ya que es string
val df2021_v2 = df2021.withColumn("Ladder score2", col("Ladder score").cast("Double"))


// Encontrar puntuacion máxima
val puntMaxima = df2021_v2.select(max("Ladder score2").cast("double")).collect()(0)(0)
//isplay(puntmaxima)

//seleccionar el pais más feliz
val paisMasFeliz = df2021_v2.filter(col("Ladder score2") === puntMaxima).select("Country name").first()

// Mostrar el resultado
println(s"El país más feliz en 2021 es $paisMasFeliz con una puntuación de $puntMaxima.")






// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 2
// MAGIC
// MAGIC ¿Cuál es el país más “feliz” del 2021 por continente según la data?
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions.{col, max}
// cambiar el tipo de dato de Ladder score ya que es string
val df2021_v2 = df2021.withColumn("Ladder score2", col("Ladder score").cast("Double"))


// Encontrar puntuacion máxima
val puntMaximaByRegion = df2021_v2.groupBy("Regional indicator").agg(max(col("Ladder score2")).as("Max Ladder Score"))


//seleccionar el pais más feliz por continente
val paisMasFelizByRegion = df2021_v2.join(puntMaximaByRegion, Seq("Regional indicator"), "inner")
  .where(col("Ladder score2") === col("Max Ladder Score"))
  .select("Regional indicator", "Country name", "Max Ladder Score")

// Mostrar el resultado
paisMasFelizByRegion.orderBy("Regional indicator","Max Ladder Score").show()



// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 3
// MAGIC
// MAGIC ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

import org.apache.spark.sql.functions.{col, max, count, desc, lit}

// Cambiar el tipo de dato de Ladder score ya que es string para 2021
val df2021_v2 = df2021.withColumn("Ladder score2", col("Ladder score").cast("Double")).withColumn("year", lit("2021"))

// Encontrar puntuación máxima para 2021
val puntMaximaByCountry2021 = df2021_v2.groupBy("year").agg(max(col("Ladder score2")).as("Max Ladder Score"))

// Seleccionar el país más feliz para 2021
val paisMasFelizByCountry2021 = df2021_v2.join(puntMaximaByCountry2021, Seq("year"), "inner")
  .where(col("Ladder score2") === col("Max Ladder Score"))
  .select("Country name", "Max Ladder Score")

// Cambiar el tipo de dato de Life Ladder ya que es string para todos los años
val dfreport_v2 = dfreport.withColumn("Ladder score2", col("Life Ladder").cast("Double"))


// Encontrar puntuación máxima para todos los años
val puntMaximaByCountryAll = dfreport_v2.groupBy("year").agg(max(col("Ladder score2")).as("Max Ladder Score"))

// Seleccionar el país más feliz para todos los años
val paisMasFelizByCountryAll = dfreport_v2.join(puntMaximaByCountryAll, Seq("year"), "inner")
  .where(col("Ladder score2") === col("Max Ladder Score"))
  .select("Country name", "Max Ladder Score")

// Unir los resultados en un solo DataFrame
val resultadoFinal = paisMasFelizByCountry2021.union(paisMasFelizByCountryAll)

// Contar la frecuencia de aparición de cada país en el DataFrame unido
val conteoPaises = resultadoFinal.groupBy("Country name").agg(count("*").as("Nº de veces"))

// Encontrar el país que aparece más veces
val numeroMaximo = conteoPaises.agg(max("Nº de veces")).first().getLong(0)
val paisMasFrecuente = conteoPaises.filter(col("Nº de veces") === numeroMaximo)

// Mostrar el resultado
paisMasFrecuente.show()





// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 4
// MAGIC
// MAGIC  ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


// cambiar el tipo de dato de Life Ladder y GDP ya que son string
val dfreport_v2 = dfreport.withColumn("Life Ladder", col("Life Ladder").cast("Double")).withColumn("Log GDP per capita",col("Log GDP per capita").cast("Double"))
// filtrar por año
val df2020 = dfreport_v2.filter(col("year")==="2020")

//añadir indice
val dfindice= df2020.withColumn("indice",row_number().over(Window.orderBy(col("Life Ladder").desc)))


// Encontrar maximo gdp
val gdpMaximo = dfindice.select(max("Log GDP per capita")).collect()(0)(0)


//seleccionar el pais con mas gdp
val paisMasGdp = dfindice.filter(col("Log GDP per capita") === gdpMaximo).select("Country name").first()
//seleccionar el puesto de felicidad del pais con mas gdp
val puestoFelicidad =dfindice.filter(col("Log GDP per capita") === gdpMaximo).select("indice").first()

// Mostrar el resultado
println(s"El país con mas GDP en 2020 es $paisMasGdp en el puesto  $puestoFelicidad de Felicidad.")

// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 5 
// MAGIC
// MAGIC ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó 
// MAGIC o disminuyó?
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// cambiar el tipo de dato de GDP ya que es string
val dfreport_v2 = dfreport.withColumn("Log GDP per capita",col("Log GDP per capita").cast("Double"))
// filtrar por año
val df2020 = dfreport_v2.filter(col("year")==="2020")

//calcular promedio 2020

val promedioGdp2020 = df2020.groupBy("year").agg(avg(col("Log GDP per capita").as("Promedio GDP2020")))

// cambiar el tipo de dato de GDP ya que es string
val df2021_v2 = df2021.withColumn("Logged GDP per capita", col("Logged GDP per capita").cast("Double")).withColumn("year", lit("2021"))


//df2021_v2.printSchema

//calcular promedio 2021
val promedioGdp2021 = df2021_v2.groupBy("year").agg(avg(col("Logged GDP per capita").as("Promedio GDP2021")))

//calcular variación 

//val variacionGdp = ((promedioGdp2021.collect()(0)(1) - promedioGdp2020.collect()(0)(1))/ promedioGdp2020.collect()(0)(1)) *100
val variacionGdp =((promedioGdp2021.collect()(0)(1).asInstanceOf[Double] - promedioGdp2020.collect()(0)(1).asInstanceOf[Double]) / promedioGdp2020.collect()(0)(1).asInstanceOf[Double]) * 100
//variacionGdp.show()

// Determinar si disminuyó o aumentó
if (variacionGdp < 0) {
  val variacionGdpFormateada = f"%%.2f".format(variacionGdp)
  println(s"El GDP disminuyó en un $variacionGdpFormateada% desde 2020 hasta 2021.")
} else if (variacionGdp > 0) {
  val variacionGdpFormateada = f"%%.2f".format(variacionGdp)
  println(s"El GDP aumentó en un $variacionGdpFormateada% desde 2020 hasta 2021.")
} else {
  println("El GDP se mantuvo constante desde 2020 hasta 2021.")
}


// COMMAND ----------

// MAGIC %md
// MAGIC ###Ejercicio 6
// MAGIC
// MAGIC  ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia 
// MAGIC en ese indicador en el 2019? (**nota discord**: 2020 y 2021 y luego comparar con 2019)
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions._


// Cambiar el tipo de dato de Healthy life expectancy ya que es string para 2021 y cambio nombre para que coincidan los 2 datasets
val df2021_v2 = df2021.withColumn("Healthy life", col("Healthy life expectancy").cast("Double")).withColumn("year", lit("2021"))
val df2021Healthy = df2021_v2.select(col("year"),col("Healthy life"),col("Country name"))

// Cambiar el tipo de dato de Healthy life expectancy at birth ya que es string para todos los años y cambio nombre para que coincidan los 2 datasets
val dfreport_v2 = dfreport.withColumn("Healthy life", col("Healthy life expectancy at birth").cast("Double"))
// selecciono año 2020 y 2010
val dfreportHealthy = dfreport_v2.select(col("year"),col("Healthy life"),col("Country name"))
val df2020 = dfreportHealthy.filter(col("year")==="2020")
val df2019 = dfreportHealthy.filter(col("year")==="2019")

// Unir 2020 y 2021en un solo DataFrame
val df2020_2021 = df2020.union(df2021Healthy)

// pais con mayor indice healthy en 2020 y 2021

val maxHealthy = df2020_2021.select(max("Healthy life").cast("double")).collect()(0)(0)


//seleccionar el con mas esperanza de vida
val paisMasHealthy = df2020_2021.filter(col("Healthy life") === maxHealthy).select("Country name").first().getAs[String]("Country name")
//indicador en 2019
val healthy2019 = df2019.filter(col("Country name")===paisMasHealthy).select("Healthy life").first()

//mostrar resultado
println(s"El pais con más esperanza de vida en 2020/2021 con un indicador de $maxHealthy fue $paisMasHealthy y tenía un indicador en 2019 de $healthy2019.")
