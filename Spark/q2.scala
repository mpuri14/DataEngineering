// Databricks notebook source
// MAGIC %fs ls /FileStore/tables/

// COMMAND ----------

import org.apache.spark.sql.functions._
val sparkdf = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/FileStore/tables/bitcoinotc.csv")
val newColNames = Seq("v", "d", "c")
val df2 = sparkdf.toDF(newColNames: _*)
val df3 = df2.select(df2("v"), df2("d"), df2("c")).distinct
var df4 = df3.filter($"c">=5)
val df5 = df4.groupBy($"v").sum("c").withColumnRenamed("sum(c)", "weighted-out-degree")
val df6 = df4.groupBy($"d").sum("c").withColumnRenamed("sum(c)", "weighted-in-degree")
val df7= df5.join(df6, df5("v") === df6("d"),"fullouter")
val df9 = df7.select(coalesce($"v", $"d"),$"weighted-out-degree", $"weighted-in-degree")
val df10 = df9.na.fill(0,Seq("weighted-out-degree")).withColumnRenamed("coalesce(v, d)", "node")
val df11 = df10.withColumn("weighted-total-degree", $"weighted-out-degree" + $"weighted-in-degree").orderBy("node")
df11.sort($"weighted-total-degree".desc)
val df13 = df11.select($"node", $"weighted-total-degree")
val newColNames2 = Seq("v", "d")
val df14 = df13.toDF(newColNames2: _*)
val df15 = df14.orderBy(desc("weighted-total-degree"),asc("node")).withColumn("c", lit("t")).limit(1)
df11.sort($"weighted-in-degree".desc)
val df16 = df11.select($"node", $"weighted-in-degree")
val newColNames3 = Seq("v", "d")
val df17 = df16.toDF(newColNames3: _*)
val df18 = df17.orderBy(desc("weighted-in-degree"),asc("node")).withColumn("c", lit("i")).limit(1)
df11.sort($"weighted-out-degree".desc)
val df19 = df11.select($"node", $"weighted-out-degree")
val newColNames4 = Seq("v", "d")
val df20 = df19.toDF(newColNames4: _*)
val df21 = df20.orderBy(desc("weighted-out-degree"),asc("node")).withColumn("c", lit("o")).limit(1)
val df22 = df18.union(df21)
val df23 = df22.union(df15)
df23.show()

