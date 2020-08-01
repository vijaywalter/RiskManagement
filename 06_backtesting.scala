// Databricks notebook source
// MAGIC %md
// MAGIC # Value at risk - back testing
// MAGIC 
// MAGIC In this notebook, we show how to
// MAGIC - Compute VaR for a specific industry / sector / country
// MAGIC - AS-OF join of VaR to actual return
// MAGIC - Apply a 250 days sliding window to detect breaches
// MAGIC 
// MAGIC ### BASEL III
// MAGIC The Basel Committee specified a methodology for backtesting VaR. The 1 day VaR 99 results are to
// MAGIC be compared against daily P&L’s. Backtests are to be performed quarterly using the most recent 250
// MAGIC days of data. Based on the number of exceedances experienced during that period, the VaR
// MAGIC measure is categorized as falling into one of three colored zones:
// MAGIC 
// MAGIC | Level   | Threshold                 | Results                       |
// MAGIC |---------|---------------------------|-------------------------------|
// MAGIC | Green   | Up to 4 exceedances       | No particular concerns raised |
// MAGIC | Yellow  | Up to 9 exceedances       | Monitoring required           |
// MAGIC | Red     | More than 10 exceedances  | VaR measure to be improved    |
// MAGIC 
// MAGIC ### Authors
// MAGIC - Antoine Amend [<antoine.amend@databricks.com>]

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP0` Configuration

// COMMAND ----------

// DBTITLE 1,Import libraries
// MAGIC %python
// MAGIC %matplotlib inline
// MAGIC import pandas as pd
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt
// MAGIC import time
// MAGIC from datetime import datetime, timedelta
// MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
// MAGIC from pyspark.sql.window import Window
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql import functions as F

// COMMAND ----------

// DBTITLE 1,Import libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.joda.time.DateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Control parameters
val portfolio_table = "var_portfolio"
val stock_table = "var_stock"
val stock_return_table = "var_stock_return"
val market_table = "var_market"
val market_return_table = "var_market_return"
val trial_table = "var_monte_carlo"

//number of simulations
val runs = 50000

//value at risk confidence
val confidenceVar = 95

// COMMAND ----------

// DBTITLE 1,Register VAR user aggregated function from previous notebook
class ValueAtRisk(n: Int) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(Array(StructField("worst", ArrayType(DoubleType))))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  // The order we process dataframe does not matter, the worst will always be the worst
  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq.empty[Double]
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Seq[Double]](0) :+ input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  // We only keep worst N events
  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    buffer(0) = (buffer.getAs[Seq[Double]](0) ++ row.getAs[Seq[Double]](0)).sorted.take(n)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  // Our value at risk is best of the worst n overall
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[Double]](0).sorted.last
  }

}

// Assume we've generated 100,000 monte-carlo simulations for each instrument
val numRecords = runs

// We want to compute Var(99)
val confidence = confidenceVar

// So the value at risk is the best of the worst N events 
val n = (100 - confidence) * numRecords / 100

// Register UADFs
val valueAtRisk = new ValueAtRisk(n)
spark.udf.register("VALUE_AT_RISK", new ValueAtRisk(n))

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP1` Compute value at risk

// COMMAND ----------

// DBTITLE 1,Compute historical VAR
val convertDate = udf((s: String) => {
  new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime
})

case class VarHistory(time: Long, valueAtRisk: String)

val historicalVars = sql(s"""
  SELECT t.run_date, VALUE_AT_RISK(t.return) AS valueAtRisk
  FROM 
    (
    SELECT m.run_date, m.seed, sum(m.trial) AS return
    FROM ${trial_table} m
    GROUP BY m.run_date, m.seed
    ) t
  GROUP BY 
    t.run_date
  """
  )
  .withColumn("time", convertDate(col("run_date")))
  .orderBy(asc("time"))
  .select("time", "valueAtRisk")
  .as[VarHistory]
  .collect()
  .sortBy(_.time)
  .reverse

val historicalVarsB = spark.sparkContext.broadcast(historicalVars)
display(historicalVars.toList.toDF())

// COMMAND ----------

// DBTITLE 1,Retrieve historical returns and as-of join
val asOfVar = udf((s: java.sql.Date) => {
  val historicalVars = historicalVarsB.value
  if(s.getTime < historicalVars.last.time) {
    Some(historicalVars.last.valueAtRisk)
  } else {
    historicalVarsB.value.dropWhile(_.time > s.getTime).headOption.map(_.valueAtRisk) 
  }
})

val minDate = new java.sql.Date(new DateTime(historicalVars.last.time).minusDays(250).getMillis)
val maxDate = new java.sql.Date(historicalVars.head.time)

val historicalReturns = spark
  .read
  .table(stock_return_table)
  .filter(col("date") >= lit(minDate))
  .filter(col("date") <= lit(maxDate))
  .groupBy("date")
  .agg(sum("return").as("return"))
  .withColumn("var", asOfVar(col("date")))
  .orderBy(asc("date"))

display(historicalReturns)

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP2` Extract breaches

// COMMAND ----------

// DBTITLE 1,Extract number of breaches through 250 days sliding window
import org.apache.spark.sql.expressions.Window

val toTime = udf((s: java.sql.Date) => {
  s.getTime / 1000
})

val windowSpec = Window.orderBy("time").rangeBetween(-3600 * 24 * 250, 0)
val countBreaches = udf((asOfVar: Double, returns: Seq[Double]) => {
  returns.count(_ < asOfVar)
})

historicalReturns
  .withColumn("time", toTime(col("date")))
  .withColumn("returns", collect_list("return").over(windowSpec))
  .withColumn("count", countBreaches(col("var"), col("returns")))
  .drop("returns", "time")
  .createOrReplaceTempView("breaches")

// COMMAND ----------

// DBTITLE 1,Report breaches
// MAGIC %python
// MAGIC 
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt 
// MAGIC 
// MAGIC breaches = sql("SELECT date, return, CAST(var AS DOUBLE), `count` FROM breaches ORDER BY date ASC").toPandas()
// MAGIC 
// MAGIC # create pandas datetime dataframe
// MAGIC breaches.index = breaches['date']
// MAGIC breaches = breaches.drop(['date'], axis=1)
// MAGIC 
// MAGIC # detect breach severity
// MAGIC basel1 = breaches[breaches['count'] <= 4]
// MAGIC basel2 = breaches[(breaches['count'] > 4) & (breaches['count'] < 10)]
// MAGIC basel3 = breaches[breaches['count'] >= 10]
// MAGIC 
// MAGIC # plot it
// MAGIC f, (a0, a1) = plt.subplots(2, 1, figsize=(20,8), gridspec_kw={'height_ratios': [10,1]})
// MAGIC 
// MAGIC a0.plot(breaches.index, breaches['return'], color='#86bf91', label='returns')
// MAGIC a0.plot(breaches.index, breaches['var'], label="var99", c='red', linestyle='--')
// MAGIC a0.axhline(y=0, linestyle='--', alpha=0.2, color='#86bf91', zorder=1)
// MAGIC a0.title.set_text('VAR99 backtesting')
// MAGIC a0.set_ylabel('Daily log return')
// MAGIC a0.legend(loc="upper left")
// MAGIC a0.get_xaxis().set_ticks([])
// MAGIC 
// MAGIC a1.bar(basel1.index, 1, color='green', label='breaches', alpha=0.1, width=10, zorder=3)
// MAGIC a1.bar(basel2.index, 1, color='orange', label='breaches', alpha=0.1, width=10, zorder=5)
// MAGIC a1.bar(basel3.index, 1, color='red', label='breaches', alpha=0.1, width=10, zorder=10)
// MAGIC a1.get_yaxis().set_ticks([])
// MAGIC a1.set_xlabel('Date')
// MAGIC 
// MAGIC plt.subplots_adjust(wspace=0, hspace=0)

// COMMAND ----------

// DBTITLE 1,Report breaches
// MAGIC %python
// MAGIC basel3.head(10)

// COMMAND ----------

// MAGIC %md
// MAGIC # `HOMEWORK` Model risk management
// MAGIC 
// MAGIC With your model stored as a `pyfunc` model, how would you attach above evidence and publish your model to MRM?
// MAGIC 
// MAGIC ```
// MAGIC # Attach artifacts
// MAGIC client = mlflow.tracking.MlflowClient()
// MAGIC client.log_artifact(model_run_id, "evidence/breaches.csv")
// MAGIC client.log_artifact(model_run_id, "evidence/backtest.png")
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC # Publish model
// MAGIC client = mlflow.tracking.MlflowClient()
// MAGIC model_uri = "runs:/{}/model".format(model_run_id)
// MAGIC model_name = "value_at_risk"
// MAGIC 
// MAGIC result = mlflow.register_model(
// MAGIC     model_uri,
// MAGIC     model_name
// MAGIC )
// MAGIC 
// MAGIC version = result.version
// MAGIC ```
