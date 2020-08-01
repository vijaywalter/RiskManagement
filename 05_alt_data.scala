// Databricks notebook source
// MAGIC %md
// MAGIC # Value at risk - alternative data
// MAGIC 
// MAGIC In this notebook, we show how to
// MAGIC - wget latest GDELT file from 15mn feed
// MAGIC - use third parties libraries to parse as dataframe
// MAGIC - detect trends through sliding window
// MAGIC 
// MAGIC This notebook requires the following dependencies
// MAGIC - `com.aamend.spark:spark-gdelt:2.0`
// MAGIC 
// MAGIC ### Authors
// MAGIC - Antoine Amend [<antoine.amend@databricks.com>]

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP0` Configuration

// COMMAND ----------

val news_table_bronze = "gdelt_news_analytics_bronze"
val news_table_silver = "gdelt_news_analytics_silver"
val news_table_gold = "gdelt_news_analytics_gold"

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP1` Access raw data

// COMMAND ----------

// DBTITLE 1,Retrieve GDELT files for year to date
// MAGIC %sh
// MAGIC 
// MAGIC MASTER_URL=http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
// MAGIC 
// MAGIC if [[ -e /tmp/gdelt ]] ; then
// MAGIC   rm -rf /tmp/gdelt
// MAGIC fi
// MAGIC mkdir /tmp/gdelt
// MAGIC 
// MAGIC echo "Retrieve latest URL from [${MASTER_URL}]"
// MAGIC URLS=`curl ${MASTER_URL} 2>/dev/null | awk '{print $3}' | grep gkg.csv.zip | grep gdeltv2/20200501`
// MAGIC for URL in $URLS; do
// MAGIC   echo "Downloading ${URL}"
// MAGIC   wget $URL -O /tmp/gdelt/gdelt.csv.zip > /dev/null 2>&1
// MAGIC   unzip /tmp/gdelt/gdelt.csv.zip -d /tmp/gdelt/ > /dev/null 2>&1
// MAGIC   LATEST_FILE=`ls -1rt /tmp/gdelt/*.csv | head -1`
// MAGIC   LATEST_NAME=`basename ${LATEST_FILE}`
// MAGIC   cp $LATEST_FILE /dbfs/tmp/gdelt/$LATEST_NAME
// MAGIC   rm -rf /tmp/gdelt/gdelt.csv.zip
// MAGIC   rm $LATEST_FILE
// MAGIC done

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP2` Bronze, silver and gold tables

// COMMAND ----------

// DBTITLE 1,Store clean content on Bronze
import com.aamend.spark.gdelt._
val gdeltDF = spark.read.gdeltGkg("/tmp/gdelt")
gdeltDF.write.format("delta").mode("overwrite").saveAsTable(news_table_bronze)
display(spark.read.table(news_table_bronze))

// COMMAND ----------

// DBTITLE 1,Filter events on Silver
import org.apache.spark.sql.functions._

val countryCodes = Map(
  "CI" -> "CHILE", 
  "CO" -> "COLUMBIA", 
  "MX" -> "MEXICO", 
  "PM" -> "PANAMA", 
  "PE" -> "PERU"
)

val to_country = udf((s: String) => countryCodes(s))
val filter_theme = udf((s: String) => {
  !s.startsWith("TAX") && !s.matches(".*\\d.*")
})

// Select countries and themes of interest
spark.read.table(news_table_bronze)
  .withColumn("location", explode(col("locations"))).drop("locations")
  .filter(col("location.countryCode").isin(countryCodes.keys.toArray: _*))
  .withColumn("tone", col("tone.tone"))
  .withColumn("fips", col("location.countryCode"))
  .withColumn("country", to_country(col("fips")))
  .filter(size(col("themes")) > 0)
  .withColumn("theme", explode(col("themes")))
  .filter(filter_theme(col("theme")))
  .select(
    "publishDate",
    "theme",
    "tone",
    "country"
  )
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable(news_table_silver)

display(spark.read.table(news_table_silver))

// COMMAND ----------

// DBTITLE 1,Detect trends on Gold table
import java.sql.Timestamp
import java.sql.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val to_day = udf((s: Timestamp) => new Date(s.getTime()))
val to_time = udf((s: Date) => s.getTime() / 1000)

// Group themes by day and country, finding total number of articles
val dailyThemeDF = spark.read.table(news_table_silver)
  .withColumn("day", to_day(col("publishDate")))
  .groupBy("day", "country", "theme")
  .agg(
    sum(lit(1)).as("total"),
    avg(col("tone")).as("tone")
  )
  .filter(col("day") >= "2020-01-01")
  .select("day", "country", "theme", "total", "tone")

// Use total number of articles by country
val dailyCountryDf = dailyThemeDF
  .groupBy("day", "country")
  .agg(sum(col("total")).as("global"))

// Normalize number of articles as proxy for media coverage
val mediaCoverageDF = dailyCountryDf
  .join(dailyThemeDF, List("country", "day"))
  .withColumn("coverage", lit(100) * col("total") / col("global"))
  .select("day", "country", "theme", "coverage", "tone", "total")

// Detect trends using a cross over between a 7 and a 30 days window
val ma30 = Window.partitionBy("country", "theme").orderBy("time").rangeBetween(-30 * 24 * 3600, 0)
val ma07 = Window.partitionBy("country", "theme").orderBy("time").rangeBetween(-7 * 24 * 3600, 0)

// Detect trends
val trendDF = mediaCoverageDF
  .withColumn("time", to_time(col("day")))
  .withColumn("ma30", avg(col("coverage")).over(ma30))
  .withColumn("ma07", avg(col("coverage")).over(ma07))
  .select("day", "country", "theme", "coverage", "ma07", "ma30", "tone", "total")
  .orderBy(asc("day"))
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable(news_table_gold)

display(spark.read.table(news_table_gold))

// COMMAND ----------

// MAGIC %md
// MAGIC # `STEP3` Detect trends

// COMMAND ----------

// DBTITLE 1,Get highest media coverage per country in last quarter
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW grouped_news AS 
// MAGIC SELECT 
// MAGIC   country, 
// MAGIC   theme, 
// MAGIC   MAX(coverage) AS moving_average,
// MAGIC   COUNT(1) AS total
// MAGIC FROM gdelt_news_analytics_gold
// MAGIC WHERE LENGTH(theme) > 0
// MAGIC GROUP BY theme, country;
// MAGIC   
// MAGIC SELECT * FROM (
// MAGIC   SELECT *, row_number() OVER (PARTITION BY country ORDER BY moving_average DESC) rank 
// MAGIC   FROM grouped_news
// MAGIC ) tmp
// MAGIC WHERE rank <= 5
// MAGIC ORDER BY country

// COMMAND ----------

// DBTITLE 1,Timeline events for mining activities in Peru
// MAGIC %sql
// MAGIC SELECT 
// MAGIC   day,
// MAGIC   total,
// MAGIC   CASE 
// MAGIC     WHEN sig = 0 THEN 'N/A'
// MAGIC     WHEN sig = -1 THEN 'HIGH'
// MAGIC     WHEN sig = 1 THEN 'LOW'
// MAGIC   END AS trend
// MAGIC FROM (
// MAGIC   SELECT day, total, SIGNUM(ma30 - ma07) AS sig FROM gdelt_news_analytics_gold
// MAGIC   WHERE country = "PERU"
// MAGIC   AND theme = 'ENV_MINING'
// MAGIC ) tmp
// MAGIC WHERE sig != 0
// MAGIC ORDER BY day ASC

// COMMAND ----------

// MAGIC %md
// MAGIC # `HOMEWORK` Predict market volatility
// MAGIC 
// MAGIC With news analytics available on delta, could you predict market volatility based on news events, as they unfold?
// MAGIC 
// MAGIC Reference: [https://eprints.soton.ac.uk/417880/1/manuscript2_2.pdf](https://eprints.soton.ac.uk/417880/1/manuscript2_2.pdf)
