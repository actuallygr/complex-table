from pyspark.sql.types import StructField, StructType, StringType, LongType, DateType, DoubleType

myNyseDailySchema = StructType([
  StructField("exchange", StringType(), True),
  StructField("stock_symbol", StringType(), True),
  StructField("date", StringType(), False),
  StructField("open_price", DoubleType(), False),
  StructField("high_price", DoubleType(), False),
  StructField("low_price", DoubleType(), False),
  StructField("close_price", DoubleType(), False),
  StructField("volume", LongType(), False),
  StructField("adj_close_price", DoubleType(), False)
])

nyse_daily = spark.read.format("csv")\
.schema(myNyseDailySchema)\
.option("sep","\t")\
.load("/FileStore/tables/NYSE_daily_1-cb8bb.tsv")

nyse_daily.show()

nyse_daily.createOrReplaceTempView("nyse_daily")

nyseDividentsSchema = StructType([
  StructField("exchange", StringType(), True),
  StructField("stock_symbol", StringType(), True),
  StructField("date", StringType(), False),
  StructField("dividents", DoubleType(), False)
])

nyse_divi = spark.read.format("csv")\
.schema(nyseDividentsSchema)\
.option("sep", "\t")\
.load("/FileStore/tables/NYSE_dividends_1-55339.tsv")

nyse_divi.show()

nyse_divi.createOrReplaceTempView("nyse_divi")

spark.sql("SELECT * from nyse_daily WHERE close_price >= 200 AND volume >= 10000000").show() #problem 1

spark.sql("SELECT stock_symbol, count(dividents) as count_dividents from nyse_divi group by stock_symbol ").createOrReplaceTempView("problem2")
spark.sql("SELECT * from problem2 where count_dividents > 50 ").show() # problem 2


