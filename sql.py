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
problem2 = spark.sql("SELECT * from problem2 where count_dividents > 50 ")

problem2.write.csv("problem2.csv")


spark.sql("Select daily.stock_symbol, daily.close_price, div.dividents, div.date from nyse_daily daily join nyse_divi div on daily.stock_symbol = div.stock_symbol and daily.date = div.date WHERE daily.close_price >=100").show() #problem3




tructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("year", DataTypes.IntegerType, true),
                DataTypes.createStructField("length", DataTypes.IntegerType, true),
                DataTypes.createStructField("title", DataTypes.StringType, true),
                DataTypes.createStructField("subject", DataTypes.StringType, true),
                DataTypes.createStructField("actor", DataTypes.StringType, true),
                DataTypes.createStructField("actores", DataTypes.StringType, true),
                DataTypes.createStructField("director", DataTypes.StringType, true),
                DataTypes.createStructField("popularity", DataTypes.IntegerType, true),
                DataTypes.createStructField("award", DataTypes.StringType, true),
                DataTypes.createStructField("file_name", DataTypes.StringType, true)
        });

        Dataset<Row> movieDf = spark.read().format("csv")
                .schema(schema)
                .option("sep",";")
                .option("multiline", true)
                .load("src/main/resources/movie_datset.txt");

       movieDf = movieDf.withColumn("decade", col("year").minus(1900).divide(10).cast(DataTypes.IntegerType));
        movieDf = movieDf.drop("subject").drop("actor").drop("actores").drop("director").drop("award").drop("file_name").drop("length");
        
        Dataset<Row> maxPopularity = movieDf.groupBy(col("decade")).agg(max("popularity").as("popularity"));

        Dataset<Row> joinedDf = movieDf.join(maxPopularity, movieDf.col("decade").equalTo(maxPopularity.col("decade")).and(movieDf.col("popularity").equalTo(maxPopularity.col("popularity"))))
                .orderBy(movieDf.col("decade").desc())
                .drop(movieDf.col("decade"))
                .drop(movieDf.col("popularity"));

        joinedDf.show();
        
        
        +----+--------------------+------+----------+
|year|               title|decade|popularity|
+----+--------------------+------+----------+
|1990| Blood in, Blood Out|     9|        88|
|1990| Guilty by Suspicion|     9|        88|
|1992|           Class Act|     9|        88|
|1990|   Dangerous Pursuit|     9|        88|
|1991|           Raw Nerve|     9|        88|
|1991|     Great Race, The|     9|        88|
|1989|      New Year's Day|     8|        88|
|1986|  Best of Times, The|     8|        88|
|1982|Ballad of Narayam...|     8|        88|
|1985|Gonzo Presents Mu...|     8|        88|
|1980|Happy Birthday to Me|     8|        88|
|1989|         Let It Ride|     8|        88|
|1986|         Head Office|     8|        88|
|1985|       Out of Africa|     8|        88|
|1989|        Final Notice|     8|        88|
|1988|        Five Corners|     8|        88|
|1970|   Fellini Satyricon|     7|        88|
|1976|Creature from Bla...|     7|        88|
|1971|French Connection...|     7|        88|
|1972|    Jeremiah Johnson|     7|        88|
+----+--------------------+------+----------+
