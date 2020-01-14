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

        movieDf = movieDf.withColumn("decade", col("year").minus(1920).divide(10).cast(DataTypes.IntegerType));
        
        
        +----+------+--------------------+-------+--------------------+-------------------+--------------------+----------+-----+-------------------+------+
|year|length|               title|subject|               actor|            actores|            director|popularity|award|          file_name|decade|
+----+------+--------------------+-------+--------------------+-------------------+--------------------+----------+-----+-------------------+------+
|1990|   111|Tie Me Up! Tie Me...| Comedy|   Banderas, Antonio|    Abril, Victoria|    Almod�var, Pedro|        68|   No|   NicholasCage.png|     7|
|1991|   113|          High Heels| Comedy|        Bos�, Miguel|    Abril, Victoria|    Almod�var, Pedro|        68|   No|   NicholasCage.png|     7|
|1983|   104|      Dead Zone, The| Horror| Walken, Christopher|      Adams, Brooke|   Cronenberg, David|        79|   No|   NicholasCage.png|     6|
|1979|   122|                Cuba| Action|       Connery, Sean|      Adams, Brooke|     Lester, Richard|         6|   No|    seanConnery.png|     5|
|1978|    94|      Days of Heaven|  Drama|       Gere, Richard|      Adams, Brooke|    Malick, Terrence|        14|   No|   NicholasCage.png|     5|
|1983|   140|           Octopussy| Action|        Moore, Roger|        Adams, Maud|          Glen, John|        68|   No|   NicholasCage.png|     6|
|1984|   101|        Target Eagle| Action|      Connors, Chuck|        Adams, Maud|Loma, Jos� Antoni...|        14|   No|   NicholasCage.png|     6|
|1989|    99|American Angels: ...|  Drama|   Bergen, Robert D.|       Adams, Trudy|  Sebastian, Beverly|        28|   No|   NicholasCage.png|     6|
|1985|   104|              Subway|  Drama|Lambert, Christopher|   Adjani, Isabelle|         Besson, Luc|         6|   No|   NicholasCage.png|     6|
|1990|   149|     Camille Claudel|  Drama|   Depardieu, G�rard|   Adjani, Isabelle|      Nuytten, Bruno|        32|   No|   NicholasCage.png|     7|
|1982|   188| Fanny and Alexander|  Drama|     Ahlstedt, B�rje|Adolphson, Kristina|     Bergman, Ingmar|        81|  Yes|        Bergman.png|     6|
|1982|   117|Tragedy of a Ridi...|  Drama|       Tognazzi, Ugo|       Aimee, Anouk|Bertolucci, Bernardo|        17|   No|   NicholasCage.png|     6|
|1966|   103|     A Man & a Woman|  Drama|Trintignant, Jean...|       Aimee, Anouk|     Lelouch, Claude|        46|  Yes|   NicholasCage.png|     4|
|1986|   112|A Man & a Woman: ...|  Drama|Trintignant, Jean...|       Aimee, Anouk|     Lelouch, Claude|        49|   No|   NicholasCage.png|     6|
|1966|   103|Un Hombre y una M...|  Drama|Trintignant, Jean...|       Aimee, Anouk|     Lelouch, Claude|         6|  Yes|   NicholasCage.png|     4|
|1985|   112| Official Story, The|  Drama|     Alterio, Hector|    Aleandro, Norma|        Puenzo, Luiz|        39|  Yes|   NicholasCage.png|     6|
|1976|   150|Lindbergh Kidnapp...|  Drama|    Hopkins, Anthony|  Alexander, Denise|         Kulik, Buzz|        51|   No| AnthonyHopkins.png|     5|
|1929|    84|           Blackmail|Mystery|       Longden, John|       Algood, Sara|   Hitchcock, Alfred|         2|   No|alfredHitchcock.png|     0|
|1963|   109|      Donovan's Reef| Comedy|         Wayne, John|   Allen, Elizabeth|          Ford, John|        62|   No|      johnWayne.png|     4|
|1988|   110|Tucker: The Man &...|  Drama|       Bridges, Jeff|        Allen, Joan|Coppola, Francis ...|        68|   No|   NicholasCage.png|     6|
+----+------+--------------------+-------+--------------------+-------------------+--------------------+----------+-----+-------------------+------+

        
        

