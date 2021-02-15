Dataframe Operation

Reading a DataFrame

val myDF = spark
.read
.format("json")
.schema(schema)
.option("mode","failFast")
.option("path", "path of the datafile")
.load()

//Alternative Reading with Option Map

With the help of Map, we can avoid writing multiple option

val myDF = spark.read
.format
.options(map(
"mode" -> "failFast",
"path" -> "path of the Datafile Location",
"inferSchema" -> "true"
))
.load()


/**
Writing or Saving DataFrame
* format
* save mode - overwrite, append, ignore, errorIfExists
* path
* Zero or more options
**/

myDF.write
.format("json")
.mode(saveMode.overwrite)
.save("path where the data will be saved")


//Json Flags



======

Dataframe columns and Expressions

To obtain the Columns and expression from the Dataframe

val firstColumn = myDF.col("name")

val carNames = myDF.select(firstColumn)

carNames.show()

Various select methods

We can pass multiple column in select methods in multiple ways.

myDF.select(
myDF.col("name"),
col("Acceleration"),
column("weight_in_lbs"),
'Year',   //Scala - Symbol, Auto converted to Column
$"Horsepower",  //Fancier interpolated string
expr("Origin")  //Expressions
)

//To use col("Name") directly we will have to import org.apache.spark.sql.{col}

Other way is to use straight forward column name in select methods
//Select with plain columns name
myDF.select("Name", "Year")

//Note : Either use column method of object or use plain column methods. Don't use combination of both

//Expressions

val simplestExpression = myDF.col("weight_in_lbs")
val weightInKgExpression = myDF.col("weight_in_lbs") / 2.2

val carwithWeightDF = myDF.select(
col("name"),
col("weight_in_lbs"),
weightInKgExpression.as("weight_in_kgs") //Renaming the column value from "weight_in_lbs" / 2.2 to weight_in_kgs
)

carwithWeightDF.show()

OR we can directly use expr inside select method as well

val carwithWeightDF = myDF.select(
col("name"),
col("weight_in_lbs"),
weightInKgExpression.as("weight_in_kgs"), //Renaming the column value from "weight_in_lbs" / 2.2 to weight_in_kgs
expr("weight_in_lbs" / 2.2).as("weight_in_kgs") 
)


//selectExpr method

Using this method we can pass multiple Expression in a single selectExpr method

val carWithMultipleSelectExpr = myDF.selectExpr(
"name",
"weight_in_lbs",
"weight_in_lbs / 2.2"
)


//DataFrame Processing

//Adding a Column (withColumn)

//Creating a new column weight_in_kgs from weight_in_lbs
val newDF = myDF.withColumn("weight_in_kgs",col("weight_in_lbs") / 2.2 )

//Renaming a Column(withColumnRenamed)
//Renaming weight_in_lbs to weight_in_pounds
val renamedDF = myDF.withColumnRenamed("weight_in_lbs", "weight_in_pound")

//Careful while renaming a column
//If there is space between the name then we will have to use `` backtrace to query that

val renamedDF = myDF.withColumnRenamed("weight_in_lbs", "weight in pound")

//To Select use `` as below
renamedDF.selectExpr("`weight in pound`")

//Dropping a column
renamedDF.drop("`weight in pound`", "weight_in_lbs")

//Filtering the column

myDF.filter(col("origin") =!= "USA") // =!= --> stands for Not equal to
We can use filter or where here
myDF.where(col("origin") =!= "USA")

//Filtering with Expression string
myDF.filter("Origin = 'USA'")

//Multiple filter

myDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)

//We can use and method to avoid multiple filter keyword

myDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))

//We can pass multiple column in Single String as well

myDF.filter("Origin = 'USA' and Horsepower" > 150)

//Union in Dataframe -- Adding More columns


Lets Say we have a file in which we have more data

val moreCarsDF = spark.read.option("inferSchema","true").json("path of the file")

Join the 2 DataFrame

val moreCarsDF = myDF.union(moreCarsDF)  //Works if the DFs have similar Schema

//Distinct Values

val distinctValue = myDF.select("origin").distinct()


--Exercise
//Select 2 columns of your Choice

myDF.select(
"Name",
"Model"
)

OR myDF.selectExpr("Name", "Model")

//From Movies Datafile, create a column that sums up the Total profit of the Movies = US_gross + worldwide_gross + DVD_sales

val moviesProfitDF = movieDF.select(
col("Title"),
col("US_Gross"),
col("Worldwide_Gross"),
(col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")

Or

val moviesProfitDF2 = movieDF.selectExpr(
"Title",
"US_Gross",
"Worldwide_Gross",
"US_Gross + Worldwide_Gross as Total_Gross"
)

Or

val moviesProfitDF3 = movieDF.select("Title","US_Gross","Worldwide_Gross")
.withColumn("Total_Gross", col("US_Gross) + col("Worldwide_Gross"))

//Select all Comedy_movie with IMDB rating above 6

val comedieSDF = movieDF.select("Title", "IMDB_Rating").where(col("Major_Genre") === "comedy" and col("IMDB_Rating") > 6)

Or

val comedieSDF2 = movieDF.select("Title", "IMDB_Rating")
.where(col("Major_Genre") === "comedy")
.where(col("IMDB_Rating") > 6)

Or


val comedieSDF2 = movieDF.select("Title", "IMDB_Rating")
.where("Major_Genre = 'comedy' and IMDB_Rating > 6")
.where(col()



//Aggregation

--Counting

val countMajorGenre = movieDF.select(count(col("Major_Genre")))   //All the Value except null

Using SelectExpr

val countMajorGenre = movieDF.select("count(Major_Genre)")   //All the Value except null

Count all the Rows including null

val countDF = movieDF.select(count("*"))

//Counting Single columns exclude null and counting all include Null as well

//Counting Distinct Values(countDistinct)

Distinct Major Genre

movieDF.select(countDistinct(col("Major_Genre")))

//Approx Count(approx_count_distinct)

movieDF.select(approx_count_distinct(col("Major_Genre")))

//Min and Max

movieDF.select(min(col("IMDB_Rating")))

Or

movieDF.selectExpr("min(IMDB_Rating)")


//Sum

movieDF.select(sum(col("US_Gross")))
Or
movieDF.selectExpr("sum(US_Gross)")

//Avg

movieDF.select(avg(col("Rotten_Tomatoes_Rating")))
Or
movieDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

//DataScience
mean/Stddev

movieDF.select(mean(col("Rotten_Tomatoes_Rating")),
stddev(col("Rotten_Tomatoes_Rating")))


//Grouping(groupBy())

movieDF.groupBy(col("Major_Genre")).count()  //Similar to select count(*) from movieDF groupBy Major_Genre

//Group By also includes null values

//Average Rating By Genre

movieDF.groupBy(col("Major_Genre"))
.avg("IMDB_Rating")


movieDF.groupBy(col("Major_Genre"))
.agg(
   count("*").as("N_Movies"),
   avg("IMDB_Rating").as("Avg_Rating")
)
.orderBy(col("Avg_Rating"))

//Exercise

Sum up all the Profits of All the Movies in the DataFrame

movieDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
.select(sum("Total_Gross"))
.show()

Count how many Distinct Directors we have
movieDF.select(countDistinct(col("Directors"))).show()

Show the mean and Standard Deviation of US_Gross revenue of the Movie
movieDF.select(mean("US_Gross"),
stddev("US_Gross")).show()


Compute Average IMDB_Rating and average US Gross revenue per Director

movieDF.groupBy("Director")
.agg(
avg("IMDB_Rating").as("Avg_Rating"),
sum("US_Gross").as("Total_US_Gross")
)
.orderBy(col("Avg_Rating").desc_null_last)
.show()

//desc_null_last will put the Null value in the Last(Descending order)




//Joins











val DF1 = Seq(
(101, "sachin",40),
(102, "zahir",41),
(103, "virat",29),
(104, "saurav",41),
(105,"rohit",30)
).toDF("id", "name","age")


scala> DF1.show()
+---+------+---+
| id|  name|age|
+---+------+---+
|101|sachin| 40|
|102| zahir| 41|
|103| virat| 29|
|104|saurav| 41|
|105| rohit| 30|
+---+------+---+

val DF2 = Seq(
(101, "batsman"),
(102, "bowler"),
(103, "batsman"),
(104, "batsman")
).toDF("id", "skill")
saurzDF2: org.apache.spark.sql.DataFrame = [id: int, skill: string]

saurzDF2.show()
+---+-------+
| id| skill|
+---+-------+
|101|batsman|
|102| bowler|
|103|batsman|
|104|batsman|
+---+-------+

val DF3 = Seq(
(101, "sachin",100),
(103, "virat",50),
(104,  "saurav",45),
(105,"rohit",35)
).toDF("id", "name","centuries")
DF3: org.apache.spark.sql.DataFrame = [id: int, name: string, centuries: int]

DF3.show()
+---+------+---------+
| id|  name|centuries|
+---+------+---------+
|101|sachin|      100|
|103| virat|       50|
|104|saurav|       45|
|105| rohit|       35|
+---+------+---------+


      DF1                     DF2                DF3

+---+------+---+        +---+-------+     +---+------+---------+
| id|  name|age|        | id| skill |     | id|  name|centuries|
+---+------+---+        +---+-------+     +---+------+---------+
|101|sachin| 40|        |101|batsman|     |101|sachin|      100|
|102| zahir| 41|        |102| bowler|     |103| virat|       50|
|103| virat| 29|        |103|batsman|     |104|saurav|       45|
|104|saurav| 41|        |104|batsman|     |105| rohit|       35|
|105| rohit| 30|        +---+-------+     +---+------+---------+
+---+------+---+        

Joining Dataframe

val jDF = DF1.join(DF2)


Inner Joining Using Column
This join behaves exactly like INNER join in SQL and in the result, join column will appear exactly once.

val joinColDF = DF1.join(DF2,"id")

scala> joinColDF.show()
+---+------+---+-------+
| id|  name|age|  skill|
+---+------+---+-------+
|101|sachin| 40|batsman|
|102| zahir| 41| bowler|
|103| virat| 29|batsman|
|104|saurav| 41|batsman|
+---+------+---+-------+

Inner Join using Sequence of Columns
This is also equivalent to SQL INNER Join, but using a sequence of columns, and join columns will appear exactly once.

val joinMultiColDF = DF1.join(DF3,Seq("id", "name"))
joinMultiColDF.show()

scala> joinMultiColDF.show()
+---+------+---+---------+
| id|  name|age|centuries|
+---+------+---+---------+
|101|sachin| 40|      100|
|103| virat| 29|       50|
|104|saurav| 41|       45|
|105| rohit| 30|       35|
+---+------+---+---------+


Left Outer Join Using Sequence of columns
This is also equivalent to SQL LEFT OUTER Join, but using a sequence of columns, and join columns will appear exactly once.

val leftJoinDF= DF1.join(DF3,Seq("id", "name"),"left_outer")
leftJoinDF.show()

scala> leftJoinDF.show()
+---+------+---+---------+
| id|  name|age|centuries|
+---+------+---+---------+
|101|sachin| 40|      100|
|102| zahir| 41|     null|
|103| virat| 29|       50|
|104|saurav| 41|       45|
|105| rohit| 30|       35|
+---+------+---+---------+

Left Semi Join using Sequence of Columns
This is also equivalent to SQL LEFT SEMI Join, and the output contains only columns from left data frame.

val leftSemiJoinDF = DF1.join(DF3,Seq("id", "name"),"leftsemi")
leftSemiJoinDF.show()

scala> leftSemiJoinDF.show()
+---+------+---+
| id|  name|age|
+---+------+---+
|101|sachin| 40|
|103| virat| 29|
|104|saurav| 41|
|105| rohit| 30|
+---+------+---+

Outer Join Using Sequence of Columns
This is also equivalent to SQL OUTER Join, but using a sequence of columns, and join columns will appear exactly once.

val outerJoinDF = DF1.join(DF3,Seq("id", "name"),"outer")
outerJoinDF.show()

scala> outerJoinDF.show()
+---+------+---+---------+
| id|  name|age|centuries|
+---+------+---+---------+
|105| rohit| 30|       35|
|103| virat| 29|       50|
|101|sachin| 40|      100|
|102| zahir| 41|     null|
|104|saurav| 41|       45|
+---+------+---+---------+


Right Outer Join Using Sequence of Columns
This is also equivalent to SQL RIGHT OUTER JOIN, but using a sequence of columns, and join columns will appear exactly once.

val rightOuterJoinDF = DF1.join(DF3,Seq("id", "name"),"right_outer")
rightOuterJoinDF.show()

scala> rightOuterJoinDF.show()
+---+------+---+---------+
| id|  name|age|centuries|
+---+------+---+---------+
|101|sachin| 40|      100|
|103| virat| 29|       50|
|104|saurav| 41|       45|
|105| rohit| 30|       35|
+---+------+---+---------+

Inner join Using Join Expressions
It performs INNER join operation using a join expression. Joins using expressions, 
produce join columns from both data frames and it is required to explicitly select the columns from output or the undesired column can be dropped later.

val innerJoinExprDF = DF1.join(DF2,DF1("id") === DF2("id"),"inner")
innerJoinExprDF.show()

Outer Join Using Join Expressions
Similar to above, it performs  OUTER JOIN.

val outerJoinDF = DF1.join(DF2,DF1("id")=== DF2("id"),"outer")
outerJoinDF.show()

scala> outerJoinDF.show()
+---+------+---+----+-------+
| id|  name|age|  id|  skill|
+---+------+---+----+-------+
|101|sachin| 40| 101|batsman|
|103| virat| 29| 103|batsman|
|102| zahir| 41| 102| bowler|
|105| rohit| 30|null|   null|
|104|saurav| 41| 104|batsman|
+---+------+---+----+-------+

Left Outer Join using Join Expressions
Similar to above, it performs LEFT OUTER JOIN.

val leftOuterJoinDF = DF1.join(DF2,DF1("id") === DF2("id"),"left_outer")
leftOuterJoinDF.show()


Right Outer Join using Join Expressions
Similar to above , it performs RIGHT OUTER JOIN.

val rightOuterJoinDF = DF1.join(DF2,DF1("id") === DF2("id"),"right_outer")
rightOuterJoinDF.show()


Left Semi Join using Join Expressions
Similar to above, it performs LEFT SEMI JOIN. Please note the difference between LEFT SEMI JOIN and INNER JOIN here – 
LEFT SEMI JOIN , will do an inner join and only give columns from the left data frame, while INNER join will give columns from both the data frames.

val leftSemiJoinDF = DF1.join(DF2,DF1("id") === DF2("id"),"leftsemi")
leftSemiJoinDF.show()

