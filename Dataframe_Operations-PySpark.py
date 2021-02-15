from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, expr, col
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Dataframe Transformation") \
        .master("local[3]") \
        .config("spark.shuffle.partitions",3) \
        .getOrCreate()

    dataList = [("Ravi","28","1","2002"),
                ("Sumeet","23","5","81"),
                ("Amit","12","12","6"),
                ("Sumeet","23","5","81"),
                ("Rosy","7","8","63")
                ]

    dataNDF = spark.createDataFrame(dataList)

    dataNDF.printSchema()

    '''
    root
    |-- _1: string (nullable = true)
    |-- _2: string (nullable = true)
    |-- _3: string (nullable = true)
    |-- _4: string (nullable = true)
    '''

    dataDF = spark.createDataFrame(dataList).toDF("Name","day","month","year")

    dataDF.printSchema()

    '''
    root
    |-- Name: string (nullable = true)
    |-- day: string (nullable = true)
    |-- month: string (nullable = true)
    |-- year: string (nullable = true)
    '''

    uniqueIDDF = dataDF.withColumn("id", monotonically_increasing_id())

    uniqueIDDF.show()

    '''
    +------+---+-----+----+-----------+
    |  Name|day|month|year|         id|
    +------+---+-----+----+-----------+
    |  Ravi| 28|    1|2002|          0|
    |Sumeet| 23|    5|  81| 8589934592|
    |  Amit| 12|   12|   6| 8589934593|
    |Sumeet| 23|    5|  81|17179869184|
    |  Rosy|  7|    8|  63|17179869185|
    +------+---+-----+----+-----------+
    '''

    df1 = uniqueIDDF.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end"""))

    df1.show()

    '''
    +------+---+-----+------+-----------+
    |  Name|day|month|  year|         id|
    +------+---+-----+------+-----------+
    |  Ravi| 28|    1|  2002|          0|
    |Sumeet| 23|    5|1981.0| 8589934592|
    |  Amit| 12|   12|2006.0| 8589934593|
    |Sumeet| 23|    5|1981.0|17179869184|
    |  Rosy|  7|    8|1963.0|17179869185|
    +------+---+-----+------+-----------+
    '''

    '''
    The Year column will be of Decimal type since the spark will convert the Year which is in String to add 
    it with the value provided as above case expression and then again it will convert it into the String
    again.
    '''

    # Cast the Datatype exactly where it is required

    df2 = uniqueIDDF.withColumn("year", expr("""
    case when year < 21 then cast(year as int) + 2000
    when year < 100 then cast(year as int) + 1900
    else year
    end"""))

    df2.show()

    '''
    +------+---+-----+----+-----------+
    |  Name|day|month|year|         id|
    +------+---+-----+----+-----------+
    |  Ravi| 28|    1|2002|          0|
    |Sumeet| 23|    5|1981| 8589934592|
    |  Amit| 12|   12|2006| 8589934593|
    |Sumeet| 23|    5|1981|17179869184|
    |  Rosy|  7|    8|1963|17179869185|
    +------+---+-----+----+-----------+
    '''

    # Casting Datatype of the required column in the beginning itself

    df3 = uniqueIDDF.withColumn("day", col("day").cast(IntegerType())) \
            .withColumn("month", col("month").cast(IntegerType())) \
            .withColumn("year", col("year").cast(IntegerType()))

    df3.printSchema()

    '''
    root
    |-- Name: string (nullable = true)
    |-- day: integer (nullable = true)
    |-- month: integer (nullable = true)
    |-- year: integer (nullable = true)
    |-- id: long (nullable = false)
    '''

    # Adding a New Column with the help of Existing One
    '''
    We will add a new Date column with the Help of day,month and year column
    method 1: Create an expression to concat the day, month and year field
    and convert it into the Date data type
    concat(day,'/',month,'/',year) --> This is a string so we will convert it into date using to_date
    to_date(concat(day,'/',month,'/',year),'d/M/y') --> this is an expression so use expr
    expr("to_date(concat(day,'/',month,'/',year),'d/M/y')")
    OR
    to_date(expr("concat(day,'/',month,'/',year)"),'d/M/y'))
    OR
    expr("""
    to_date(concat(day,'/',month,'/',year),'d/M/y')
    """)
    All are correct
    '''

    df4 = df3.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))

    df4.show()

    '''
    +------+---+-----+----+-----------+----------+
    |  Name|day|month|year|         id|       dob|
    +------+---+-----+----+-----------+----------+
    |  Ravi| 28|    1|2002|          0|2002-01-28|
    |Sumeet| 23|    5|  81| 8589934592|0081-05-23|
    |  Amit| 12|   12|   6| 8589934593|0006-12-12|
    |Sumeet| 23|    5|  81|17179869184|0081-05-23|
    |  Rosy|  7|    8|  63|17179869185|0063-08-07|
    +------+---+-----+----+-----------+----------+
    '''

    df5 = df3.withColumn("dob", expr("""
    to_date(concat(day,'/',month,'/',year),'d/M/y')""")) \
        .drop("day","month","year")

    print("This is new")
    df5.show()

    '''
    +------+-----------+----------+
    |  Name|         id|       dob|
    +------+-----------+----------+
    |  Ravi|          0|2002-01-28|
    |Sumeet| 8589934592|0081-05-23|
    |  Amit| 8589934593|0006-12-12|
    |Sumeet|17179869184|0081-05-23|
    |  Rosy|17179869185|0063-08-07|
    +------+-----------+----------+
    '''

    '''
    Since we have a new date column, we can drop the existing day,month and year columns
    '''
    df4.drop("day","month","year").show()

    '''
    +------+-----------+----------+
    |  Name|         id|       dob|
    +------+-----------+----------+
    |  Ravi|          0|2002-01-28|
    |Sumeet| 8589934592|0081-05-23|
    |  Amit| 8589934593|0006-12-12|
    |Sumeet|17179869184|0081-05-23|
    |  Rosy|17179869185|0063-08-07|
    +------+-----------+----------+
    '''

    '''
    Removing Duplicates column from the Dataframe
    With the Function 'dropDuplicates()' we can remove the duplicate column
    from the Dataframe 
    '''

    '''
    Dropping Duplicate.
    Since we have ID as Unique, we will first remove that column and then drop the Duplicate
    '''
    dfu = df4 \
        .drop("id") \
        .dropDuplicates()

    dfu.show()

    '''
    +------+---+-----+----+----------+
    |  Name|day|month|year|       dob|
    +------+---+-----+----+----------+
    |  Ravi| 28|    1|2002|2002-01-28|
    |  Rosy|  7|    8|  63|0063-08-07|
    |Sumeet| 23|    5|  81|0081-05-23|
    |  Amit| 12|   12|   6|0006-12-12|
    +------+---+-----+----+----------+
    '''

    '''
    Dropping Duplicate column based on the Specific Field
    This approach will help to delete the duplicate value based on the Specific column
    '''

    df4.show()

    '''
    +------+---+-----+----+-----------+----------+
    |  Name|day|month|year|         id|       dob|
    +------+---+-----+----+-----------+----------+
    |  Ravi| 28|    1|2002|          0|2002-01-28|
    |Sumeet| 23|    5|  81| 8589934592|0081-05-23|
    |  Amit| 12|   12|   6| 8589934593|0006-12-12|
    |Sumeet| 23|    5|  81|17179869184|0081-05-23|
    |  Rosy|  7|    8|  63|17179869185|0063-08-07|
    +------+---+-----+----+-----------+----------+
    '''

    dfn = df4.dropDuplicates(['name'])

    dfn.show()

    '''
    +------+---+-----+----+-----------+----------+
    |  Name|day|month|year|         id|       dob|
    +------+---+-----+----+-----------+----------+
    |  Ravi| 28|    1|2002|          0|2002-01-28|
    |  Rosy|  7|    8|  63|17179869185|0063-08-07|
    |Sumeet| 23|    5|  81| 8589934592|0081-05-23|
    |  Amit| 12|   12|   6| 8589934593|0006-12-12|
    +------+---+-----+----+-----------+----------+
    '''

    df4.dropDuplicates(["name","month"]).show()

    jsonDF = spark.read \
        .format("json") \
        .load("dataDir/Invoice-set1.json")

    jsonDF.printSchema()

    '''
    Flattened the Json Data
    '''

    jsonDF.selectExpr("DeliveryAddress.AddressLine as AddressLine",
                      "DeliveryAddress.City as City",
                      "DeliveryAddress.ContactNumber as ContactNumber",
                      "DeliveryAddress.PinCode as PinCode",
                      "DeliveryAddress.State as State").show()

    '''
    +--------------------+-------------+-------------+-------+--------------+
    |         AddressLine|         City|ContactNumber|PinCode|         State|
    +--------------------+-------------+-------------+-------+--------------+
    |                null|         null|         null|   null|          null|
    |   444-1842 Dui. Rd.|    Shivapuri|   7243866404| 561012|Madhya Pradesh|
    |                null|         null|         null|   null|          null|
    |2465 Laoreet, Street|        Dehri|   2662305605| 637308|         Bihar|
    |        7114 Eu, Rd.|       Ratlam|   4057182350| 925281|Madhya Pradesh|
    |  517-8912 Nulla St.|    Champdani|   8183195143| 680616|   West Bengal|
    |                null|         null|         null|   null|          null|
    |     5418 Magna. Rd.|      Chennai|   6557358508| 386032|    Tamil Nadu|
    |                null|         null|         null|   null|          null|
    |Flat No. #210-902...|South Dum Dum|   7508353683| 504795|   West Bengal|
    |                null|         null|         null|   null|          null|
    |  295-7690 At Street| Shahjahanpur|   4624129756| 228410| Uttar Pradesh|
    |                null|         null|         null|   null|          null|
    |                null|         null|         null|   null|          null|
    |    7947 Mauris, Av.|     Tambaram|   5898142373| 339533|    Tamil Nadu|
    |Flat No. #316-562...|      Nellore|   5232382321| 333433|Andhra Pradesh|
    |House No 534, 997...|      Bellary|   7652091989| 761574|     Karnataka|
    |          773 Eu Ave|      Sitapur|   9934440762| 431667| Uttar Pradesh|
    |                null|         null|         null|   null|          null|
    |                null|         null|         null|   null|          null|
    +--------------------+-------------+-------------+-------+--------------+
    only showing top 20 rows
    '''
