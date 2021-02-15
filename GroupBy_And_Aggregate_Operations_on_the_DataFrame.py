'''
Grouping the data based on the Columns and performing Aggregate Functions over it.
Aggregate Functions like sum,avg,min,max,count,mean etc
'''

from pyspark.sql import SparkSession

from pyspark.sql.functions import sum, avg, max, min, mean, count, col

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("grouping and Aggregation") \
        .master("local[3]") \
        .getOrCreate()

    simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
                  ("Michael", "Sales", "NY", 86000, 56, 20000),
                  ("Robert", "Sales", "CA", 81000, 30, 23000),
                  ("Maria", "Finance", "CA", 90000, 24, 23000),
                  ("Raman", "Finance", "CA", 99000, 40, 24000),
                  ("Scott", "Finance", "NY", 83000, 36, 19000),
                  ("Jen", "Finance", "NY", 79000, 53, 15000),
                  ("Jeff", "Marketing", "CA", 80000, 25, 18000),
                  ("Kumar", "Marketing", "NY", 91000, 50, 21000)
                  ]

    schema = ["employee_name", "department", "state", "salary", "age", "bonus"]

    df = spark.createDataFrame(data=simpleData, schema=schema)
    df.printSchema()
    
    '''
        root
     |-- employee_name: string (nullable = true)
     |-- department: string (nullable = true)
     |-- state: string (nullable = true)
     |-- salary: long (nullable = true)
     |-- age: long (nullable = true)
     |-- bonus: long (nullable = true)
    '''
     
    
    df.show(truncate=False)
    
    '''
    +-------------+----------+-----+------+---+-----+
    |employee_name|department|state|salary|age|bonus|
    +-------------+----------+-----+------+---+-----+
    |James        |Sales     |NY   |90000 |34 |10000|
    |Michael      |Sales     |NY   |86000 |56 |20000|
    |Robert       |Sales     |CA   |81000 |30 |23000|
    |Maria        |Finance   |CA   |90000 |24 |23000|
    |Raman        |Finance   |CA   |99000 |40 |24000|
    |Scott        |Finance   |NY   |83000 |36 |19000|
    |Jen          |Finance   |NY   |79000 |53 |15000|
    |Jeff         |Marketing |CA   |80000 |25 |18000|
    |Kumar        |Marketing |NY   |91000 |50 |21000|
    +-------------+----------+-----+------+---+-----+
    '''

    '''
    Get the Sum of Salary of Each Department or Department wise Total Salary.
    For this we will have to first Group the Data based on the Department and then use sum to get the Salary
    '''

    df.groupBy("department").sum("salary").show()
    
    '''
    +----------+-----------+
    |department|sum(salary)|
    +----------+-----------+
    |     Sales|     257000|
    |   Finance|     351000|
    | Marketing|     171000|
    +----------+-----------+
    '''

    '''
    Get the Average Salary of each Department or Department wise average Salary.
    '''

    df.groupBy("department").avg("salary").show()
    
    '''
    +----------+-----------------+
    |department|      avg(salary)|
    +----------+-----------------+
    |     Sales|85666.66666666667|
    |   Finance|          87750.0|
    | Marketing|          85500.0|
    +----------+-----------------+
    '''

    '''
    #Get the Number of Employee in Each Department or Department wise Number of Employees
    #Apply GroupBy on the Department and then use count to get the Number of Employee
    '''

    df.groupBy("department").count().show()
    
    '''
    +----------+-----+
    |department|count|
    +----------+-----+
    |     Sales|    3|
    |   Finance|    4|
    | Marketing|    2|
    +----------+-----+
    '''

    '''
    Get the Department wise min/max salary
    '''

    df.groupBy("department").min("salary").show()
    '''
    +----------+-----------+
    |department|min(salary)|
    +----------+-----------+
    |     Sales|      81000|
    |   Finance|      79000|
    | Marketing|      80000|
    +----------+-----------+
    '''

    df.groupBy("department").max('salary').show()
    '''
    +----------+-----------+
    |department|max(salary)|
    +----------+-----------+
    |     Sales|      90000|
    |   Finance|      99000|
    | Marketing|      91000|
    +----------+-----------+
    '''

    df.groupBy("department").agg(
        min("salary"),
        max("salary")
    ).show()

    '''
    +----------+-----------+-----------+
    |department|min(salary)|max(salary)|
    +----------+-----------+-----------+
    |     Sales|      81000|      90000|
    |   Finance|      79000|      99000|
    | Marketing|      80000|      91000|
    +----------+-----------+-----------+
    '''

    '''
    GroupBy and Aggregate Function on multiple columns
    Get the Department wise Total salary and Total Bonus in each State
    '''

    df.groupBy("department", "state") \
        .sum("salary", "bonus") \
        .show()
    
    '''
    +----------+-----+-----------+----------+
    |department|state|sum(salary)|sum(bonus)|
    +----------+-----+-----------+----------+
    |   Finance|   NY|     162000|     34000|
    | Marketing|   NY|      91000|     21000|
    |     Sales|   CA|      81000|     23000|
    | Marketing|   CA|      80000|     18000|
    |   Finance|   CA|     189000|     47000|
    |     Sales|   NY|     176000|     30000|
    +----------+-----+-----------+----------+
    '''

    '''
    Performing Multiple Aggregation at a Time
    To perform multiple aggregation all at a time, We can use agg()
    function.
    Example: Department wise min,max,avg and total(sum) salary
    '''

    df.groupBy("department") \
        .agg(sum("salary").alias("sum_salary"),
             avg("salary").alias("avg_salary"),
             sum("bonus").alias("sum_bonus"),
             max("bonus").alias("max_bonus"),
             min("salary").alias("minSalary"),
             mean("salary").alias("MeanValue"))\
        .show(truncate=False)
    '''
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|minSalary|MeanValue        |
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    |Sales     |257000    |85666.66666666667|53000    |23000    |81000    |85666.66666666667|
    |Finance   |351000    |87750.0          |81000    |24000    |79000    |87750.0          |
    |Marketing |171000    |85500.0          |39000    |21000    |80000    |85500.0          |
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    '''


    '''
    Using Filter on Aggregate Data
    Find the Department wise total salary,average salary,total Bonus, maximum bonus
    Minimum Salary where total bonus is Greater than 50000.
    '''

    df.groupBy("department") \
        .agg(sum("salary").alias("sum_salary"),
             avg("salary").alias("avg_salary"),
             sum("bonus").alias("sum_bonus"),
             max("bonus").alias("max_bonus"),
             min("salary").alias("minSalary"),
             mean("salary").alias("MeanValue")) \
        .where(col("sum_bonus") > 50000) \
        .show(truncate=False)
    '''
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|minSalary|MeanValue        |
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    |Sales     |257000    |85666.66666666667|53000    |23000    |81000    |85666.66666666667|
    |Finance   |351000    |87750.0          |81000    |24000    |79000    |87750.0          |
    +----------+----------+-----------------+---------+---------+---------+-----------------+
    '''
