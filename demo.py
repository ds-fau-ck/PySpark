#The basic abstraction of spark is RDD->This is a type of Data Structure available in the spark which will hold your actual data. 
"""abstraction->This is able to handle any type of data----Semistructured, unstructured, completely unstructered data. To handle this kind of data in spark we have concept of Data Frame.
Data Fram is also a RDD but it is just wraper on rdd which will only capable enough to hold the structured data only row columnar data but everything in spark, that object which is holding some data
and will be know as RDD and it will follow all those properties."""
"""The basic abstraction of Spark is RDD. This is a type of data structure available in Spark that will hold your actual data.
abstraction: This is able to handle any type of data—semistructured, unstructured, or completely unstructured data.
To handle this kind of data in Spark, we have the concept of a data frame.
Data Fram is also a RDD, but it is just a wrapper on the RDD, which will only be capable of holding the structured data, only row-column data,
but everything in Spark, that object that is holding some data.and will be known as RDD, and it will follow all those properties."""
#necessary libraries of pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


if __name__ == '__main__':
    #Creating spark session
    #SparkSession->This will be treated as a entry point for our spark application which will help to connect our application with the cluster.
    spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").getOrCreate()
    #Spark context available as 'sc' (master = local[*], app id = local-1705557914925).
    #master=local[*]->what is the meaning of * here?---Lets say we have a machine with 8 core and on that machine i have initiated my pyspark shell.
    # If i mention the local[*]->* ultimately means leats say your machine has a 16 core so all 16 core will be used while executing any step ----transformation
    #spark session parameters->
    #Create list of data to prepare data frame
    person_list = [("Berry","","Allen",1,"M"),
        ("Oliver","Queen","",2,"M"),
        ("Robert","","Williams",3,"M"),
        ("Tony","","Stark",4,"F"),
        ("Rajiv","Mary","Kumar",5,"F")
    ]
    

    #defining schema for dataset
    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", IntegerType(), True), \
        StructField("gender", StringType(), True), \
      
    ])
    
    #creating spark dataframe
    df = spark.createDataFrame(data=person_list,schema=schema)

    #Printing data frame schema
    df.printSchema()

    #Printing data
    df.show(truncate=False)

    #Writing file in hadoop
    df.write.csv("record.csv")
