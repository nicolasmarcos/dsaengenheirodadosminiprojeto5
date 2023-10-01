# Importa SparkSession
from pyspark.sql import SparkSession


# Define o Spark Session a ser executado em node específico, pois o job será executado via linha de comando com o spark-submit

spark = (
    SparkSession
    .builder
    .appName("CargaBronzeFullCustomerAddress")
    .config("master", "spark://192.168.0.144:7077")
    .getOrCreate())

sc = spark.sparkContext

# Define propriedade de conexão do database
url = "jdbc:mysql://192.168.0.144/adventureworks"
properties = {"user": "hadoop", "password":"Dsahadoop@1", "driver":"com.mysql.cj.jdbc.Driver"}
table_name = "customeraddress"

# Cria DF com base nos dados
dfCustomerAddress = spark.read.jdbc(url, table_name, properties=properties)
#dfCustomerAddress.show()
dfCustomerAddress = dfCustomerAddress.limit(10)
#dfCustomerAddress.show()

# Define propriedades do arquivo a ser salvo
path = "hdfs:///user/hadoop/miniprojeto5/bronze/customeraddress"
format = "parquet"
partitionColumn = "addresstypeid"
wmode = "overwrite"

# Realiza load dos arquivos
dfCustomerAddress.write.mode(wmode).format(format).partitionBy(partitionColumn).save(path)