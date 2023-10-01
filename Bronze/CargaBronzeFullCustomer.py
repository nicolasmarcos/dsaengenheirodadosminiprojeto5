# Importa SparkSession
from pyspark.sql import SparkSession


# Define o Spark Session a ser executado em node específico, pois o job será executado via linha de comando com o spark-submit

spark = (
    SparkSession
    .builder
    .appName("CargaBronzeFullCustomer")
    .config("master", "spark://192.168.0.144:7077")
    .getOrCreate())

sc = spark.sparkContext

# Define propriedade de conexão do database
url = "jdbc:mysql://192.168.0.144/adventureworks"
properties = {"user": "hadoop", "password":"Dsahadoop@1", "driver":"com.mysql.cj.jdbc.Driver"}
table_name = "customer"

# Cria DF com base nos dados
dfCustomer = spark.read.jdbc(url, table_name, properties=properties)
# Comandos para testes:
#dfCustomer.show()
#dfCustomer = dfCustomer.limit(10)
#dfCustomer.show()

# Define propriedades do arquivo a ser salvo
path = "hdfs:///user/hadoop/miniprojeto5/bronze/customer"
format = "parquet"
partitionColumn = "territoryid"
wmode = "overwrite"

# Realiza load dos arquivos
dfCustomer.write.mode(wmode).format(format).partitionBy(partitionColumn).save(path)