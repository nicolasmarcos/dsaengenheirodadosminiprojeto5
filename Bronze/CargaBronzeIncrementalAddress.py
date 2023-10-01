# Importa SparkSession
from pyspark.sql import SparkSession

# Define o Spark Session a ser executado em node específico, pois o job será executado via linha de comando com o spark-submit

spark = (
    SparkSession
    .builder
    .appName("CargaBronzeIncrementalAddress")
    .config("master", "spark://192.168.0.144:7077")
    .getOrCreate())

sc = spark.sparkContext

# Define propriedade de conexão do database
url = "jdbc:mysql://192.168.0.144/adventureworks"
properties = {"user": "hadoop", "password":"Dsahadoop@1", "driver":"com.mysql.cj.jdbc.Driver"}
table_name = "address"

# Cria DF com base nos dados da origem
dfOrigin = spark.read.jdbc(url, table_name, properties=properties)
# dfOrigin.show()
dfTarget = spark.read.parquet("hdfs:///user/hadoop/miniprojeto5/bronze/address")
# dfTarget.show()

# Instancia-se condição de match e filtro de join que considerará apenas os registros que existem na origem, mas não no destino
matchClause = [dfOrigin["addressid"] == dfTarget["addressid"], dfOrigin["modifieddate"] == dfTarget["modifieddate"]]
filterCondition = (dfTarget["addressid"].isNull())

dfInsert = dfOrigin.join(dfTarget, on=matchClause, how="leftanti")

# Define propriedades do arquivo a ser salvo
path = "hdfs:///user/hadoop/miniprojeto5/bronze/address"
format = "parquet"
partitionColumn = "stateprovinceid"
wmode = "append"

# Realiza load dos dados
dfInsert.write.mode(wmode).format(format).partitionBy(partitionColumn).save(path)

# Comandos para testes:
# teste = spark.read.parquet(path)
# teste.count()
# teste.show()