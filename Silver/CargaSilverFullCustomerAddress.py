# Importa SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max
from delta.tables import DeltaTable

# Define o Spark Session a ser executado em node específico, pois o job será executado via linha de comando com o spark-submit

spark = (
    SparkSession
    .builder
    .appName("CargaSilverFullCustomerAddress")
    .config("master", "spark://192.168.0.144:7077")
    .getOrCreate())

sc = spark.sparkContext

# Define caminho da origem e instancia df, o cacheando para posterior reutilização
pathBronze = "hdfs:///user/hadoop/miniprojeto5/bronze/customeraddress"
dfBronze = spark.read.parquet(pathBronze)
#dfBronze.show()
dfBronze.cache()

# Obtém registros com última atualização
dfLast = dfBronze.groupBy("customerid","addressid").agg(_max("modifieddate")).withColumnRenamed("max(modifieddate)", "modifieddate")
matchClause = [dfBronze["customerid"] == dfLast["customerid"] , dfBronze["addressid"] == dfLast["addressid"] ,dfBronze["modifieddate"] == dfLast["modifieddate"]]
dfLastRecord = dfBronze.join(dfLast,on = matchClause, how="leftsemi")

# Define propriedades e realiza load do arquivo
pathSilver = "hdfs:///user/hadoop/miniprojeto5/silver/customeraddress"
formatSilver = "delta"
partitionColumnSilver = "addresstypeid"
wmodeSilver = "overwrite"

#dfLastRecord = dfLastRecord.limit(10)

dfLastRecord.write.mode(wmodeSilver).format(formatSilver).partitionBy(partitionColumnSilver).save(pathSilver)

# Comandos para testes
#teste = spark.read.format("delta").load(pathSilver)
#teste.count()
#teste.show()
