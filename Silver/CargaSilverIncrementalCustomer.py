# Importa SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max
from delta.tables import DeltaTable

# Define o Spark Session a ser executado em node específico, pois o job será executado via linha de comando com o spark-submit

spark = (
    SparkSession
    .builder
    .appName("CargaSilverIncrementalCustomer")
    .config("master", "spark://192.168.0.144:7077")
    .getOrCreate())

sc = spark.sparkContext

# Define caminho da origem e instancia df, o cacheando para posterior reutilização
pathBronze = "hdfs:///user/hadoop/miniprojeto5/bronze/customer"
dfBronze = spark.read.parquet(pathBronze)
#dfBronze.show()
dfBronze.cache()

# Obtém registros com última atualização
dfLast = dfBronze.groupBy("customerid").agg(_max("modifieddate")).withColumnRenamed("max(modifieddate)", "modifieddate")
matchClause = [dfBronze["customerid"] == dfLast["customerid"] , dfBronze["modifieddate"] == dfLast["modifieddate"]]
dfLastRecord = dfBronze.join(dfLast,on = matchClause, how="leftsemi")
dfLastRecord.cache()
dfBronze.unpersist()

# Obtém registros da camada silver para carga incremental
pathSilver = "hdfs:///user/hadoop/miniprojeto5/silver/customer"
dfSilver = DeltaTable.forPath(spark,pathSilver) 

# Define cláusulas de update e insert
updateMatch = "upsert.customerid = target.customerid"
update = {"accountnumber": "upsert.accountnumber", "customertype": "upsert.customertype", "rowguid": "upsert.rowguid", "modifieddate": "upsert.modifieddate", "territoryid": "upsert.territoryid"}
insert = {"customerid":"upsert.customerid","accountnumber": "upsert.accountnumber", "customertype": "upsert.customertype", "rowguid": "upsert.rowguid", "modifieddate": "upsert.modifieddate", "territoryid": "upsert.territoryid"}

# Realiza load da tabela
dfSilver.alias("target").merge(dfLastRecord.alias("upsert"), updateMatch).whenMatchedUpdate(set=update).whenNotMatchedInsert(values=insert).execute()

# Comandos para testes
#teste = spark.read.format("delta").load(pathSilver)
#teste.count()
#teste.show()

