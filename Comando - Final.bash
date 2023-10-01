# Este script tem por objetivo nortear a execução dos jobs de carga no datalake das tabelas de clientes

# Etapa 0: Import nos dados no MySQL 

# Importa dados:
mysql -u root -p < ./AWBackup.sql
mysql -u root -p

# Realiza testes
show tables;
describe customer;
describe customer;

select * from customer limit 5;

# Cria usuário do hadoop e concede acesso remoto
use adventureworks
CREATE USER 'hadoop'@'%' IDENTIFIED BY 'Dsahadoop@1';
GRANT ALL PRIVILEGES ON adventureworks.* TO 'hadoop'@'%';

exit

# 1º Etapa: Carga da Bronze/Raw layer no HDFS com Sqoop

hdfs dfs -mkdir /user/hadoop/miniprojeto5
hdfs dfs -mkdir /user/hadoop/miniprojeto5/bronze
hdfs dfs -ls /user/hadoop/miniprojeto5/bronze

# 1.1: Carga das tabelas para o lake
# Será feita uma carga fria das tabelas com limitada quantidade de registros a fim de posteriormente testar estratégia da carga incremental
# Inicialmente, tentou-se import de todas as tabelas para a camada bronze, mas o sqoop apresentou dificuldade de compatibilidade com estrutura de algumas. Decidi então importar apenas as que atendem ao requisito.
# Depois, tentou-se utilizar o Sqoop para realização da carga incremental, contudo o as propriedades de check-column do Sqoop não suportam análise de incremento por mais de uma coluna, o que impactaria chaves primárias compostas e campos de data de modificação
# Em tempo, pela obrigatoriedade do Sqoop exigir tabelas atendendo formas normais, pensou-se que futuramente poderiam existir tabelas fora do padrão, derivadas de packages ou stored procedures cujas ingestões no lake fossem necessárias. Assim, optou-se por utilizar o PySpark
# As camadas bronze armazenam os dados em parquet. 
# A camada bronze foi implementada com processos de carga full, caso seja desejada uma carga geral e cargas incrementais que consideram os registros que estão na origem, mas não no lake.
# Uma vez que há data de modificação, esta camada foi implementada gravando histórico. Ou seja, se em uma data 1 o registro de um cliente X estava de um jeito e na data 2 estava de outro, o processo de carga manterá no lake o registro do dia 1 e gravará também a foto do dia 2. 
# A carga dos registros nas tabelas da camada silver consideram apenas a versão mais recente dos cadastros. Também foram criados processos de cargas full para uma primeira carga e carga incremental.
# Buscando atender cenários de insert e update, utilizou-se na criação das tabelas silver Delta Tables. Caso um registro advindo da origem (bronze) já exista no destino (silver), será feito update. Senão, update. Este fluxo não contempla casos de delete na origem. Se fosse o caso, optaria-se por cargas full.

# 1.1.1 Carga Customer 
# 1.1.1.1: Criado job para carga full da tabela customer

spark-submit CargaBronzeFullCustomer.py

# 1.1.1.2: Criado job para carga incremental da tabela customer

spark-submit CargaBronzeIncrementalCustomer.py

# 1.1.2 Carga CustomerAddress 

# 1.1.2.1: Criado job para carga full da tabela customeraddress

spark-submit CargaBronzeFullCustomerAddress.py

# 1.1.2.2: Criado job para carga incremental da tabela customeraddress

spark-submit CargaBronzeIncrementalCustomerAddress.py

# 1.1.3 Carga Address 

# 1.1.3.1: Criado job para carga full da tabela address

spark-submit CargaBronzeFullAddress.py

# 1.1.3.2: Criado job para carga incremental da tabela address

spark-submit CargaBronzeIncrementalAddress.py

# 2: Carga fatos e dimensões 

# Script para criação da camada silver
hdfs dfs -mkdir /user/hadoop/miniprojeto5/silver
hdfs dfs -ls /user/hadoop/miniprojeto5/silver

# 2.1: Carga das tabelas para camada silver

# 2.1.1: Carga dimensão cliente

# 2.1.1.1: Criado job para carga full da tabela customer

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverFullCustomer.py

# 2.1.1.2: Criado job para carga incremental da tabela customer

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverIncrementalCustomer.py

# 2.1.2: Carga dimensão customeraddress

# 2.1.2.1: Criado job para carga full da tabela customeraddress

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverFullCustomerAddress.py

# 2.1.2.2: Criado job para carga incremental da tabela customeraddress

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverIncrementalCustomerAddress.py

# 2.1.3: Carga dimensão address

# 2.1.3.1: Criado job para carga full da tabela address

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverFullAddress.py

# 2.1.3.2: Criado job para carga incremental da tabela address

spark-submit --packages io.delta:delta-core_2.12:2.0.2 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" CargaSilverIncrementalAddress.py

# 3: Inserts no MySQL para reexecução do script e teste da carga incremental
#INSERT INTO `adventureworks`.`address` VALUES  (32522,'1970 Napa Ct.',NULL,'Bothell',79,'98011',0x0DCBAD9ACF363F4884D8585C2D4EC6E9,'1998-01-04 00:00:00');
#INSERT INTO `adventureworks`.`customer` VALUES  (29484,1,'AW00000001','S',0x5EE95A3F7DB8ED4A95B4C3797AFCB74F,'2004-10-13 11:15:07');
#INSERT INTO `adventureworks`.`customeraddress` VALUES  (29484,32522,3,0x74254F31751F7F459BD174D1CE53DAA5,'2001-08-01 00:00:00');
#commit;