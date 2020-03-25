#!/usr/bin/python
# coding: utf-8
# IMPORTs
from pyspark import SparkContext, StorageLevel # SparkConf
from pyspark.sql import HiveContext, SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# from pyspark.sql import functions as sqlfunc
# from pyspark import StorageLevel

#class IptvSession:
    # Obtendo o Spark contexto e a sessão do spark.
print("Obtendo contexto Spark...")
sc = SparkContext(appName=None)
    # Set log level
sc.setLogLevel('ERROR')
sparkSession = SparkSession.builder.appName("Teste").enableHiveSupport().getOrCreate()

def lerBase5():
	sparkSession.sql("select sum(tot_bytes_ret) tot_bytes_ret from p_dbm_db.80623286_base").persist(StorageLevel(True, True, False, False, 2)).createOrReplaceTempView("base5")

def lerBase4():
	sparkSession.sql("select substr(dt_requisicao,2,11) data, count(*) qtde_erros from p_dbm_db.80623286_base where retorno_HTTP = 404 group by substr(dt_requisicao,2,11)").persist(StorageLevel(True, True, False, False, 2)).createOrReplaceTempView("base4")
	lerBase5()	

def lerBase3():
	sparkSession.sql("select x.desc_requisicao, x.qtde_erros from (select desc_requisicao, count(*) qtde_erros from p_dbm_db.80623286_base group by desc_requisicao )x SORT BY x.qtde_erros DESC LIMIT 5").persist(StorageLevel(True, True, False, False, 2)).createOrReplaceTempView("base3")
	lerBase4()

def lerBase2():
	sparkSession.sql("select 'Qtde de erros 404 = ', count(*) from p_dbm_db.80623286_base where retorno_HTTP = 404").persist(StorageLevel(True, True, False, False, 2)).createOrReplaceTempView("base2")
	lerBase3()
	
def lerBase1():
    sparkSession.sql("select 'Hosts distintos = ', count(distinct host)  from p_dbm_db.80623286_base").persist(StorageLevel(True, True, False, False, 2)).createOrReplaceTempView("base1")
    lerBase2()	

# 1. Número de hosts únicos.
sqlDF = spark.sql("SELECT * FROM base1")
sqlDF.show()

#2. O total de erros 404.
sqlDF = spark.sql("SELECT * FROM base2")
sqlDF.show()

#3. Os 5 URLs que mais causaram erro 404.
sqlDF = spark.sql("SELECT * FROM base3")
sqlDF.show()

#4. Quantidade de erros 404 por dia.
sqlDF = spark.sql("SELECT * FROM base4")
sqlDF.show()

#5. O total de bytes retornados.
sqlDF = spark.sql("SELECT * FROM base5")
sqlDF.show()

if __name__ == "__main__":
    lerBase1()


