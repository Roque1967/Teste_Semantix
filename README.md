# Instruções

1) Baixar os arquivos das seguintes URLs:
	Jul 01 to Jul 31, ASCII format, 20.7 MB gzip
	Aug 04 to Aug 31, ASCII format, 21.8 MB gzip

2) Criar diretório no HFDS
hdfs dfs -mkdir -p /user/HOME/teste/

3) Copiar os arquivos baixados para o diretório HFS criado no passo 2
	hdfs dfs -put NASA_access_log_Jul95.gz /user/HOME/teste/ 
	hdfs dfs -put NASA_access_log_Aug95.gz /user/HOME/teste/

4) Gerar tabela externa, utilizando o diretório HDFS do passo 3, conforme segue:

```drop table owner.HOME_teste;
create external table onwer.HOME_teste ( 
coluna_1 string,
coluna_2 string,
coluna_3 string,
coluna_4 string,
coluna_5 string,
coluna_6 string,
coluna_7 string,
coluna_8 string,
coluna_9 string,
coluna_10 string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS textfile LOCATION '/user/HOME/teste';```

4) Gerar tabela HIVE com as coluna que serão utilizadas na solução, conforme segue:

```create table owner.HOME_base as
select coluna_1 as host,
concat(coluna_4, coluna_5) as dt_requisicao,
concat(coluna_6, coluna_7, coluna_8) as desc_requisicao,
coluna_9 as retorno_HTTP,
cast(coluna_10 as BIGINT) as tot_bytes_ret
from owner.HOME_teste;```

5) Entrar no ambiente Spark.

6) Executar o script Teste.py para obter nos logs as respostas do desafio. 

7) Logs obtidos na execução do Teste.py
