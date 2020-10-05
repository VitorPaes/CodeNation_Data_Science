from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, expr, concat, lit, col, broadcast, trim, length,lpad, sum
from pyspark.sql.types import *
from decimal import Decimal
def corrigir_nomes(nome):
    nome = nome.replace('.', '').replace('ç', 'c').replace('ô', 'o').replace('é', 'e').replace('á', 'a').replace('í', 'i')
    return nome
def unionall(*dfs):
    print("Função para unir dataframes em um")
    return reduce(DataFrame.unionAll, dfs)
def normalizer(df_entrada, mes, ano):
    base_nome = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/glossario_de_aerodromo.csv',header=True,sep=';')
    mes_ref = mes
    ano_ref = ano
    
    base_nome = base_nome.withColumnRenamed('Sigla OACI','SIGLA')
    drops = ['Codigo DI','Codigo Tipo Linha','Grupo DI','Partida Real','Partida Prevista', 'Chegada Prevista','Chegada Real']
    df_entrada = df_entrada.drop(*drops)
    df_entrada = df_entrada.withColumnRenamed('ICAO Empresa Aerea','Empresa_Area')\
        .withColumnRenamed('Numero Voo','ID_VOO').withColumnRenamed('ICAO Aerodromo Origem','Origem').withColumnRenamed('ICAO Aerodromo Destino','Destino')
    df_join = df_entrada.join(base_nome,trim(df_entrada['Origem'])== trim(base_nome['SIGLA']),'left')
    drop2 = ['SIGLA','Continente']
    df_join = df_join.drop(*drop2)
    df_join = df_join.withColumn("Origem",df_join["Cidade"])
    base_nome = base_nome.select([col(field).alias(fiel+'_dest')
            for(field in base_nome.schema.names)])
    df_join = df_join.join(base_nome,trim(df_join['Destino'])== trim(base_nome['SIGLA_dest']),'left')
    drop2 = ['SIGLA_dest','Continente_dest']
    df_join = df_join.drop(*drop2)
    df_join = df_join.withColumn("Destino",df_join["Cidade_dest"])
    df_join = df_join.withColumn('Ano',lit(ano_ref))
    df_join = df_join.withColumn('MES',lit(mes_ref))
    print('Quantidade de voos concelados em {} mes {} QTD: *{}*'.format(ano_ref,mes_ref, df_join.filter(col('Situacao Voo') == 'CANCELADO').count()))
    return df_join
    
df  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\ANAC_VRA_05_2019.csv',header=True,sep=';')
df1  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_012019.csv',      header=True,sep=';')
df2  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_022019.csv',      header=True,sep=';')
df3  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_032019.csv',      header=True,sep=';')
df4  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_062019.csv',      header=True,sep=';')
df5  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_072019.csv',      header=True,sep=';')
df6  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\VRA_082019.csv',      header=True,sep=';')
df7  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\vra_092019.csv',      header=True,sep=';')
df8  = spark.read.csv('D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2019\\VRA_AbriL_2019.csv',  header=True,sep=';')
df_2019 = unionall(df , df2 , df3 , df4 , df5 , df6 , df7 , df8)

df9 = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\vra_012020.csv', header=True,sep=';')
df10  = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\VRA_022020.csv', header=True,sep=';')
df11  = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\vra_032020.csv', header=True,sep=';')
df12  = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\vra_042020.csv', header=True,sep=';')
df13  = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\vra_05_2020.csv',header=True,sep=';')
df14  = spark.read.csv'D://Documentos/hack_snt/hackathon_stn/dataset/base_voos/2020\\VRA_06_2020.csv',header=True,sep=';') 

df_2020 = unionall(df , df2 , df3 , df4 , df5 , df6 , df7 , df8)

df_final_1 = normalizer(df8, 'abril', '2020')
df_final_2 = normalizer(df8, 'abril', '2019')
df_final_3 = normalizer(df8, 'marco', '2019')
df_final_4 = normalizer(df8, 'marco', '2020')

df_final_1.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2020_04.csv",header = 'true')
group1 = df_final_1.groupby(col('Pais_dest')).count()
group1.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2020_04_group.csv",header = 'true')

df_final_2.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2019_04.csv",header = 'true')
group2 = df_final_2.groupby(col('Pais_dest')).count()
group2.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2019_04_group.csv",header = 'true')

df_final_3.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2019_03.csv",header = 'true')
group3 = df_final_3.groupby(col('Pais_dest')).count()
group3.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2019_03_group.csv",header = 'true')

df_final_4.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2020_03.csv",header = 'true')
group4 = df_final_4.groupby(col('Pais_dest')).count()
group4.repartition(1).write.format('com.databricks.spark.csv').mode('overwrite').save("D://Documentos/hack_snt/hackathon_stn/dataset/saida/2020_03_group.csv",header = 'true')

