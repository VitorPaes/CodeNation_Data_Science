from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest, col,when,
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("teste").getOrCreate()

inicio_semestre = "01/01/2020"
fim_semestre = "01/06/2020"

df = spark.sql("SELECT * FROM db_pontos_anuais.tb_pontuacao_geral)

df = df.filter(col("data_ponto").between(inicio_semestre,fim_semestre))

df = df.grouby("nome_casa","nome_aluno").agg(sum("valor_ponto").alias("pontos_gerais")


df_grifinória = df.where("nome_casa" == "Grifinória")

df_sonserina = df.where("nome_casa" == "Sonserina")

df_corvinal = df.where("nome_casa" == "Corvinal")

df_lufalufa = df.where("nome_casa" == "Lufa-Lufa")


df_grifinória_maior = df_grifinória.groupby("nome_aluno").max('pontos_gerais').collect()[0].asDict()['Maior']

df_corvinal_maior = df_sonserina.groupby("nome_aluno").max('pontos_gerais').collect()[0].asDict()['Maior']

df_lufalufa_marior = df_corvinal.groupby("nome_aluno").max('pontos_gerais').collect()[0].asDict()['Maior']

df_sonserina_marior = df_lufalufa.groupby("nome_aluno").max('pontos_gerais').collect()[0].asDict()['Maior']


df_grifinória_menor = df_grifinória.groupby("nome_aluno").min('pontos_gerais').collect()[0].asDict()['Maior']

df_corvinal_menor = df_sonserina.groupby("nome_aluno").min('pontos_gerais').collect()[0].asDict()['Maior']

df_lufalufa_menor = df_corvinal.groupby("nome_aluno").min('pontos_gerais').collect()[0].asDict()['Maior']

df_sonserina_menor = df_lufalufa.groupby("nome_aluno").min('pontos_gerais').collect()[0].asDict()['Maior']


print("Grifinória: Maior" + str(df_grifinória_maior) + "Menor" + str(df_grifinória_menor))
print("Sonserina: Maior" + str(df_corvinal_maior) + "Menor" + str(df_corvinal_menor))
print("Corvinal: Maior" + str(df_lufalufa_marior) + "Menor" + str(df_lufalufa_menor))
print("Lufa: Maior" + str(df_sonserina_marior) + "Menor" + str(df_sonserina_menor))

Pontos_totais = df.grouby("nome_casa").agg(sum("pontos_gerais").alias("pontos_gerais")

Pontos_totais.show(truncate = False)


