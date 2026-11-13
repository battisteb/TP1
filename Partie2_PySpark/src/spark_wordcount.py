"""Comptage de mots avec Apache Spark (PySpark) : quatre implémentations.

  1. rdd_reduce   : API RDD, flatMap -> map -> reduceByKey   (agrégation locale avant le shuffle, comme un combiner)
  2. rdd_group    : API RDD, flatMap -> map -> groupByKey -> somme (pas d'agrégation locale : comme le pseudo-MapReduce)
  3. dataframe    : API DataFrame, split -> explode -> groupBy().count()  (optimiseur Catalyst, exécution dans la JVM)
  4. sql          : même requête écrite en Spark SQL

Usage :
    python src/spark_wordcount.py data/mots_N10000_m100.txt --methode dataframe --master "local[*]"
"""
import argparse
import os
import sys
import time

# Sous Windows, Spark lance des workers Python : on lui indique l'interpréteur courant.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# Spark 4 exige Java 17 ou 21 : on utilise le JDK 17 s'il est installé à l'emplacement standard.
_JDK17 = r"C:\Program Files\Java\jdk-17"
if "JAVA_HOME" not in os.environ and os.path.isdir(_JDK17):
    os.environ["JAVA_HOME"] = _JDK17

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def creer_session(master="local[*]", nom="wordcount"):
    spark = (SparkSession.builder
             .master(master)
             .appName(nom)
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")   # 200 par défaut : beaucoup trop pour ~40 mots distincts
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _lignes(spark, fichier):
    sc = spark.sparkContext
    # au moins une partition par cœur disponible, sinon les petits fichiers n'utilisent qu'1 ou 2 cœurs
    return sc.textFile(fichier, minPartitions=sc.defaultParallelism)


def wordcount_rdd_reduce(spark, fichier):
    return dict(_lignes(spark, fichier)
                .flatMap(lambda ligne: ligne.split())
                .map(lambda mot: (mot, 1))
                .reduceByKey(lambda a, b: a + b)
                .collect())


def wordcount_rdd_group(spark, fichier):
    return dict(_lignes(spark, fichier)
                .flatMap(lambda ligne: ligne.split())
                .map(lambda mot: (mot, 1))
                .groupByKey()
                .mapValues(sum)
                .collect())


def wordcount_dataframe(spark, fichier):
    mots = (spark.read.text(fichier)   # découpé en ~1 partition par cœur (spark.sql.files.minPartitionNum)
            .select(F.explode(F.split(F.col("value"), r"\s+")).alias("mot"))
            .where(F.col("mot") != ""))
    return {r["mot"]: r["count"] for r in mots.groupBy("mot").count().collect()}


def wordcount_sql(spark, fichier):
    spark.read.text(fichier).createOrReplaceTempView("lignes")
    resultat = spark.sql("""
        SELECT mot, COUNT(*) AS nb
        FROM (SELECT explode(split(value, '\\\\s+')) AS mot FROM lignes)
        WHERE mot <> ''
        GROUP BY mot
    """)
    return {r["mot"]: r["nb"] for r in resultat.collect()}


METHODES = {
    "rdd_reduce": wordcount_rdd_reduce,
    "rdd_group": wordcount_rdd_group,
    "dataframe": wordcount_dataframe,
    "sql": wordcount_sql,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("fichier")
    parser.add_argument("--methode", choices=METHODES, default="rdd_reduce")
    parser.add_argument("--master", default="local[*]")
    args = parser.parse_args()

    t0 = time.perf_counter()
    spark = creer_session(args.master)
    t1 = time.perf_counter()
    resultats = METHODES[args.methode](spark, os.path.abspath(args.fichier))
    t2 = time.perf_counter()
    for mot, nb in sorted(resultats.items(), key=lambda x: x[1], reverse=True):
        print(f"{mot} : {nb}")
    print(f"# session {t1 - t0:.2f} s, calcul {t2 - t1:.2f} s", file=sys.stderr)
    spark.stop()
