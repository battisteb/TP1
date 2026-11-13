"""Benchmark du comptage de mots : Python (TP1) vs Spark (4 implémentations), à exécuter dans le conteneur Docker.

Trois séries de mesures, sur les mêmes fichiers que le TP1 (N = 10 000 lignes dupliquées m fois) :
  1. temps_chaud.csv : chaque méthode dans une session Spark déjà démarrée (local[*]),
                       + Python simple et pseudo-MapReduce (processus complet, comme au TP1) ;
  2. temps_froid.csv : `python src/spark_wordcount.py fichier` dans un nouveau processus
                       (démarrage de la JVM et de la SparkSession compris) ;
  3. scalabilite.csv : même fichier, local[1], local[2], local[4], local[8].
Chaque temps est la médiane de `--repetitions` exécutions, après une exécution de chauffe.

Usage (depuis la racine du projet, sous Windows) :
    docker run --rm -v "%cd%:/work" spark-wordcount:4.2.0 python src/benchmark_spark.py
"""
import argparse
import csv
import os
import statistics
import subprocess
import sys
import time

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(RACINE, "src")
sys.path.insert(0, SRC)

import generer_fichier  # noqa: E402
import spark_wordcount  # noqa: E402
import wordcount_simple  # noqa: E402


def ecrire_csv(chemin, lignes):
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(lignes[0].keys()))
        w.writeheader()
        w.writerows(lignes)


def chrono(fonction, repetitions):
    durees, resultat = [], None
    for _ in range(repetitions):
        t0 = time.perf_counter()
        resultat = fonction()
        durees.append(time.perf_counter() - t0)
    return statistics.median(durees), min(durees), resultat


def processus(commande):
    t0 = time.perf_counter()
    res = subprocess.run(commande, capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        raise RuntimeError(res.stderr[-2000:])
    return time.perf_counter() - t0, res.stdout


def parser_sortie(texte):
    return {mot: int(nb) for mot, nb in (l.rsplit(" : ", 1) for l in texte.splitlines() if " : " in l)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=10_000)
    parser.add_argument("--m", type=int, nargs="+", default=[1, 10, 100, 500, 1000, 2000])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--m-scalabilite", type=int, default=500)
    parser.add_argument("--coeurs", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--data", default="/tmp/data", help="hors du montage Windows : lecture disque rapide")
    parser.add_argument("--out", default=os.path.join(RACINE, "results"))
    parser.add_argument("--series", nargs="+", default=["chaud", "froid", "scalabilite"])
    args = parser.parse_args()

    os.makedirs(args.data, exist_ok=True)
    os.makedirs(args.out, exist_ok=True)
    base = generer_fichier.generer_base(os.path.join(args.data, f"mots_N{args.n}.txt"), args.n)
    fichiers = {m: generer_fichier.dupliquer(base, os.path.join(args.data, f"mots_N{args.n}_m{m}.txt"), m)
                for m in sorted(set(args.m + [args.m_scalabilite]))}
    references = {m: wordcount_simple.compter_occurrences(f) for m, f in fichiers.items()}
    infos = lambda m: dict(m=m, lignes=args.n * m, taille_mo=round(os.path.getsize(fichiers[m]) / 1e6, 3))

    # ---------- 1. temps "chauds" ----------
    if "chaud" in args.series:
        lignes = []
        spark = spark_wordcount.creer_session("local[*]")
        coeurs = spark.sparkContext.defaultParallelism
        for meth in spark_wordcount.METHODES.values():          # chauffe (JIT, workers Python)
            meth(spark, fichiers[min(fichiers)])
        for m in args.m:
            f = fichiers[m]
            print(f"\n=== m={m} ({args.n * m} lignes)", flush=True)
            for nom, script in [("python_simple", "wordcount_simple.py"), ("pseudo_mapreduce", "wordcount_mapreduce.py")]:
                durees = []
                for _ in range(args.repetitions):
                    d, out = processus([sys.executable, os.path.join(SRC, script), f])
                    assert parser_sortie(out) == references[m], nom
                    durees.append(d)
                lignes.append(dict(**infos(m), methode=nom, temps_s=round(statistics.median(durees), 4),
                                   temps_min_s=round(min(durees), 4), coeurs=1))
                print(f"  {nom:<18} {statistics.median(durees):8.3f} s", flush=True)
            for nom, meth in spark_wordcount.METHODES.items():
                med, mini, res = chrono(lambda: meth(spark, f), args.repetitions)
                assert res == references[m], nom
                lignes.append(dict(**infos(m), methode=f"spark_{nom}", temps_s=round(med, 4),
                                   temps_min_s=round(mini, 4), coeurs=coeurs))
                print(f"  spark_{nom:<12} {med:8.3f} s", flush=True)
            ecrire_csv(os.path.join(args.out, "temps_chaud.csv"), lignes)
        spark.stop()

    # ---------- 2. temps "à froid" : nouveau processus à chaque fois ----------
    if "froid" in args.series:
        lignes = []
        for m in args.m:
            for nom in ["rdd_reduce", "dataframe"]:
                d, out = processus([sys.executable, os.path.join(SRC, "spark_wordcount.py"), fichiers[m],
                                    "--methode", nom])
                assert parser_sortie(out) == references[m], nom
                lignes.append(dict(**infos(m), methode=f"spark_{nom}", temps_total_s=round(d, 3)))
                print(f"froid m={m:<5} spark_{nom:<12} {d:8.2f} s", flush=True)
            ecrire_csv(os.path.join(args.out, "temps_froid.csv"), lignes)

    # ---------- 3. scalabilité en nombre de cœurs ----------
    if "scalabilite" in args.series:
        lignes = []
        m = args.m_scalabilite
        for k in args.coeurs:
            spark = spark_wordcount.creer_session(f"local[{k}]", nom=f"scalabilite-{k}")
            for nom in ["rdd_reduce", "dataframe"]:
                meth = spark_wordcount.METHODES[nom]
                meth(spark, fichiers[min(fichiers)])               # chauffe
                med, mini, res = chrono(lambda: meth(spark, fichiers[m]), args.repetitions)
                assert res == references[m], nom
                lignes.append(dict(**infos(m), methode=f"spark_{nom}", coeurs=k,
                                   temps_s=round(med, 4), temps_min_s=round(mini, 4)))
                print(f"local[{k}] spark_{nom:<12} {med:8.3f} s", flush=True)
            spark.stop()
            ecrire_csv(os.path.join(args.out, "scalabilite.csv"), lignes)

    print("\nTerminé.")


if __name__ == "__main__":
    main()
