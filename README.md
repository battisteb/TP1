# TP1 — Comptage de mots : quatre méthodes

**Battiste Boungo — ID 04** · Fondements et Application de Big Data (5A) · 13/10/2026

Le même problème, compter les mots d'un fichier, résolu de **quatre façons** et comparé sur les **mêmes fichiers**
(N = 10 000 lignes dupliquées m fois, de 10 000 à 20 millions de lignes) :

| # | Méthode | Dossier | Notebook |
|---|---|---|---|
| 1 | Python séquentiel | [`Partie1_Python_MapReduce_Hadoop/`](Partie1_Python_MapReduce_Hadoop) | [`notebook_comptage_mots.ipynb`](Partie1_Python_MapReduce_Hadoop/notebook_comptage_mots.ipynb) |
| 2 | Python pseudo-MapReduce | idem | idem |
| 3 | Hadoop MapReduce (cluster HDFS + YARN dans Docker ; job Java et job Streaming Python) | idem | idem |
| 4 | PySpark (RDD `reduceByKey`, RDD `groupByKey`, DataFrame, Spark SQL) | [`Partie2_PySpark/`](Partie2_PySpark) | [`notebook_spark_wordcount.ipynb`](Partie2_PySpark/notebook_spark_wordcount.ipynb) |

Les deux notebooks sont exécutés de bout en bout, sans erreur ; chacun commence par une cellule indiquant le titre
du TP, la date et le nom. Chaque dossier a son README (installation, commandes, résultats), son code (`src/`), ses
tests (`tests/`) et ses mesures (`results/`).

## En bref

* **Python séquentiel** est imbattable sur les petits fichiers : aucun coût de démarrage.
* **Pseudo-MapReduce** reproduit le schéma *map → shuffle → reduce* en Python : 3 à 4 fois plus lent et très
  gourmand en mémoire (toutes les paires `(mot, 1)` sont gardées).
* **Hadoop MapReduce** paie environ 70 s de latence par job (démarrage de conteneurs YARN, écritures sur disque) :
  il ne devient intéressant que lorsque les données dépassent une machine.
* **PySpark** garde le même modèle *map → reduce* mais en mémoire et en parallèle : ≈ 1 à 5 s par job une fois
  la session démarrée (contre ≈ 70 s pour Hadoop). Dès le million de lignes, l'API **DataFrame / SQL** est la plus
  rapide (jusqu'à ≈ 2 fois plus que Python pur), alors que l'API RDD avec des lambdas Python est pénalisée par la
  sérialisation JVM ↔ Python.

Les analyses détaillées et les chiffres sont dans les conclusions des deux notebooks.

Autres TP : [TP2 — accidents de la route](https://github.com/battisteb/TP2) ·
[TP3 — small data et big data](https://github.com/battisteb/TP3_ID04).
