"""Tests : les 4 implémentations Spark donnent le bon résultat et le même que Python (TP1)."""
import os
import sys
from collections import Counter

import pytest

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RACINE, "src"))

import generer_fichier  # noqa: E402
import spark_wordcount  # noqa: E402
import wordcount_mapreduce  # noqa: E402
import wordcount_simple  # noqa: E402

METHODES = list(spark_wordcount.METHODES)


@pytest.fixture(scope="session")
def spark():
    session = spark_wordcount.creer_session("local[2]", nom="tests")
    yield session
    session.stop()


@pytest.fixture
def petit_fichier(tmp_path):
    chemin = tmp_path / "mots.txt"
    chemin.write_text("chat\nchien\nchat\narbre\nchat\nchien\n", encoding="utf-8")
    return str(chemin)


@pytest.mark.parametrize("methode", METHODES)
def test_petit_exemple(spark, petit_fichier, methode):
    assert spark_wordcount.METHODES[methode](spark, petit_fichier) == {"chat": 3, "chien": 2, "arbre": 1}


@pytest.mark.parametrize("methode", METHODES)
def test_espaces_et_lignes_vides(spark, tmp_path, methode):
    """Plusieurs mots par ligne, espaces multiples, tabulations et lignes vides."""
    chemin = tmp_path / "sale.txt"
    chemin.write_text("  chat   chien \n\n\tchat\n\nchien chat\n", encoding="utf-8")
    assert spark_wordcount.METHODES[methode](spark, str(chemin)) == {"chat": 3, "chien": 2}


@pytest.mark.parametrize("methode", METHODES)
def test_fichier_vide(spark, tmp_path, methode):
    chemin = tmp_path / "vide.txt"
    chemin.write_text("", encoding="utf-8")
    assert spark_wordcount.METHODES[methode](spark, str(chemin)) == {}


@pytest.mark.parametrize("methode", METHODES)
def test_identique_a_python_et_counter(spark, tmp_path, methode):
    base = generer_fichier.generer_base(str(tmp_path / "base.txt"), 5_000, graine=7)
    fichier = generer_fichier.dupliquer(base, str(tmp_path / "gros.txt"), 20)       # 100 000 lignes
    attendu = Counter(open(fichier, encoding="utf-8").read().split())
    resultat = spark_wordcount.METHODES[methode](spark, fichier)
    assert resultat == attendu
    assert resultat == wordcount_simple.compter_occurrences(fichier)
    assert resultat == wordcount_mapreduce.compter_occurrences(fichier)


def test_partitionnement(spark, tmp_path):
    """Le RDD est découpé en au moins une partition par cœur (ici local[2])."""
    fichier = generer_fichier.generer_base(str(tmp_path / "f.txt"), 1_000)
    assert spark.sparkContext.textFile(fichier, minPartitions=2).getNumPartitions() >= 2
