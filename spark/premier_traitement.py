# ============================================================
# Projet Big Data ENSIASD — Premier traitement Spark (TP Jalon 1, étape 6)
# Données : jeu Elliptic (transactions Bitcoin étiquetées)
# Objectif : mesurer la répartition licite / illicite / inconnu
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Démarrer Spark ("local[*]" = utiliser tous les processeurs de la machine)
spark = (SparkSession.builder
         .appName("TP1-Elliptic-premier-traitement")
         .master("local[*]")
         .getOrCreate())
print(f"\n===== Version de Spark : {spark.version} =====\n")

# 2. Lire les données
# classes : txId + class ("1" = illicite, "2" = licite, "unknown" = inconnu)
classes = spark.read.csv("/opt/data/elliptic_txs_classes.csv", header=True)

# features : pas d'en-tête ; colonne 0 = txId, colonne 1 = time step (période)
features = (spark.read.csv("/opt/data/elliptic_txs_features.csv", header=False)
            .select(F.col("_c0").cast("long").alias("txId"),
                    F.col("_c1").cast("int").alias("time_step")))

# 3. Traduire les étiquettes en mots lisibles
classes = (classes
           .withColumn("txId", F.col("txId").cast("long"))
           .withColumn("statut",
                       F.when(F.col("class") == "1", "illicite")
                        .when(F.col("class") == "2", "licite")
                        .otherwise("inconnu")))

total = classes.count()
print(f"===== Nombre total de transactions : {total} =====\n")

# 4. RÉSULTAT 1 : répartition globale, avec pourcentages
repartition = (classes.groupBy("statut").count()
               .withColumn("pourcentage", F.round(F.col("count") / total * 100, 2))
               .orderBy(F.desc("count")))
print("===== Répartition globale =====")
repartition.show()

# 5. RÉSULTAT 2 : répartition par période (on relie les deux fichiers par txId)
par_periode = (classes.join(features, on="txId")
               .groupBy("time_step")
               .pivot("statut", ["licite", "illicite", "inconnu"])
               .count()
               .orderBy("time_step"))
print("===== Répartition par période (time step) =====")
par_periode.show(49)

# 6. Sauvegarder le résultat 2 dans un fichier CSV
(par_periode.coalesce(1)
 .write.mode("overwrite")
 .csv("/opt/spark-apps/resultats/repartition_par_periode", header=True))
print("===== Résultat sauvegardé dans spark/resultats/ =====")

spark.stop()