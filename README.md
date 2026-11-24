# SahlPay : plateforme Big Data pour un portefeuille mobile

Projet **Écosystème Big Data 2** (EMSI, 5ᵉ année, 2026-2027).
Suivi des paiements en temps réel et détection de structures inhabituelles avec Apache Spark, Kafka, Delta Lake, GraphX et GraphFrames.

- **Réalisé par** : Hiba KAAOUACH et Oumaima BOUMAZOUED
- **Encadré par** : Dr. Mohammed SALIHOUN

```
producer.py → Kafka (topic transactions, 3 partitions) → Spark (Master + 2 Workers) → MinIO s3a://lake
                                                                                     (bronze → silver → gold)
```

## État d'avancement

| Partie | Contenu | Livrable | Statut |
|---|---|---|---|
| 1 | Cluster Docker et positionnement de Spark | `work/work/partie1.ipynb` | ✅ Jalon 1 |
| 2 | SparkCore (RDD) | `work/work/partie2.ipynb` | ✅ Jalon 1 |
| 3 | Spark SQL et DataFrames | `work/work/partie3.ipynb`, `work/work/partie3_benchmark.ipynb` | ✅ Jalon 1 |
| 4 | Structured Streaming depuis Kafka | `work/work/partie4.py` | Jalon 2 |
| 5 | Pipeline de production Spark-Kafka, Delta Lake | `work/work/partie5.py` | Jalon 2 |
| 6 | GraphFrames (PySpark) et GraphX (Scala) | `work/work/partie6_graphframes.ipynb`, `work/work/graphx_analysis.scala` | Jalon 2 |

## Versions

| Composant | Version | Image / package |
|---|---|---|
| Apache Spark | 3.5.1 (Scala 2.12.18, Java 11) | `apache/spark:3.5.1-python3` |
| Python / PySpark | 3.8.10 / 3.5.1 | image Jupyter construite depuis `Dockerfile.spark` |
| JupyterLab | 4.3.8 | `pip` (Dockerfile.spark) |
| Apache Kafka | 3.7.0, mode KRaft (sans ZooKeeper) | `apache/kafka:3.7.0` |
| MinIO | RELEASE.2026-11-23 | `chainguard/minio:latest` |
| Client MinIO (`mc`) | 2025.8.13 | `heyvaldemar/minio-client:2025.8.13` |
| hadoop-aws / AWS SDK | 3.3.4 / 1.12.262 | `spark.jars.packages` |
| Connecteur Kafka | spark-sql-kafka-0-10_2.12:3.5.1 | `spark.jars.packages` |
| Delta Lake | delta-spark_2.12:3.2.0 | `spark.jars.packages` |
| GraphFrames (partie 6) | 0.8.3-spark3.5-s_2.12 | `spark.jars.packages` + dépôt spark-packages |

Les packages JVM sont téléchargés automatiquement au premier démarrage d'une SparkSession, puis mis en cache dans `/tmp/ivy`. Toute la configuration Spark (MinIO S3A, Delta, packages) est centralisée dans [`work/work/common.py`](work/work/common.py).

## Arborescence

```
.
├── docker-compose.yml        # Kafka, MinIO (+ création du bucket), Spark Master + 2 Workers, Jupyter
├── Dockerfile.spark          # image Jupyter (PySpark 3.5.1, JupyterLab, pandas, kafka-python…)
├── data/
│   ├── producer.py           # générateur de transactions (modes merchants / file / kafka)
│   └── data/                 # merchants.json, fraud_ground_truth.json (+ transactions_history.jsonl généré)
├── work/work/                # notebooks et scripts (monté dans Jupyter sous /home/work/work)
│   ├── common.py             # get_spark() : SparkSession configurée (cluster, MinIO, Delta, packages)
│   ├── schemas.py            # schémas StructType des transactions et des commerçants
│   ├── partie1.ipynb
│   ├── partie2.ipynb
│   ├── partie3.ipynb
│   └── partie3_benchmark.ipynb
├── captures/                 # preuves (Spark UI, explain, listings MinIO / Kafka)
└── rapport/                  # rapport (source HTML + PDF)
```

## Lancement

Prérequis : Docker Desktop (au moins 8 Go de RAM alloués) et Python 3 sur l'hôte (facultatif).

### 1. Démarrer l'environnement
```bash
docker compose up -d --build
```
Le service `minio-init` crée automatiquement le bucket `lake`.

| Interface | URL |
|---|---|
| Spark Master | http://localhost:8080 (2 Workers × 2 cœurs / 2 Go) |
| Spark UI de l'application en cours | http://localhost:4040 |
| JupyterLab | http://localhost:8888 |
| Console MinIO | http://localhost:9001 (identifiants de développement : voir `docker-compose.yml`) |

### 2. Créer le topic Kafka (3 partitions)
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic transactions
```

### 3. Générer les données et les copier dans MinIO
```bash
# depuis le conteneur Jupyter (Python et dépendances déjà présents)
docker exec jupyter python3 /home/data/producer.py --mode file --n 50000 --out /home/data/data

# copie dans le lac (les Workers lisent s3a://lake, pas le disque local)
docker run --rm --network sahlpay_default -v "$(pwd)/data/data:/src" \
  --entrypoint /bin/bash heyvaldemar/minio-client:2025.8.13 -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && \
   mc cp /src/transactions_history.jsonl local/lake/bronze/history/ && \
   mc cp /src/merchants.json local/lake/ref/merchants/ && \
   mc ls --recursive local/lake/"
```
> Le réseau Docker porte le nom du dossier du projet (`<dossier>_default`) : vérifiez-le avec `docker network ls`.
> Sous PowerShell, remplacez `$(pwd)` par `${PWD}` et écrivez la commande sur une seule ligne.

### 4. Exécuter les notebooks
Dans JupyterLab, ouvrez `work/` et exécutez dans l'ordre `partie1.ipynb`, `partie2.ipynb`, `partie3.ipynb` puis `partie3_benchmark.ipynb`.

Chaque notebook crée sa session avec :
```python
from common import get_spark, LAKE
spark = get_spark("SahlPay-P2-RDD")
```

> ⚠️ **Une seule application Spark à la fois.** Chaque session réserve les 4 cœurs du cluster : une seconde application reste `WAITING` (*Initial job has not accepted any resources*). Terminez chaque notebook par `spark.stop()` ou arrêtez son noyau.
> La Spark UI n'est publiée que sur le port 4040 : si une autre application l'occupe, la suivante passe sur 4041, qui n'est pas exposé.

Exécution non interactive (les sorties sont enregistrées dans le notebook) :
```bash
docker exec -w /home/work/work jupyter jupyter nbconvert --to notebook --execute --inplace partie3.ipynb
```

## Organisation du lac (MinIO, bucket `lake`)

```
lake/
  ref/merchants/                     référentiel des commerçants (JSON Lines, 300 lignes)
  bronze/history/                    historique brut (transactions_history.jsonl, 50 000 événements)
  bronze/transactions_raw/           flux Kafka brut en JSON (partie 4)
  silver/transactions_history/       Parquet dédoublonné, partitionné par city (partie 3)
  silver/transactions_clean/         flux nettoyé et enrichi, partitionné par city et date (partie 5)
  gold/kpi_city_10min/               KPI Delta Lake (partie 5)
  gold/graph_analysis/               anomalies du graphe (partie 6)
  checkpoints/                       checkpoints Structured Streaming
```

## Résultats du jalon 1 (résumé)

- **Partie 1** : SparkSession connectée à `spark://spark-master:7077`, Spark 3.5.1, `defaultParallelism = 4`.
- **Partie 2** : montant total et moyen par ville (Casablanca : 1 771 958,41 MAD, 4 044 paiements) et top 5 des commerçants (M-001 : 637 paiements). Le job compte 3 stages, séparés par 2 shuffles.
- **Partie 3** : résultats identiques à ceux de la partie 2. Pour la jointure avec le référentiel, Spark choisit un `BroadcastHashJoin`. L'UDF Python est environ 2,2 fois plus lente que `when/otherwise`. La Silver Parquet compte 49 066 lignes (934 doublons retirés). Sur 1 million d'événements, le DataFrame et le SQL sont environ 2 à 2,5 fois plus rapides que le RDD.

Les preuves sont dans [`captures/`](captures/) et le détail dans le rapport ([`rapport/`](rapport/)).
