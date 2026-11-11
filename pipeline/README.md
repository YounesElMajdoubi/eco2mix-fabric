# Pipeline `pl_eco2mix_quotidien`

Exécution planifiée chaque jour à 07:00 (heure de Paris).

| Ordre | Activité | Type | Élément | Réglages |
| --- | --- | --- | --- | --- |
| 1 | Bronze | Notebook | `01_bronze_ingestion` | 2 nouvelles tentatives, intervalle 300 s, étiquette de session `eco2mix` |
| 2 | Silver | Notebook | `02_silver_qualite` | 2 nouvelles tentatives, intervalle 300 s, étiquette de session `eco2mix` |
| 3 | Sync SQL | Attendre | — | 60 s, le temps que le point de terminaison SQL du lakehouse voie le nouveau silver |
| 4 | Gold | Procédure stockée | `wh_eco2mix` → `gold.sp_rafraichir` | — |

Chaque activité ne démarre qu'en cas de réussite de la précédente.

**Haute simultanéité** (paramètres de l'espace de travail → Ingénierie des données → Paramètres Spark → Concurrence élevée → « pour les pipelines exécutant plusieurs notebooks ») : avec la même étiquette de session, Bronze et Silver partagent une seule session Spark. Sans ce réglage, la capacité d'essai refusait la deuxième session (erreur `TooManyRequestsForCapacity`, HTTP 430).

Le Starter Pool est limité à 3 nœuds : largement suffisant pour quelques dizaines de milliers de lignes.

La définition JSON du pipeline (`pl_eco2mix_quotidien.json`) s'exporte depuis Fabric : ouvrir le pipeline → afficher le code JSON → copier.
