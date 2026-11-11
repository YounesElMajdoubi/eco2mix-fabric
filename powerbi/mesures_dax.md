# Modèle sémantique `sm_eco2mix`

Mode **Direct Lake** sur les cinq tables du schéma `gold` de l'entrepôt `wh_eco2mix`.

## Relations

| De (côté 1) | Vers (côté *) | Filtre |
| --- | --- | --- |
| `dim_date[date]` | `fact_production[date_locale]` | Unique |
| `dim_date[date]` | `fact_consommation[date_locale]` | Unique |
| `dim_date[date]` | `conso_jour[date_locale]` | Unique |

`dim_date` est marquée comme table de dates. `qualite` est indépendante : elle décrit les exécutions, pas l'électricité.

## Mesures

| Table | Mesure | Format |
| --- | --- | --- |
| `fact_production` | `Énergie produite (TWh) = SUM ( fact_production[mwh] ) / 1000000` | 1 décimale |
| `fact_production` | `Part nucléaire = DIVIDE ( CALCULATE ( [Énergie produite (TWh)], fact_production[filiere] = "Nucléaire" ), [Énergie produite (TWh)] )` | % |
| `fact_production` | `Part bas carbone = DIVIDE ( CALCULATE ( [Énergie produite (TWh)], fact_production[filiere] IN { "Nucléaire", "Hydraulique", "Éolien", "Solaire" } ), [Énergie produite (TWh)] )` | % |
| `fact_production` | `Part gaz = DIVIDE ( CALCULATE ( [Énergie produite (TWh)], fact_production[filiere] = "Gaz" ), [Énergie produite (TWh)] )` | % |
| `conso_jour` | `Consommation (TWh) = SUM ( conso_jour[energie_mwh] ) / 1000000` | 1 décimale |
| `conso_jour` | `Pic de consommation (MW) = MAX ( conso_jour[pic_mw] )` | 0 décimale |
| `fact_consommation` | `Consommation moyenne (MW) = AVERAGE ( fact_consommation[consommation] )` | 0 décimale |
| `fact_consommation` | `Émissions (kt CO2) = SUM ( fact_consommation[emissions_t] ) / 1000` | 0 décimale |
| `fact_consommation` | `Intensité CO2 (g/kWh) = DIVIDE ( SUM ( fact_consommation[emissions_t] ) * 1000, SUM ( fact_consommation[production_mwh] ) )` | 1 décimale |
| `fact_consommation` | `Erreur prévision J-1 = DIVIDE ( SUMX ( fact_consommation, ABS ( fact_consommation[consommation] - fact_consommation[prevision_j1] ) ), SUM ( fact_consommation[consommation] ) )` | % |
| `qualite` | `Contrôles exécutés = COUNTROWS ( qualite )` | entier |
| `qualite` | `Contrôles en échec = CALCULATE ( COUNTROWS ( qualite ), qualite[statut] = "KO" )` | entier |
| `qualite` | `Taux de réussite = 1 - DIVIDE ( [Contrôles en échec], [Contrôles exécutés] )` | % |
| `qualite` | `Dernière exécution = MAX ( qualite[run_ts] )` | date et heure (UTC) |

**L'intensité est un ratio de sommes, pas une moyenne des taux.** Une moyenne simple des intensités journalières donne 19,0 g/kWh pour 2024, contre 20,1 g/kWh pour le ratio pondéré, plus proche du bilan RTE.

## Requêtes de test (vue de requête DAX)

Totaux annuels, à comparer au bilan électrique RTE :

```
EVALUATE
SUMMARIZECOLUMNS (
    dim_date[annee],
    "Conso (TWh)", [Consommation (TWh)],
    "Part nucléaire", [Part nucléaire],
    "Intensité (g/kWh)", [Intensité CO2 (g/kWh)]
)
```

Intégrité référentielle (doit renvoyer trois valeurs vides) :

```
EVALUATE
{
    ( "fact_production",   COUNTROWS ( FILTER ( fact_production,   ISBLANK ( RELATED ( dim_date[date] ) ) ) ) ),
    ( "fact_consommation", COUNTROWS ( FILTER ( fact_consommation, ISBLANK ( RELATED ( dim_date[date] ) ) ) ) ),
    ( "conso_jour",        COUNTROWS ( FILTER ( conso_jour,        ISBLANK ( RELATED ( dim_date[date] ) ) ) ) )
}
```
