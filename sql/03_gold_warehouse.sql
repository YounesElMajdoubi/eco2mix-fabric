-- Warehouse Fabric : wh_eco2mix (même espace de travail que le Lakehouse lh_eco2mix)
-- La couche gold est construite en T-SQL, en lisant le silver du Lakehouse par requête
-- inter-bases (lh_eco2mix.dbo.<table>). Le projet montre ainsi PySpark ET SQL.
-- Reconstruction complète à chaque exécution : les volumes sont faibles (~35 000 lignes par an).

CREATE SCHEMA gold;   -- une seule fois
GO

CREATE OR ALTER PROCEDURE gold.sp_rafraichir
AS
BEGIN
    -- 1) Production par filière, au format long (idéal pour les graphiques de mix)
    DROP TABLE IF EXISTS gold.fact_production;
    CREATE TABLE gold.fact_production AS
    -- énergie (MWh) = puissance (MW) x durée du pas (15 ou 30 min, colonne pas_min du silver)
    SELECT s.ts_utc, s.ts_local, s.date_locale, v.filiere, v.mw, v.mw * s.pas_min / 60.0 AS mwh
    FROM lh_eco2mix.dbo.silver_eco2mix AS s
    CROSS APPLY (VALUES
        ('Nucléaire',   s.nucleaire),
        ('Hydraulique', s.hydraulique),
        ('Éolien',      s.eolien),
        ('Solaire',     s.solaire),
        ('Gaz',         s.gaz),
        ('Bioénergies', s.bioenergies),
        ('Fioul',       s.fioul),
        ('Charbon',     s.charbon)
    ) AS v(filiere, mw)
    WHERE v.mw IS NOT NULL;

    -- 2) Consommation, prévisions et émissions au quart d'heure
    --    taux_co2 en g/kWh = kg/MWh, donc émissions (t) = taux_co2 * production (MWh) / 1000
    DROP TABLE IF EXISTS gold.fact_consommation;
    CREATE TABLE gold.fact_consommation AS
    SELECT s.ts_utc, s.ts_local, s.date_locale,
           DATEPART(hour, s.ts_local) AS heure_locale,
           s.pas_min,
           s.consommation, s.prevision_j, s.prevision_j1, s.ech_physiques, s.taux_co2,
           p.production_mw,
           p.production_mw * s.pas_min / 60.0 AS production_mwh,
           s.taux_co2 * p.production_mw * s.pas_min / 60.0 / 1000.0 AS emissions_t,
           s.nature
    FROM lh_eco2mix.dbo.silver_eco2mix AS s
    LEFT JOIN (SELECT ts_utc, SUM(mw) AS production_mw
               FROM gold.fact_production GROUP BY ts_utc) AS p
           ON p.ts_utc = s.ts_utc;

    -- 3) Synthèse journalière, avec l'heure du pic
    DROP TABLE IF EXISTS gold.conso_jour;
    CREATE TABLE gold.conso_jour AS
    SELECT j.date_locale, j.energie_mwh, j.pic_mw, pk.ts_local AS heure_pic,
           j.emissions_t, j.intensite_co2_g_kwh
    FROM (
        SELECT date_locale,
               SUM(consommation * pas_min / 60.0) AS energie_mwh,
               MAX(consommation)        AS pic_mw,
               SUM(emissions_t)         AS emissions_t,
               SUM(emissions_t) * 1000.0 / NULLIF(SUM(production_mwh), 0) AS intensite_co2_g_kwh
        FROM gold.fact_consommation GROUP BY date_locale
    ) AS j
    JOIN (
        SELECT date_locale, ts_local,
               ROW_NUMBER() OVER (PARTITION BY date_locale ORDER BY consommation DESC) AS rn
        FROM gold.fact_consommation
    ) AS pk ON pk.date_locale = j.date_locale AND pk.rn = 1;

    -- 4) Dimension date (Direct Lake n'accepte pas les tables calculées DAX)
    DROP TABLE IF EXISTS gold.dim_date;
    CREATE TABLE gold.dim_date AS
    SELECT d.[date],
           YEAR(d.[date])  AS annee,
           MONTH(d.[date]) AS mois,
           DAY(d.[date])   AS jour,
           DATEFROMPARTS(YEAR(d.[date]), MONTH(d.[date]), 1) AS debut_mois,   -- axe mensuel
           d.js AS jour_semaine_iso,                                            -- 1 = lundi
           CASE d.js WHEN 1 THEN '1-lun' WHEN 2 THEN '2-mar' WHEN 3 THEN '3-mer'
                     WHEN 4 THEN '4-jeu' WHEN 5 THEN '5-ven' WHEN 6 THEN '6-sam'
                     ELSE '7-dim' END AS jour_semaine,                          -- préfixe = ordre de tri
           CASE WHEN d.js >= 6 THEN 1 ELSE 0 END AS est_weekend
    FROM (SELECT DISTINCT date_locale AS [date],
                 (DATEPART(weekday, date_locale) + @@DATEFIRST + 5) % 7 + 1 AS js
          FROM gold.fact_consommation) AS d;

    -- 5) Résultats qualité, copiés pour rester en Direct Lake (une vue basculerait en DirectQuery)
    DROP TABLE IF EXISTS gold.qualite;
    CREATE TABLE gold.qualite AS
    SELECT run_id, run_ts, CAST(run_ts AS date) AS date_controle,
           controle, perimetre, valeur, seuil, statut, detail
    FROM lh_eco2mix.dbo.dq_resultats;
END;
GO

-- EXEC gold.sp_rafraichir;


-- Les mesures DAX du modèle sémantique sont dans powerbi/mesures_dax.md
