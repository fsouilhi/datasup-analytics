-- ============================================================
-- DataSup Analytics — Schéma PostgreSQL
-- Version : 1.0
-- Auteur  : Fatima Souilhi
-- ============================================================
-- Ce schéma modélise les données de l'enseignement supérieur
-- français à partir des sources open data data.gouv.fr :
--   - Parcoursup (vœux et admissions 2021–2023)
--   - Insertion professionnelle des diplômés (MESRI)
-- ============================================================

-- Suppression dans l'ordre inverse des dépendances
DROP TABLE IF EXISTS insertion_pro CASCADE;
DROP TABLE IF EXISTS parcoursup    CASCADE;
DROP TABLE IF EXISTS formation     CASCADE;
DROP TABLE IF EXISTS campagne      CASCADE;
DROP TABLE IF EXISTS domaine       CASCADE;
DROP TABLE IF EXISTS etablissement CASCADE;

-- ============================================================
-- TABLE : etablissement
-- Référentiel des établissements d'enseignement supérieur.
-- La clé naturelle est le code UAI (identifiant national).
-- ============================================================
CREATE TABLE etablissement (
    id_etab     SERIAL       PRIMARY KEY,
    uai         VARCHAR(8)   UNIQUE NOT NULL,
    nom         VARCHAR(255) NOT NULL,
    ville       VARCHAR(100),
    departement VARCHAR(100),
    region      VARCHAR(100),
    academie    VARCHAR(100),
    type_etab   VARCHAR(50)
        CHECK (type_etab IN (
            'Université', 'IUT', 'BTS', 'CPGE', 'École ingénieur',
            'École commerce', 'Institut', 'Autre'
        ))
);

COMMENT ON TABLE  etablissement           IS 'Référentiel des établissements (source : Parcoursup)';
COMMENT ON COLUMN etablissement.uai       IS 'Code UAI — identifiant national unique de l''établissement';
COMMENT ON COLUMN etablissement.type_etab IS 'Type de structure d''enseignement';

-- ============================================================
-- TABLE : domaine
-- Classification disciplinaire des formations.
-- Correspond à la filière grande catégorie de Parcoursup.
-- ============================================================
CREATE TABLE domaine (
    id_domaine SERIAL       PRIMARY KEY,
    code       VARCHAR(20)  UNIQUE NOT NULL,
    libelle    VARCHAR(150) NOT NULL,
    secteur    VARCHAR(50)
        CHECK (secteur IN ('Sciences', 'Lettres & SHS', 'Droit & Éco', 'Santé', 'Arts', 'Autre'))
);

COMMENT ON TABLE  domaine         IS 'Classification disciplinaire des formations';
COMMENT ON COLUMN domaine.code    IS 'Code court utilisé en interne (ex : INFO, DROIT, BIO)';
COMMENT ON COLUMN domaine.secteur IS 'Regroupement macroscopique pour les analyses transversales';

-- ============================================================
-- TABLE : campagne
-- Référentiel temporel — une ligne par année Parcoursup.
-- ============================================================
CREATE TABLE campagne (
    id_campagne SERIAL     PRIMARY KEY,
    annee       SMALLINT   UNIQUE NOT NULL
        CHECK (annee BETWEEN 2018 AND 2030)
);

COMMENT ON TABLE  campagne       IS 'Référentiel des années de campagne Parcoursup';
COMMENT ON COLUMN campagne.annee IS 'Année de la campagne (ex : 2023)';

-- ============================================================
-- TABLE : formation
-- Entité centrale : une formation = un couple (établissement,
-- libellé). La capacité d'accueil est stockée ici car elle
-- évolue peu d'une année sur l'autre (voir parcoursup pour
-- la valeur annuelle exacte).
-- ============================================================
CREATE TABLE formation (
    id_form    SERIAL        PRIMARY KEY,
    id_etab    INT           NOT NULL REFERENCES etablissement(id_etab) ON DELETE CASCADE,
    id_domaine INT           REFERENCES domaine(id_domaine),
    libelle    VARCHAR(255)  NOT NULL,
    niveau     VARCHAR(20)   NOT NULL
        CHECK (niveau IN ('Licence', 'Licence Pro', 'Master', 'BTS', 'BUT', 'CPGE', 'Autre')),
    UNIQUE (id_etab, libelle)
);

COMMENT ON TABLE  formation          IS 'Offre de formation de l''enseignement supérieur';
COMMENT ON COLUMN formation.libelle  IS 'Libellé officiel de la formation (Parcoursup)';
COMMENT ON COLUMN formation.niveau   IS 'Niveau de diplôme préparé';

-- ============================================================
-- TABLE : parcoursup
-- Indicateurs annuels Parcoursup par formation.
-- Granularité : une ligne = une formation × une campagne.
-- ============================================================
CREATE TABLE parcoursup (
    id_ps            SERIAL         PRIMARY KEY,
    id_form          INT            NOT NULL REFERENCES formation(id_form) ON DELETE CASCADE,
    id_campagne      INT            NOT NULL REFERENCES campagne(id_campagne),

    -- Volumétrie
    capacite         INT            CHECK (capacite > 0),
    nb_voeux         INT            CHECK (nb_voeux >= 0),
    nb_admis         INT            CHECK (nb_admis >= 0),

    -- Sélectivité
    taux_acces       NUMERIC(5, 2)  CHECK (taux_acces BETWEEN 0 AND 100),

    -- Profil des admis
    pct_mention_tb   NUMERIC(5, 2)  CHECK (pct_mention_tb BETWEEN 0 AND 100),
    pct_boursiers    NUMERIC(5, 2)  CHECK (pct_boursiers BETWEEN 0 AND 100),
    pct_femmes       NUMERIC(5, 2)  CHECK (pct_femmes BETWEEN 0 AND 100),

    UNIQUE (id_form, id_campagne)
);

COMMENT ON TABLE  parcoursup              IS 'Données Parcoursup annuelles par formation (source : data.gouv.fr)';
COMMENT ON COLUMN parcoursup.taux_acces   IS 'Part des candidats ayant reçu au moins une proposition (%)';
COMMENT ON COLUMN parcoursup.pct_mention_tb IS 'Part des admis ayant une mention Très Bien au bac (%)';

-- ============================================================
-- TABLE : insertion_pro
-- Indicateurs d'insertion professionnelle 18 mois après
-- l'obtention du diplôme (enquêtes MESRI).
-- Granularité : une ligne = une formation × une campagne.
-- ============================================================
CREATE TABLE insertion_pro (
    id_ins              SERIAL        PRIMARY KEY,
    id_form             INT           NOT NULL REFERENCES formation(id_form) ON DELETE CASCADE,
    id_campagne         INT           NOT NULL REFERENCES campagne(id_campagne),

    -- Insertion
    taux_emploi_18m     NUMERIC(5, 2) CHECK (taux_emploi_18m BETWEEN 0 AND 100),
    pct_emploi_cadre    NUMERIC(5, 2) CHECK (pct_emploi_cadre BETWEEN 0 AND 100),
    pct_cdi             NUMERIC(5, 2) CHECK (pct_cdi BETWEEN 0 AND 100),
    pct_temps_plein     NUMERIC(5, 2) CHECK (pct_temps_plein BETWEEN 0 AND 100),

    -- Rémunération
    salaire_median      INT           CHECK (salaire_median > 0),

    -- Fiabilité statistique
    nb_repondants       INT           CHECK (nb_repondants >= 0),

    UNIQUE (id_form, id_campagne)
);

COMMENT ON TABLE  insertion_pro                 IS 'Indicateurs d''insertion pro 18 mois après diplôme (source : MESRI)';
COMMENT ON COLUMN insertion_pro.taux_emploi_18m IS 'Taux d''emploi 18 mois après obtention du diplôme (%)';
COMMENT ON COLUMN insertion_pro.salaire_median  IS 'Salaire net mensuel médian en euros (emplois à temps plein)';
COMMENT ON COLUMN insertion_pro.nb_repondants   IS 'Effectif ayant répondu à l''enquête — indicateur de fiabilité';

-- ============================================================
-- INDEX
-- Couvrent les colonnes de jointure et de filtre fréquents.
-- ============================================================

-- Jointures FK
CREATE INDEX idx_formation_etab      ON formation(id_etab);
CREATE INDEX idx_formation_domaine   ON formation(id_domaine);
CREATE INDEX idx_parcoursup_form     ON parcoursup(id_form);
CREATE INDEX idx_parcoursup_campagne ON parcoursup(id_campagne);
CREATE INDEX idx_insertion_form      ON insertion_pro(id_form);
CREATE INDEX idx_insertion_campagne  ON insertion_pro(id_campagne);

-- Filtres analytiques fréquents
CREATE INDEX idx_etablissement_region   ON etablissement(region);
CREATE INDEX idx_etablissement_academie ON etablissement(academie);
CREATE INDEX idx_formation_niveau       ON formation(niveau);
CREATE INDEX idx_domaine_secteur        ON domaine(secteur);
CREATE INDEX idx_campagne_annee         ON campagne(annee);

-- ============================================================
-- DONNÉES DE RÉFÉRENCE — Campagnes
-- ============================================================
INSERT INTO campagne (annee) VALUES (2021), (2022), (2023);

-- ============================================================
-- DONNÉES DE RÉFÉRENCE — Domaines (à affiner après exploration)
-- ============================================================
INSERT INTO domaine (code, libelle, secteur) VALUES
    ('INFO',      'Informatique',                                'Sciences'),
    ('MATH',      'Mathématiques',                               'Sciences'),
    ('SCI-ING',   'Sciences de l''ingénieur',                    'Sciences'),
    ('BIO',       'Sciences de la vie et de la terre',           'Sciences'),
    ('PHYS-CHIM', 'Physique-Chimie',                             'Sciences'),
    ('DROIT',     'Droit',                                       'Droit & Éco'),
    ('ECO-GEST',  'Économie & Gestion',                          'Droit & Éco'),
    ('MIAGE',     'Méthodes Informatiques Appliquées à la Gestion','Droit & Éco'),
    ('LETTR',     'Lettres, Langues, Arts',                      'Lettres & SHS'),
    ('SHS',       'Sciences Humaines et Sociales',               'Lettres & SHS'),
    ('SANTE',     'Santé',                                       'Santé'),
    ('AUTRE',     'Autre',                                       'Autre');
