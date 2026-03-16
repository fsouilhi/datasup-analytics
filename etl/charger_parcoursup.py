"""ETL Parcoursup (2021-2023) — DataSup Analytics"""
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.connexion import obtenir_moteur

journal = logging.getLogger(__name__)
DOSSIER_BRUT = Path(__file__).parent.parent / "donnees" / "brutes"

INDICES = {
    "annee": 0, "statut": 1, "uai": 2, "nom_etab": 3,
    "dept_code": 4, "dept_lib": 5, "region": 6, "academie": 7,
    "ville": 8, "filiere_agregee": 11, "selectivite": 10,
    "filiere_detaillee": 15, "niveau": 13, "capacite": 17,
    "nb_voeux": 18, "nb_admis": 46, "taux_acces": 112,
    "pct_femmes": 76, "pct_boursiers": 80, "pct_mention_tb": 86,
}


def _charger_csv(chemin):
    for enc in ("utf-8", "latin-1", "utf-8-sig"):
        try:
            df = pd.read_csv(chemin, sep=";", encoding=enc, low_memory=False, header=0)
            journal.info("CSV charge : %s (%d lignes)", chemin.name, len(df))
            return df
        except UnicodeDecodeError:
            continue
    return None


def _renommer(df):
    cols = list(df.columns)
    rename_map = {cols[idx]: nom for nom, idx in INDICES.items() if idx < len(cols)}
    df = df.rename(columns=rename_map)
    for nom in INDICES:
        if nom not in df.columns:
            df[nom] = np.nan
    return df[list(INDICES.keys())]


def _nettoyer(df, annee):
    df = _renommer(df)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").fillna(annee).astype(int)
    df["uai"] = df["uai"].astype(str).str.strip()
    df = df[df["uai"].str.match(r"^[0-9]{7}[A-Z]$", na=False)]
    for col in ("capacite", "nb_voeux", "nb_admis", "taux_acces",
                "pct_femmes", "pct_boursiers", "pct_mention_tb"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("nom_etab", "region", "academie", "ville",
                "filiere_agregee", "filiere_detaillee", "dept_lib"):
        df[col] = df[col].astype(str).str.strip().replace({"nan": None, "": None})
    return df.reset_index(drop=True)


def _type_etab(nom, fili):
    n, f = (nom or "").lower(), (fili or "").lower()
    if "iut" in n or "i.u.t" in n or "but" in f:
        return "IUT"
    if "bts" in f:
        return "BTS"
    if "cpge" in f or "prepa" in n:
        return "CPGE"
    if "universite" in n or "univ" in n or "université" in n:
        return "Université"
    return "Autre"


def _niveau(f):
    f = (f or "").lower()
    if "master" in f:
        return "Master"
    if "licence pro" in f:
        return "Licence Pro"
    if "licence" in f:
        return "Licence"
    if "but" in f:
        return "BUT"
    if "bts" in f:
        return "BTS"
    if "cpge" in f:
        return "CPGE"
    return "Autre"


def _domaine(f, md):
    f = (f or "").lower()
    if "info" in f or "miage" in f:
        return md.get("INFO")
    if "droit" in f:
        return md.get("DROIT")
    if "gestion" in f or "management" in f:
        return md.get("ECO-GEST")
    if "sante" in f or "medecine" in f or "santé" in f:
        return md.get("SANTE")
    if "lettr" in f or "langue" in f:
        return md.get("LETTR")
    if "psycho" in f or "socio" in f:
        return md.get("SHS")
    if "math" in f:
        return md.get("MATH")
    if "physique" in f or "chimie" in f:
        return md.get("PHYS-CHIM")
    if "biolog" in f:
        return md.get("BIO")
    if "ingenieur" in f or "industriel" in f or "ingénieur" in f:
        return md.get("SCI-ING")
    return md.get("AUTRE")


def charger_parcoursup(annee):
    chemin = DOSSIER_BRUT / f"parcoursup_{annee}.csv"
    if not chemin.exists():
        journal.error("Fichier absent : %s", chemin)
        return
    journal.info("=== Chargement Parcoursup %d ===", annee)
    df = _charger_csv(chemin)
    if df is None:
        return
    df = _nettoyer(df, annee)
    journal.info("Lignes apres nettoyage : %d", len(df))
    moteur = obtenir_moteur()
    with moteur.begin() as conn:
        row = conn.execute(
            text("SELECT id_campagne FROM campagne WHERE annee=:a"), {
                "a": annee}).fetchone()
        if not row:
            journal.error("Campagne %d absente", annee)
            return
        id_campagne = row.id_campagne
        md = {r.code: r.id_domaine for r in conn.execute(
            text("SELECT code,id_domaine FROM domaine"))}
        for _, e in df[["uai", "nom_etab", "ville", "dept_lib", "region",
                        "academie", "filiere_agregee"]].drop_duplicates("uai").iterrows():
            uai = str(e["uai"])
            if len(uai) > 8:
                continue
            conn.execute(text("""
                INSERT INTO etablissement (uai,nom,ville,departement,region,academie,type_etab)
                VALUES (:uai,:nom,:ville,:dept,:region,:academie,:te)
                ON CONFLICT (uai) DO NOTHING
            """), {"uai": uai, "nom": str(e["nom_etab"] or "")[:255],
                   "ville": str(e["ville"] or "")[:100] or None,
                   "dept": str(e["dept_lib"] or "")[:100] or None,
                   "region": str(e["region"] or "")[:100] or None,
                   "academie": str(e["academie"] or "")[:100] or None,
                   "te": _type_etab(e["nom_etab"], e["filiere_agregee"])})
        uais = df["uai"].unique().tolist()
        mu = {r.uai: r.id_etab for r in conn.execute(
            text("SELECT uai,id_etab FROM etablissement WHERE uai=ANY(:u)"), {"u": uais})}
        journal.info("Etablissements traites : %d", len(mu))
        for _, f in df[["uai", "filiere_detaillee", "filiere_agregee"]
                       ].drop_duplicates(["uai", "filiere_detaillee"]).iterrows():
            ie = mu.get(f["uai"])
            if not ie:
                continue
            lib = str(f["filiere_detaillee"] or "Non renseigne")[:255]
            conn.execute(text("""
                INSERT INTO formation (id_etab,id_domaine,libelle,niveau)
                VALUES (:ie,:id,:lib,:niv)
                ON CONFLICT (id_etab,libelle) DO NOTHING
            """), {"ie": ie, "id": _domaine(f["filiere_agregee"], md),
                   "lib": lib, "niv": _niveau(f["filiere_agregee"])})
        mf = {(r.id_etab, r.libelle): r.id_form for r in conn.execute(
            text("SELECT id_etab,libelle,id_form FROM formation"))}
        journal.info("Formations chargees : %d", len(mf))
        nb_ok = nb_ko = 0

        def n(x):
            v = pd.to_numeric(x, errors="coerce")
            return None if pd.isna(v) else float(v)
        for _, ligne in df.iterrows():
            ie = mu.get(ligne["uai"])
            if not ie:
                nb_ko += 1
                continue
            lib = str(ligne["filiere_detaillee"] or "Non renseigne")[:255]
            id_form = mf.get((ie, lib))
            if not id_form:
                nb_ko += 1
                continue
            try:
                conn.execute(text("SAVEPOINT sp_ps"))
                conn.execute(text("""
                    INSERT INTO parcoursup
                        (id_form,id_campagne,capacite,nb_voeux,nb_admis,
                         taux_acces,pct_mention_tb,pct_boursiers,pct_femmes)
                    VALUES (:f,:c,:cap,:v,:a,:ta,:tb,:bo,:fe)
                    ON CONFLICT (id_form,id_campagne) DO UPDATE SET
                        capacite=EXCLUDED.capacite,nb_voeux=EXCLUDED.nb_voeux,
                        nb_admis=EXCLUDED.nb_admis,taux_acces=EXCLUDED.taux_acces,
                        pct_mention_tb=EXCLUDED.pct_mention_tb,
                        pct_boursiers=EXCLUDED.pct_boursiers,
                        pct_femmes=EXCLUDED.pct_femmes
                """), {"f": id_form, "c": id_campagne,
                       "cap": n(ligne["capacite"]), "v": n(ligne["nb_voeux"]),
                       "a": n(ligne["nb_admis"]), "ta": n(ligne["taux_acces"]),
                       "tb": n(ligne["pct_mention_tb"]), "bo": n(ligne["pct_boursiers"]),
                       "fe": n(ligne["pct_femmes"])})
                conn.execute(text("RELEASE SAVEPOINT sp_ps"))
                nb_ok += 1
            except Exception as ex:
                conn.execute(text("ROLLBACK TO SAVEPOINT sp_ps"))
                journal.debug("Ligne ignoree : %s", ex)
                nb_ko += 1
        journal.info("Parcoursup %d — inseres : %d, ignores : %d", annee, nb_ok, nb_ko)
