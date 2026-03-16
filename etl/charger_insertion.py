"""ETL Insertion professionnelle — DataSup Analytics"""
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.connexion import obtenir_moteur

journal = logging.getLogger(__name__)
DOSSIER_BRUT = Path(__file__).parent.parent / "donnees" / "brutes"

# Indices valides pour insertion_master.csv ET insertion_licence.csv
# (meme structure de colonnes)
INDICES = {
    "annee": 0, "uai": 2, "nom_etab": 3, "academie": 6,
    "code_domaine": 7, "domaine": 8, "discipline": 10,
    "situation": 11, "nb_repondants": 13,
    "taux_insertion": 16, "pct_cadre": 19,
    "pct_stables": 20, "pct_temps_plein": 21,
    "salaire_median": 22,
}

def _num(x):
    if pd.isna(x): return None
    s = str(x).strip().lower()
    if s in ("ns","nd","","nan","n/a","-"): return None
    try: return float(s.replace(",",".").replace(" ",""))
    except: return None

def _charger_csv(chemin):
    for enc in ("utf-8","latin-1","utf-8-sig"):
        try:
            df = pd.read_csv(chemin, sep=";", encoding=enc, low_memory=False, header=0)
            if len(df.columns) <= 2: return None
            journal.info("Charge : %s (%d lignes)", chemin.name, len(df))
            return df
        except UnicodeDecodeError: continue
    return None

def _renommer(df):
    cols = list(df.columns)
    rename_map = {cols[idx]: nom for nom, idx in INDICES.items() if idx < len(cols)}
    df = df.rename(columns=rename_map)
    for nom in INDICES:
        if nom not in df.columns:
            df[nom] = np.nan
    return df[list(INDICES.keys())]

def _domaine_id(disc, domaine, md):
    d = (str(disc) + " " + str(domaine)).lower()
    if "info" in d or "miage" in d: return md.get("INFO")
    if "droit" in d: return md.get("DROIT")
    if "deg" in d or "gestion" in d or "eco" in d: return md.get("ECO-GEST")
    if "sante" in d or "santé" in d: return md.get("SANTE")
    if "lettr" in d or "langue" in d: return md.get("LETTR")
    if "shs" in d or "psycho" in d or "socio" in d: return md.get("SHS")
    if "math" in d: return md.get("MATH")
    if "phy" in d or "chim" in d: return md.get("PHYS-CHIM")
    if "bio" in d: return md.get("BIO")
    if "meef" in d or "enseignement" in d: return md.get("AUTRE")
    return md.get("AUTRE")

def _get_ou_creer_form(conn, nom_etab, discipline, niveau, md):
    nom = str(nom_etab or "Inconnu")[:255]
    lib = str(discipline or "Non renseigne")[:255]

    # Chercher etablissement
    res = conn.execute(text(
        "SELECT id_etab FROM etablissement WHERE nom ILIKE :n LIMIT 1"
    ), {"n": f"%{nom[:25]}%"}).fetchone()

    if res:
        id_etab = res.id_etab
    else:
        uai = f"I{abs(hash(nom)) % 9999999:07d}"[:8]
        conn.execute(text("""
            INSERT INTO etablissement (uai,nom,type_etab)
            VALUES (:u,:n,'Université') ON CONFLICT (uai) DO NOTHING
        """), {"u": uai, "n": nom})
        res = conn.execute(text(
            "SELECT id_etab FROM etablissement WHERE uai=:u LIMIT 1"
        ), {"u": uai}).fetchone()
        if not res: return None
        id_etab = res.id_etab

    # Chercher ou créer formation
    id_dom = _domaine_id(discipline, "", md)
    conn.execute(text("""
        INSERT INTO formation (id_etab,id_domaine,libelle,niveau)
        VALUES (:ie,:id,:lib,:niv) ON CONFLICT (id_etab,libelle) DO NOTHING
    """), {"ie": id_etab, "id": id_dom, "lib": lib, "niv": niveau})

    res = conn.execute(text(
        "SELECT id_form FROM formation WHERE id_etab=:e AND libelle=:l LIMIT 1"
    ), {"e": id_etab, "l": lib}).fetchone()
    return res.id_form if res else None

def charger_insertion(type_diplome="master"):
    chemin = DOSSIER_BRUT / ("insertion_master.csv" if type_diplome=="master" else "insertion_licence.csv")
    niveau = "Master" if type_diplome=="master" else "Licence Pro"

    if not chemin.exists():
        journal.error("Absent : %s", chemin); return

    journal.info("=== Chargement insertion %s ===", type_diplome)
    df_brut = _charger_csv(chemin)
    if df_brut is None: return

    df = _renommer(df_brut)
    df = df[df["situation"].astype(str).str.contains("18 mois", na=False)]
    journal.info("Lignes 18 mois : %d", len(df))

    moteur = obtenir_moteur()
    with moteur.begin() as conn:
        md = {r.code: r.id_domaine for r in conn.execute(text("SELECT code,id_domaine FROM domaine"))}
        nb_ok = nb_ko = 0

        for _, ligne in df.iterrows():
            annee = int(_num(ligne["annee"]) or 0)
            if annee < 2010: nb_ko+=1; continue

            res = conn.execute(text(
                "SELECT id_campagne FROM campagne ORDER BY ABS(annee-:a) LIMIT 1"
            ), {"a": annee}).fetchone()
            if not res: nb_ko+=1; continue
            id_campagne = res.id_campagne

            id_form = _get_ou_creer_form(
                conn, ligne["nom_etab"], ligne["discipline"], niveau, md
            )
            if not id_form: nb_ko+=1; continue

            try:
                conn.execute(text("SAVEPOINT sp_ins"))
                conn.execute(text("""
                    INSERT INTO insertion_pro
                        (id_form,id_campagne,taux_emploi_18m,pct_emploi_cadre,
                         pct_cdi,pct_temps_plein,salaire_median,nb_repondants)
                    VALUES (:f,:c,:t,:cadre,NULL,:tps,:sal,:nb)
                    ON CONFLICT (id_form,id_campagne) DO UPDATE SET
                        taux_emploi_18m=EXCLUDED.taux_emploi_18m,
                        pct_emploi_cadre=EXCLUDED.pct_emploi_cadre,
                        pct_temps_plein=EXCLUDED.pct_temps_plein,
                        salaire_median=EXCLUDED.salaire_median,
                        nb_repondants=EXCLUDED.nb_repondants
                """), {
                    "f": id_form, "c": id_campagne,
                    "t":    _num(ligne["taux_insertion"]),
                    "cadre":_num(ligne["pct_cadre"]),
                    "tps":  _num(ligne["pct_temps_plein"]),
                    "sal":  _num(ligne["salaire_median"]),
                    "nb":   _num(ligne["nb_repondants"]),
                })
                conn.execute(text("RELEASE SAVEPOINT sp_ins"))
                nb_ok += 1
            except Exception as ex:
                conn.execute(text("ROLLBACK TO SAVEPOINT sp_ins"))
                journal.debug("Ignore : %s", ex); nb_ko += 1

        journal.info("Insertion %s — ok:%d ko:%d", type_diplome, nb_ok, nb_ko)
