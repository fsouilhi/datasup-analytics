import streamlit as st
from analytique.requetes_parcoursup import stats_globales
from analytique.requetes_insertion import stats_insertion_globales

st.set_page_config(page_title="DataSup Analytics", page_icon="🎓", layout="wide")

st.markdown("""<style>
.stApp{background-color:#0f172a}
[data-testid="stSidebar"]{background-color:#1e293b}
[data-testid="stMetric"]{background-color:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px}
[data-testid="stMetricValue"]{color:#60a5fa!important;font-size:1.8rem!important;font-weight:700!important}
[data-testid="stMetricLabel"]{color:#94a3b8!important}
h1,h2,h3{color:#f1f5f9!important}
p,li{color:#cbd5e1}
</style>""", unsafe_allow_html=True)

st.title("DataSup Analytics")
st.markdown("**Observatoire analytique de l'enseignement superieur francais** — donnees Parcoursup 2021-2023 et insertion professionnelle (MESRI)")
st.divider()

stats_ps = stats_globales(2023)
stats_ins = stats_insertion_globales()

col1,col2,col3,col4,col5 = st.columns(5)
col1.metric("Formations", f"{stats_ps['nb_formations']:,}")
col2.metric("Etablissements", f"{stats_ps['nb_etablissements']:,}")
col3.metric("Voeux 2023", f"{stats_ps['total_voeux']:,}")
col4.metric("Admis 2023", f"{stats_ps['total_admis']:,}")
col5.metric("Taux d'acces moyen", f"{stats_ps['taux_acces_moyen']} %")

st.divider()
col_a,col_b = st.columns(2)
with col_a:
    st.markdown("""### Navigation
Utilisez le menu lateral pour acceder aux modules :
- **Parcoursup** - selectivite, evolution, profil des admis
- **Insertion** - salaires, taux d'emploi, score composite""")
with col_b:
    st.markdown("""### Sources
| Dataset | Source | Perimetre |
|---|---|---|
| Parcoursup | data.gouv.fr | 2021-2023 |
| Insertion Master | MESR | 2010-2020 |
| Insertion Licence Pro | MESR | 2010-2020 |""")

st.divider()
st.caption("DataSup Analytics · Fatima Souilhi · L3 Informatique, Nantes Universite · github.com/fsouilhi")
