import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from analytique.requetes_parcoursup import (
    classement_selectivite, evolution_admissions_par_domaine,
    profil_admis_par_niveau,
)

st.set_page_config(page_title="Parcoursup - DataSup", layout="wide")
PALETTE = px.colors.qualitative.Set2



st.title("Analyse Parcoursup 2021-2023")
st.divider()

with st.sidebar:
    st.header("Filtres")
    annee = st.selectbox("Annee", [2023, 2022, 2021], index=0)
    top_n = st.slider("Formations affichees", 10, 50, 20)

st.subheader("Profil des admis par niveau")
df_profil = profil_admis_par_niveau(annee)
if not df_profil.empty:
    col1,col2 = st.columns(2)
    with col1:
        fig = px.bar(df_profil.sort_values("moy_taux_acces"),
            x="moy_taux_acces", y="niveau", orientation="h",
            color="niveau", color_discrete_sequence=PALETTE,
            labels={"moy_taux_acces":"Taux d'acces moyen (%)","niveau":"Niveau"},
            title=f"Taux d'acces moyen par niveau ({annee})")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        labels_cat = ["% Femmes","% Boursiers","% Mention TB"]
        fig2 = go.Figure()
        for _,row in df_profil.iterrows():
            vals = [row["moy_pct_femmes"] or 0, row["moy_pct_boursiers"] or 0, row["moy_pct_mention_tb"] or 0]
            fig2.add_trace(go.Scatterpolar(r=vals+[vals[0]], theta=labels_cat+[labels_cat[0]], fill="toself", name=row["niveau"]))
        fig2.update_layout(polar=dict(bgcolor="#1e293b",radialaxis=dict(visible=True,range=[0,100],color="#94a3b8")),
            title=f"Profil des admis ({annee})",legend=dict(bgcolor="#1e293b"))
        st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(df_profil.rename(columns={"niveau":"Niveau","nb_formations":"Formations",
        "moy_taux_acces":"Taux acces (%)","moy_pct_femmes":"% Femmes",
        "moy_pct_boursiers":"% Boursiers","moy_pct_mention_tb":"% TB","total_admis":"Admis"}),
        use_container_width=True, hide_index=True)

st.divider()
st.subheader("Evolution des admissions par domaine (2021-2023)")
df_evol = evolution_admissions_par_domaine()
if not df_evol.empty:
    col3,col4 = st.columns(2)
    with col3:
        fig3 = px.line(df_evol, x="annee", y="total_admis", color="domaine",
            markers=True, color_discrete_sequence=PALETTE,
            labels={"total_admis":"Total admis","annee":"Annee"},
            title="Total admis par domaine")
        fig3.update_layout(
            xaxis=dict(tickvals=[2021,2022,2023]))
        st.plotly_chart(fig3, use_container_width=True)
    with col4:
        fig4 = px.line(df_evol, x="annee", y="taux_acces_moyen", color="domaine",
            markers=True, color_discrete_sequence=PALETTE,
            labels={"taux_acces_moyen":"Taux d'acces (%)","annee":"Annee"},
            title="Evolution du taux d'acces moyen")
        fig4.update_layout(
            xaxis=dict(tickvals=[2021,2022,2023]),
            yaxis=dict(range=[30,90]))
        st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.subheader(f"Top {top_n} formations les plus selectives ({annee})")
df_sel = classement_selectivite(annee=annee, limite=top_n)
df_sel = df_sel[df_sel['taux_acces'] > 0]
if not df_sel.empty:
    fig5 = px.bar(df_sel, x="taux_acces", y="formation", orientation="h",
        color="taux_acces", color_continuous_scale="Blues",
        hover_data=["etablissement","academie","nb_voeux","nb_admis"],
        labels={"taux_acces":"Taux d'acces (%)","formation":"Formation"},
        title=f"Formations avec le taux d'acces le plus bas ({annee})")
    fig5.update_layout(height=600,coloraxis_showscale=False)
    st.plotly_chart(fig5, use_container_width=True)
    st.dataframe(
        df_sel[["formation","niveau","taux_acces","nb_voeux","nb_admis"]].rename(
            columns={"formation":"Formation","niveau":"Niveau",
                     "taux_acces":"Taux (%)","nb_voeux":"Voeux","nb_admis":"Admis"}),
        use_container_width=True, hide_index=True,
        column_config={
            "Taux (%)": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            "Formation": st.column_config.TextColumn(width="large"),
        }
    )
