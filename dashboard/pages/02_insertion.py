import streamlit as st
import plotly.express as px
from analytique.requetes_insertion import (
    salaires_par_domaine, evolution_insertion_par_domaine,
    top_formations_insertion, stats_insertion_globales,
)

st.set_page_config(page_title="Insertion - DataSup", layout="wide")
PALETTE = px.colors.qualitative.Set2

st.markdown("""<style>
.stApp{background-color:#0f172a}
[data-testid="stSidebar"]{background-color:#1e293b}
h1,h2,h3{color:#f1f5f9!important}
p{color:#cbd5e1}
</style>""", unsafe_allow_html=True)

st.title("Insertion professionnelle")
st.divider()

stats = stats_insertion_globales()
col1,col2,col3,col4 = st.columns(4)
col1.metric("Mesures", f"{stats['nb_mesures']:,}")
col2.metric("Taux emploi moyen", f"{stats['taux_emploi_moyen']} %")
col3.metric("Salaire median moyen", f"{int(stats['salaire_median_moyen']):,} EUR")
col4.metric("Part cadre", f"{stats['pct_cadre_moyen']} %")

st.divider()
st.subheader("Salaire median et emplois cadre par domaine")
df_sal = salaires_par_domaine()
if not df_sal.empty:
    col5,col6 = st.columns(2)
    with col5:
        fig1 = px.bar(df_sal.sort_values("salaire_median_moyen"),
            x="salaire_median_moyen", y="domaine", orientation="h",
            color="secteur", color_discrete_sequence=PALETTE,
            text="salaire_median_moyen",
            labels={"salaire_median_moyen":"Salaire median (EUR/mois)","domaine":"Domaine"},
            title="Salaire net median par domaine")
        fig1.update_traces(texttemplate="%{text:.0f} EUR", textposition="outside")
        fig1.update_layout(paper_bgcolor="#0f172a",plot_bgcolor="#1e293b",font_color="#cbd5e1",height=400)
        st.plotly_chart(fig1, use_container_width=True)
    with col6:
        fig2 = px.bar(df_sal.sort_values("pct_cadre_moyen"),
            x="pct_cadre_moyen", y="domaine", orientation="h",
            color="secteur", color_discrete_sequence=PALETTE,
            text="pct_cadre_moyen",
            labels={"pct_cadre_moyen":"% Emplois cadre/PI","domaine":"Domaine"},
            title="Part des emplois cadre")
        fig2.update_traces(texttemplate="%{text:.1f} %", textposition="outside")
        fig2.update_layout(paper_bgcolor="#0f172a",plot_bgcolor="#1e293b",font_color="#cbd5e1",height=400)
        st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(
        df_sal[["domaine","secteur","nb_formations","salaire_median_moyen","pct_cadre_moyen","pct_temps_plein_moyen"]].rename(
            columns={"domaine":"Domaine","secteur":"Secteur","nb_formations":"Formations",
                     "salaire_median_moyen":"Salaire median (EUR)","pct_cadre_moyen":"% Cadre","pct_temps_plein_moyen":"% Tps plein"}),
        use_container_width=True, hide_index=True,
        column_config={
            "Salaire median (EUR)": st.column_config.NumberColumn(format="%d EUR"),
            "% Cadre":    st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            "% Tps plein": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
        }
    )

st.divider()
st.subheader("Evolution temporelle")
df_evol = evolution_insertion_par_domaine()
if not df_evol.empty:
    domaines = sorted(df_evol["domaine"].unique().tolist())
    sel = st.multiselect("Domaines", domaines, default=domaines[:5])
    if sel:
        df_f = df_evol[df_evol["domaine"].isin(sel)]
        col7,col8 = st.columns(2)
        with col7:
            fig3 = px.line(df_f, x="annee", y="salaire_median_moyen", color="domaine",
                markers=True, color_discrete_sequence=PALETTE,
                labels={"salaire_median_moyen":"Salaire median (EUR)","annee":"Annee"},
                title="Evolution du salaire median")
            fig3.update_layout(paper_bgcolor="#0f172a",plot_bgcolor="#1e293b",font_color="#cbd5e1")
            st.plotly_chart(fig3, use_container_width=True)
        with col8:
            fig4 = px.line(df_f, x="annee", y="taux_emploi_moyen", color="domaine",
                markers=True, color_discrete_sequence=PALETTE,
                labels={"taux_emploi_moyen":"Taux emploi (%)","annee":"Annee"},
                title="Evolution du taux d'emploi")
            fig4.update_layout(paper_bgcolor="#0f172a",plot_bgcolor="#1e293b",font_color="#cbd5e1")
            st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.subheader("Top 15 formations - Score composite d'insertion")
st.caption("Score = 0.4 x taux emploi + 0.3 x (salaire/25) + 0.3 x % cadre")
df_top = top_formations_insertion(top_n=15)
if not df_top.empty:
    fig5 = px.bar(df_top, x="score_composite", y="formation", orientation="h",
        color="domaine", color_discrete_sequence=PALETTE,
        hover_data=["etablissement","taux_emploi_18m","salaire_median","pct_emploi_cadre"],
        labels={"score_composite":"Score","formation":"Formation"},
        title="Meilleures formations selon le score composite")
    fig5.update_layout(paper_bgcolor="#0f172a",plot_bgcolor="#1e293b",font_color="#cbd5e1",height=500)
    st.plotly_chart(fig5, use_container_width=True)
    st.dataframe(
        df_top[["formation","domaine","taux_emploi_18m","salaire_median","pct_emploi_cadre","score_composite"]].rename(
            columns={"formation":"Formation","domaine":"Domaine",
                     "taux_emploi_18m":"Taux emploi (%)","salaire_median":"Salaire (EUR)",
                     "pct_emploi_cadre":"% Cadre","score_composite":"Score"}),
        use_container_width=True, hide_index=True,
        column_config={
            "Taux emploi (%)": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            "Salaire (EUR)":   st.column_config.NumberColumn(format="%d EUR"),
            "% Cadre":         st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            "Score":           st.column_config.NumberColumn(format="%.1f"),
            "Formation":       st.column_config.TextColumn(width="large"),
        }
    )
