"""
DATASUS - Dashboard SIH/SUS
Produção Hospitalar por Município x Subgrupo de Procedimento
Jan/2024 a Jan/2026
"""

import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pathlib import Path

# ── Configuração da Página ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="DATASUS · SIH/SUS Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("datasus.db")

# ── Tema / CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');

  :root {
    --verde:  #00C49A;
    --azul:   #0057B8;
    --laranja:#FF6B35;
    --cinza:  #F0F4F8;
    --escuro: #0D1B2A;
  }

  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

  h1, h2, h3 { font-family: 'Space Mono', monospace; }

  .block-container { padding-top: 1.5rem; }

  .metric-card {
    background: linear-gradient(135deg, #0D1B2A 0%, #1a3a5c 100%);
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    color: white;
    border-left: 4px solid var(--verde);
    margin-bottom: 0.5rem;
  }
  .metric-card .label { font-size: 0.75rem; opacity: 0.7; text-transform: uppercase; letter-spacing: 0.1em; }
  .metric-card .value { font-family: 'Space Mono', monospace; font-size: 1.8rem; font-weight: 700; color: var(--verde); }
  .metric-card .delta { font-size: 0.8rem; opacity: 0.6; }

  .section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.85rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #0057B8;
    border-bottom: 2px solid #0057B8;
    padding-bottom: 0.3rem;
    margin: 1.5rem 0 1rem 0;
  }

  div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

  .stSelectbox label, .stMultiSelect label { font-size: 0.8rem; font-weight: 500; }

  .sidebar-header {
    background: linear-gradient(135deg, #0D1B2A, #0057B8);
    color: white;
    padding: 1rem;
    border-radius: 8px;
    text-align: center;
    margin-bottom: 1rem;
  }
  .sidebar-header h3 { font-family:'Space Mono',monospace; margin:0; font-size:0.9rem; color:var(--verde); }
  .sidebar-header p  { margin:0; font-size:0.75rem; opacity:0.7; }
</style>
""", unsafe_allow_html=True)


# ── DB ─────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(f"Banco `{DB_PATH}` não encontrado. Execute o scraper ou `generate_sample_data.py`.")
        st.stop()
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data(ttl=300)
def query(sql, params=()):
    conn = get_conn()
    return pd.read_sql_query(sql, conn, params=params)


# ── Dados base ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_filters():
    municipios = query("SELECT DISTINCT municipio FROM producao_hospitalar ORDER BY municipio")['municipio'].tolist()
    subgrupos  = query("SELECT DISTINCT subgrupo_procedimento FROM producao_hospitalar ORDER BY subgrupo_procedimento")['subgrupo_procedimento'].tolist()
    periodos   = query("SELECT DISTINCT periodo, ano, mes FROM producao_hospitalar ORDER BY ano, mes")
    return municipios, subgrupos, periodos


municipios, subgrupos, periodos_df = load_filters()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-header">
        <h3>🏥 SIH / SUS</h3>
        <p>Produção Hospitalar · SP<br>Jan/2024 – Jan/2026</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Filtros**")

    anos_disp = sorted(periodos_df['ano'].unique().tolist())
    anos_sel  = st.multiselect("Ano", anos_disp, default=anos_disp)

    mun_sel = st.multiselect("Municípios", municipios,
                              default=municipios[:5] if len(municipios) >= 5 else municipios)

    sg_sel = st.multiselect("Subgrupos", subgrupos, default=subgrupos)

    tipo_sel = st.radio("Conteúdo", ["Qtd_aprovada", "Val_aprovado"],
                        format_func=lambda x: "Quantidade Aprovada" if x == "Qtd_aprovada" else "Valor Aprovado (R$)")

    st.divider()
    top_n = st.slider("Top N municípios nos rankings", 5, 20, 10)


# ── Query filtrada ─────────────────────────────────────────────────────────────
def load_main():
    if not mun_sel or not sg_sel or not anos_sel:
        return pd.DataFrame()

    mun_ph = ",".join(["?"]*len(mun_sel))
    sg_ph  = ",".join(["?"]*len(sg_sel))
    ano_ph = ",".join(["?"]*len(anos_sel))

    sql = f"""
        SELECT municipio, subgrupo_procedimento, periodo, mes, ano, tipo, valor
        FROM producao_hospitalar
        WHERE tipo = ?
          AND municipio IN ({mun_ph})
          AND subgrupo_procedimento IN ({sg_ph})
          AND ano IN ({ano_ph})
    """
    params = [tipo_sel] + mun_sel + sg_sel + anos_sel
    return query(sql, params)

df = load_main()

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<h1 style="font-family:'Space Mono',monospace;font-size:1.6rem;color:#0D1B2A;margin-bottom:0.2rem;">
    📊 DATASUS · Produção Hospitalar
</h1>
<p style="color:#666;margin-top:0;font-size:0.9rem;">
    Dados Detalhados AIH (SP) · Brasil Município · Subgrupo de Procedimento
</p>
""", unsafe_allow_html=True)

if df.empty:
    st.warning("Nenhum dado encontrado com os filtros selecionados.")
    st.stop()

# ── KPIs ───────────────────────────────────────────────────────────────────────
total_val   = df['valor'].sum()
media_val   = df['valor'].mean()
n_mun       = df['municipio'].nunique()
n_registros = len(df)
unidade     = "R$" if tipo_sel == "Val_aprovado" else "un."

c1, c2, c3, c4 = st.columns(4)

def fmt(v, prefix=""):
    if v >= 1e9:  return f"{prefix}{v/1e9:.2f} bi"
    if v >= 1e6:  return f"{prefix}{v/1e6:.2f} mi"
    if v >= 1e3:  return f"{prefix}{v/1e3:.1f} mil"
    return f"{prefix}{v:,.0f}"

with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Total {unidade}</div>
        <div class="value">{fmt(total_val)}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="label">Média por registro</div>
        <div class="value">{fmt(media_val)}</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="metric-card" style="border-left-color:#0057B8">
        <div class="label">Municípios</div>
        <div class="value" style="color:#4da8ff">{n_mun}</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="metric-card" style="border-left-color:#FF6B35">
        <div class="label">Registros</div>
        <div class="value" style="color:#FF6B35">{n_registros:,}</div>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1 · LISTA DOS DADOS
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("📋 1 · Lista dos Dados Armazenados", expanded=False):
    tabela = (df.groupby(['municipio','subgrupo_procedimento','periodo','ano','mes'])
                ['valor'].sum().reset_index()
                .sort_values(['ano','mes','municipio']))

    label_val = "Valor (R$)" if tipo_sel == "Val_aprovado" else "Quantidade"
    tabela = tabela.rename(columns={'valor': label_val})

    if tipo_sel == "Val_aprovado":
        tabela[label_val] = tabela[label_val].apply(lambda x: f"R$ {x:,.2f}")

    st.markdown(f"**{len(tabela):,} registros** · filtro atual")
    st.dataframe(tabela, use_container_width=True, height=400,
                 column_config={
                     "municipio": st.column_config.TextColumn("Município"),
                     "subgrupo_procedimento": st.column_config.TextColumn("Subgrupo"),
                     "periodo": st.column_config.TextColumn("Período"),
                     "ano": st.column_config.NumberColumn("Ano"),
                     "mes": st.column_config.NumberColumn("Mês"),
                 })

    csv_export = tabela.to_csv(index=False, sep=';').encode('utf-8-sig')
    st.download_button("⬇️ Baixar CSV", csv_export, "datasus_filtrado.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2 · ESTATÍSTICAS DESCRITIVAS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">2 · Estatísticas Descritivas</div>', unsafe_allow_html=True)

tab_geral, tab_mun, tab_sg = st.tabs(["Geral", "Por Município", "Por Subgrupo"])

with tab_geral:
    stats = df['valor'].describe()
    col_s = st.columns(4)
    labels_map = [
        ("count","Observações", "{:.0f}"),
        ("mean", "Média", "{:,.2f}"),
        ("std",  "Desvio Padrão", "{:,.2f}"),
        ("min",  "Mínimo", "{:,.2f}"),
    ]
    labels_map2 = [
        ("25%",  "1º Quartil", "{:,.2f}"),
        ("50%",  "Mediana", "{:,.2f}"),
        ("75%",  "3º Quartil", "{:,.2f}"),
        ("max",  "Máximo", "{:,.2f}"),
    ]
    for i, (k, label, fmt_str) in enumerate(labels_map):
        col_s[i].metric(label, fmt_str.format(stats[k]))
    col_s2 = st.columns(4)
    for i, (k, label, fmt_str) in enumerate(labels_map2):
        col_s2[i].metric(label, fmt_str.format(stats[k]))

    # Boxplot por ano
    fig_box = px.box(df, x='ano', y='valor', color='ano',
                     title="Distribuição por Ano",
                     labels={'valor': unidade, 'ano': 'Ano'},
                     color_discrete_sequence=px.colors.qualitative.Bold)
    fig_box.update_layout(height=360, showlegend=False,
                          plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig_box, use_container_width=True)

with tab_mun:
    stats_mun = (df.groupby('municipio')['valor']
                   .agg(['sum','mean','std','count','min','max'])
                   .reset_index()
                   .sort_values('sum', ascending=False))
    stats_mun.columns = ['Município','Total','Média','Desvio Padrão','Registros','Mínimo','Máximo']
    st.dataframe(stats_mun.style.format({
        'Total': '{:,.2f}', 'Média': '{:,.2f}', 'Desvio Padrão': '{:,.2f}',
        'Mínimo': '{:,.2f}', 'Máximo': '{:,.2f}',
    }), use_container_width=True, height=380)

with tab_sg:
    stats_sg = (df.groupby('subgrupo_procedimento')['valor']
                  .agg(['sum','mean','std','count'])
                  .reset_index()
                  .sort_values('sum', ascending=False))
    stats_sg.columns = ['Subgrupo','Total','Média','Desvio Padrão','Registros']
    st.dataframe(stats_sg.style.format({
        'Total': '{:,.2f}', 'Média': '{:,.2f}', 'Desvio Padrão': '{:,.2f}',
    }), use_container_width=True, height=380)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3 · GRÁFICOS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">3 · Gráficos</div>', unsafe_allow_html=True)

PALETTE = px.colors.qualitative.Bold


# 3.1 · Evolução Temporal Mensal ───────────────────────────────────────────────
st.markdown("##### 3.1 · Evolução Mensal Total")
serie_tempo = (df.groupby(['ano','mes','periodo'])['valor'].sum().reset_index()
                 .sort_values(['ano','mes']))
serie_tempo['data_label'] = serie_tempo['periodo']

fig_linha = px.area(serie_tempo, x='data_label', y='valor',
                    title=f"Evolução Mensal · {tipo_sel.replace('_',' ')}",
                    labels={'valor': unidade, 'data_label': 'Período'},
                    color_discrete_sequence=['#0057B8'])
fig_linha.update_traces(line_width=2.5, fillcolor='rgba(0,87,184,0.15)')
fig_linha.update_xaxes(tickangle=-45, tickfont_size=10)
fig_linha.update_layout(height=380, plot_bgcolor='white', paper_bgcolor='white',
                        xaxis=dict(showgrid=False),
                        yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_linha, use_container_width=True)


# 3.2 · Top N Municípios (Barras) ──────────────────────────────────────────────
st.markdown(f"##### 3.2 · Top {top_n} Municípios")
top_mun = (df.groupby('municipio')['valor'].sum()
             .nlargest(top_n).reset_index()
             .sort_values('valor'))

fig_bar = px.bar(top_mun, x='valor', y='municipio', orientation='h',
                 title=f"Top {top_n} · Total {unidade}",
                 labels={'valor': unidade, 'municipio': ''},
                 color='valor',
                 color_continuous_scale='Blues')
fig_bar.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                      coloraxis_showscale=False,
                      yaxis=dict(tickfont_size=11))
st.plotly_chart(fig_bar, use_container_width=True)


# 3.3 · Participação por Subgrupo (Pizza + Treemap) ────────────────────────────
col_p1, col_p2 = st.columns(2)

sg_totais = df.groupby('subgrupo_procedimento')['valor'].sum().reset_index()
sg_totais.columns = ['subgrupo', 'total']
sg_totais = sg_totais.sort_values('total', ascending=False)

with col_p1:
    st.markdown("##### 3.3a · Pizza — Subgrupos")
    fig_pie = px.pie(sg_totais, names='subgrupo', values='total',
                     color_discrete_sequence=PALETTE,
                     hole=0.4)
    fig_pie.update_traces(textposition='outside', textinfo='percent+label',
                          textfont_size=10)
    fig_pie.update_layout(height=400, showlegend=False,
                          paper_bgcolor='white')
    st.plotly_chart(fig_pie, use_container_width=True)

with col_p2:
    st.markdown("##### 3.3b · Treemap — Município × Subgrupo")
    top_mun_names = (df.groupby('municipio')['valor'].sum()
                       .nlargest(top_n).index.tolist())
    df_tree = df[df['municipio'].isin(top_mun_names)]
    tree_data = df_tree.groupby(['municipio','subgrupo_procedimento'])['valor'].sum().reset_index()

    fig_tree = px.treemap(tree_data, path=['municipio','subgrupo_procedimento'],
                          values='valor',
                          color='valor',
                          color_continuous_scale='teal',
                          title=f"Top {top_n} Municípios × Subgrupo")
    fig_tree.update_layout(height=400, paper_bgcolor='white')
    st.plotly_chart(fig_tree, use_container_width=True)


# 3.4 · Heatmap Município × Mês ────────────────────────────────────────────────
st.markdown("##### 3.4 · Heatmap — Municípios × Período")
top10_mun = (df.groupby('municipio')['valor'].sum().nlargest(10).index.tolist())
pivot = (df[df['municipio'].isin(top10_mun)]
           .groupby(['municipio','periodo','mes','ano'])['valor'].sum()
           .reset_index()
           .sort_values(['ano','mes']))

pivot_wide = pivot.pivot_table(index='municipio', columns='periodo',
                                values='valor', aggfunc='sum')
# Ordenar colunas cronologicamente
periodo_order = (pivot[['periodo','ano','mes']].drop_duplicates()
                  .sort_values(['ano','mes'])['periodo'].tolist())
periodo_order = [p for p in periodo_order if p in pivot_wide.columns]
pivot_wide = pivot_wide[periodo_order]

fig_heat = go.Figure(data=go.Heatmap(
    z=pivot_wide.values,
    x=pivot_wide.columns.tolist(),
    y=pivot_wide.index.tolist(),
    colorscale='Blues',
    colorbar=dict(title=unidade),
    hoverongaps=False,
))
fig_heat.update_layout(
    title=f"Top 10 Municípios × Período · {unidade}",
    xaxis=dict(tickangle=-45, tickfont_size=9),
    yaxis=dict(tickfont_size=10),
    height=450,
    plot_bgcolor='white', paper_bgcolor='white',
)
st.plotly_chart(fig_heat, use_container_width=True)


# 3.5 · Linha por Subgrupo ao longo do tempo ───────────────────────────────────
st.markdown("##### 3.5 · Evolução por Subgrupo")
sg_tempo = (df.groupby(['subgrupo_procedimento','ano','mes','periodo'])['valor']
              .sum().reset_index()
              .sort_values(['ano','mes']))

fig_sg = px.line(sg_tempo, x='periodo', y='valor',
                 color='subgrupo_procedimento',
                 title="Evolução Mensal por Subgrupo de Procedimento",
                 labels={'valor': unidade, 'periodo': 'Período',
                         'subgrupo_procedimento': 'Subgrupo'},
                 color_discrete_sequence=PALETTE,
                 markers=True)
fig_sg.update_layout(height=450, plot_bgcolor='white', paper_bgcolor='white',
                     legend=dict(orientation='v', yanchor='top', y=1,
                                 xanchor='left', x=1.01, font_size=10),
                     xaxis=dict(tickangle=-45, tickfont_size=9),
                     yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_sg, use_container_width=True)


# 3.6 · Comparação Anual por Município (Grouped Bar) ──────────────────────────
st.markdown("##### 3.6 · Comparativo Anual — Top Municípios")
anual_mun = (df[df['municipio'].isin(top10_mun)]
               .groupby(['municipio','ano'])['valor'].sum().reset_index())

fig_grp = px.bar(anual_mun, x='municipio', y='valor', color='ano',
                 barmode='group',
                 title="Total Anual por Município",
                 labels={'valor': unidade, 'municipio': 'Município'},
                 color_discrete_sequence=PALETTE)
fig_grp.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                      xaxis=dict(tickangle=-30),
                      yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_grp, use_container_width=True)


# 3.7 · Scatter Qtd × Valor ────────────────────────────────────────────────────
st.markdown("##### 3.7 · Dispersão: Qtd. × Valor por Município")
@st.cache_data(ttl=300)
def load_scatter(mun_list, sg_list, anos_list):
    mun_ph = ",".join(["?"]*len(mun_list))
    sg_ph  = ",".join(["?"]*len(sg_list))
    ano_ph = ",".join(["?"]*len(anos_list))
    sql = f"""
        SELECT municipio, subgrupo_procedimento, ano,
               SUM(CASE WHEN tipo='Qtd_aprovada' THEN valor ELSE 0 END) AS qtd,
               SUM(CASE WHEN tipo='Val_aprovado'  THEN valor ELSE 0 END) AS val
        FROM producao_hospitalar
        WHERE municipio IN ({mun_ph})
          AND subgrupo_procedimento IN ({sg_ph})
          AND ano IN ({ano_ph})
        GROUP BY municipio, subgrupo_procedimento, ano
    """
    params = mun_list + sg_list + anos_list
    return query(sql, params)

df_scatter = load_scatter(mun_sel, sg_sel, [int(a) for a in anos_sel])
if not df_scatter.empty and df_scatter['qtd'].sum() > 0:
    fig_sc = px.scatter(df_scatter, x='qtd', y='val',
                        color='municipio', symbol='ano',
                        size='qtd', size_max=25,
                        hover_data=['subgrupo_procedimento'],
                        title="Dispersão: Quantidade vs Valor por Município/Subgrupo",
                        labels={'qtd':'Qtd Aprovada','val':'Valor Aprovado (R$)'},
                        color_discrete_sequence=PALETTE,
                        opacity=0.75)
    fig_sc.update_layout(height=480, plot_bgcolor='white', paper_bgcolor='white',
                         legend=dict(font_size=9))
    st.plotly_chart(fig_sc, use_container_width=True)
else:
    st.info("Selecione ambos os conteúdos (Qtd e Valor) para visualizar a dispersão.")


# ── Rodapé ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<p style="font-size:0.75rem;color:#999;text-align:center;">
  Fonte: <a href="https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def" target="_blank">
  DATASUS · SIH/SUS</a> · Produção Hospitalar · Dados Detalhados de AIH (SP) · Brasil Município
</p>
""", unsafe_allow_html=True)
