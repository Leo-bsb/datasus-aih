"""
DATASUS - Dashboard SIH/SUS
Produção Hospitalar por Município × Período
Jan/2024 a Jan/2026
"""

import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

# ── Configuração ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DATASUS · SIH/SUS",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("datasus.db")

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  h1, h2, h3 { font-family: 'Space Mono', monospace; }
  .block-container { padding-top: 1.5rem; }
  .metric-card {
    background: linear-gradient(135deg, #0D1B2A 0%, #1a3a5c 100%);
    border-radius: 12px; padding: 1.2rem 1.5rem; color: white;
    border-left: 4px solid #00C49A; margin-bottom: 0.5rem;
  }
  .metric-card .label { font-size: 0.75rem; opacity: 0.7; text-transform: uppercase; letter-spacing: 0.1em; }
  .metric-card .value { font-family: 'Space Mono', monospace; font-size: 1.8rem; font-weight: 700; color: #00C49A; }
  .section-title {
    font-family: 'Space Mono', monospace; font-size: 0.85rem;
    letter-spacing: 0.15em; text-transform: uppercase; color: #0057B8;
    border-bottom: 2px solid #0057B8; padding-bottom: 0.3rem; margin: 1.5rem 0 1rem 0;
  }
</style>
""", unsafe_allow_html=True)

PALETTE = px.colors.qualitative.Bold

# ── DB ─────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(f"Banco `{DB_PATH}` não encontrado. Execute `python datasus_loader.py`.")
        st.stop()
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@st.cache_data(ttl=300)
def query(sql, params=()):
    return pd.read_sql_query(sql, get_conn(), params=params)

@st.cache_data(ttl=300)
def load_filters():
    municipios = query("""
        SELECT DISTINCT municipio FROM producao_hospitalar
        WHERE lower(municipio) NOT LIKE '%ignorado%'
        ORDER BY municipio
    """)['municipio'].tolist()

    anos = query("""
        SELECT DISTINCT ano FROM producao_hospitalar
        WHERE ano BETWEEN 2024 AND 2026
        ORDER BY ano
    """)['ano'].tolist()

    tipos = query("SELECT DISTINCT tipo FROM producao_hospitalar ORDER BY tipo")['tipo'].tolist()

    periodos = query("""
        SELECT DISTINCT periodo, ano, mes FROM producao_hospitalar
        WHERE ano BETWEEN 2024 AND 2026
        ORDER BY ano, mes
    """)

    return municipios, anos, tipos, periodos

municipios, anos_disp, tipos_disp, periodos_df = load_filters()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#0D1B2A,#0057B8);color:white;
                padding:1rem;border-radius:8px;text-align:center;margin-bottom:1rem">
        <div style="font-family:'Space Mono',monospace;color:#00C49A;font-size:0.9rem">🏥 SIH / SUS</div>
        <div style="font-size:0.75rem;opacity:0.7">Produção Hospitalar · SP<br>Jan/2024 – Jan/2026</div>
    </div>
    """, unsafe_allow_html=True)

    anos_sel = st.multiselect("Ano", anos_disp, default=anos_disp)

    mun_default = municipios[:10] if len(municipios) >= 10 else municipios
    mun_sel = st.multiselect("Municípios", municipios, default=mun_default)

    tipo_label = {"Qtd_aprovada": "Quantidade Aprovada", "Val_aprovado": "Valor Aprovado (R$)"}
    tipo_sel = st.radio("Conteúdo", tipos_disp,
                        format_func=lambda x: tipo_label.get(x, x))

    st.divider()
    top_n = st.slider("Top N municípios", 5, 20, 10)

# ── Query principal ────────────────────────────────────────────────────────────
def load_data():
    if not mun_sel or not anos_sel:
        return pd.DataFrame()
    mun_ph = ",".join(["?"]*len(mun_sel))
    ano_ph = ",".join(["?"]*len(anos_sel))
    sql = f"""
        SELECT municipio, periodo, mes, ano, tipo, SUM(valor) as valor
        FROM producao_hospitalar
        WHERE tipo=? AND municipio IN ({mun_ph}) AND ano IN ({ano_ph})
        GROUP BY municipio, periodo, mes, ano, tipo
    """
    return query(sql, [tipo_sel] + mun_sel + [int(a) for a in anos_sel])

df = load_data()

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<h1 style="font-family:'Space Mono',monospace;font-size:1.6rem;color:#0D1B2A;margin-bottom:0.2rem">
    📊 DATASUS · Produção Hospitalar
</h1>
<p style="color:#666;margin-top:0;font-size:0.9rem">
    Dados Detalhados AIH (SP) · Brasil Município · Jan/2024–Jan/2026
</p>
""", unsafe_allow_html=True)

if df.empty:
    st.warning("Nenhum dado com os filtros selecionados.")
    st.stop()

unidade = "R$" if tipo_sel == "Val_aprovado" else "un."

def fmt(v):
    if v >= 1e9:  return f"{v/1e9:.2f} bi"
    if v >= 1e6:  return f"{v/1e6:.2f} mi"
    if v >= 1e3:  return f"{v/1e3:.1f} mil"
    return f"{v:,.0f}"

total_val = df['valor'].sum()
media_val = df.groupby('municipio')['valor'].sum().mean()
n_mun     = df['municipio'].nunique()
n_per     = df['periodo'].nunique()

c1,c2,c3,c4 = st.columns(4)
cards = [
    (c1, f"Total {unidade}", fmt(total_val), "#00C49A"),
    (c2, "Média por município", fmt(media_val), "#00C49A"),
    (c3, "Municípios", str(n_mun), "#4da8ff"),
    (c4, "Períodos", str(n_per), "#FF6B35"),
]
for col, label, value, color in cards:
    with col:
        st.markdown(f"""<div class="metric-card">
            <div class="label">{label}</div>
            <div class="value" style="color:{color}">{value}</div>
        </div>""", unsafe_allow_html=True)

# ── Ordenar períodos cronologicamente ─────────────────────────────────────────
periodo_order = (df[['periodo','ano','mes']].drop_duplicates()
                   .sort_values(['ano','mes'])['periodo'].tolist())

# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1 · LISTA
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("📋 1 · Lista dos Dados Armazenados", expanded=False):
    tabela = df.sort_values(['ano','mes','municipio'])[
        ['municipio','periodo','ano','mes','valor']
    ].copy()
    label_v = "Valor (R$)" if tipo_sel == "Val_aprovado" else "Quantidade"
    tabela = tabela.rename(columns={'valor': label_v, 'municipio':'Município',
                                    'periodo':'Período','ano':'Ano','mes':'Mês'})
    st.dataframe(tabela, use_container_width=True, height=400)
    st.download_button("⬇️ Baixar CSV",
                       tabela.to_csv(index=False, sep=';').encode('utf-8-sig'),
                       "datasus_filtrado.csv", "text/csv")

# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2 · ESTATÍSTICAS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">2 · Estatísticas Descritivas</div>',
            unsafe_allow_html=True)

tot_mun = df.groupby('municipio')['valor'].sum().reset_index()
stats = tot_mun['valor'].describe()

tab_geral, tab_mun, tab_per = st.tabs(["Geral", "Por Município", "Por Período"])

with tab_geral:
    cols_s = st.columns(4)
    for i, (k, lbl) in enumerate([("count","Municípios"),("mean","Média"),
                                    ("std","Desvio Padrão"),("max","Máximo")]):
        cols_s[i].metric(lbl, f"{stats[k]:,.0f}")

    fig_box = px.box(df, x='ano', y='valor', color='ano',
                     labels={'valor': unidade, 'ano':'Ano'},
                     color_discrete_sequence=PALETTE)
    fig_box.update_layout(height=320, showlegend=False,
                          plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig_box, use_container_width=True)

with tab_mun:
    sm = (df.groupby('municipio')['valor']
            .agg(['sum','mean','max','count'])
            .reset_index().sort_values('sum', ascending=False))
    sm.columns = ['Município','Total','Média','Máximo','Períodos']
    st.dataframe(sm.style.format({'Total':'{:,.0f}','Média':'{:,.0f}','Máximo':'{:,.0f}'}),
                 use_container_width=True, height=380)

with tab_per:
    sp = (df.groupby(['periodo','ano','mes'])['valor']
            .agg(['sum','mean','count'])
            .reset_index().sort_values(['ano','mes']))
    sp.columns = ['Período','Ano','Mês','Total','Média','Municípios']
    st.dataframe(sp.style.format({'Total':'{:,.0f}','Média':'{:,.0f}'}),
                 use_container_width=True, height=380)

# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3 · GRÁFICOS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">3 · Gráficos</div>', unsafe_allow_html=True)

# 3.1 Evolução mensal total
st.markdown("##### 3.1 · Evolução Mensal Total")
serie = (df.groupby(['ano','mes','periodo'])['valor'].sum()
           .reset_index().sort_values(['ano','mes']))
fig_area = px.area(serie, x='periodo', y='valor',
                   labels={'valor': unidade, 'periodo':'Período'},
                   color_discrete_sequence=['#0057B8'],
                   category_orders={'periodo': periodo_order})
fig_area.update_traces(line_width=2.5, fillcolor='rgba(0,87,184,0.15)')
fig_area.update_layout(height=360, plot_bgcolor='white', paper_bgcolor='white',
                       xaxis=dict(tickangle=-45, tickfont_size=10, showgrid=False),
                       yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_area, use_container_width=True)

# 3.2 Top N municípios
st.markdown(f"##### 3.2 · Top {top_n} Municípios — Total do Período")
top_mun = (df.groupby('municipio')['valor'].sum()
             .nlargest(top_n).reset_index().sort_values('valor'))
fig_bar = px.bar(top_mun, x='valor', y='municipio', orientation='h',
                 labels={'valor': unidade, 'municipio':''},
                 color='valor', color_continuous_scale='Blues')
fig_bar.update_layout(height=max(350, top_n*38), plot_bgcolor='white',
                      paper_bgcolor='white', coloraxis_showscale=False)
st.plotly_chart(fig_bar, use_container_width=True)

# 3.3 Evolução por município (linhas)
st.markdown("##### 3.3 · Evolução Mensal por Município")
top_mun_names = (df.groupby('municipio')['valor'].sum()
                   .nlargest(top_n).index.tolist())
df_top = (df[df['municipio'].isin(top_mun_names)]
            .groupby(['municipio','periodo','ano','mes'])['valor'].sum()
            .reset_index().sort_values(['ano','mes']))
fig_lines = px.line(df_top, x='periodo', y='valor', color='municipio',
                    labels={'valor': unidade, 'periodo':'Período',
                            'municipio':'Município'},
                    color_discrete_sequence=PALETTE, markers=True,
                    category_orders={'periodo': periodo_order})
fig_lines.update_layout(height=450, plot_bgcolor='white', paper_bgcolor='white',
                        xaxis=dict(tickangle=-45, tickfont_size=9),
                        yaxis=dict(gridcolor='#eee'),
                        legend=dict(font_size=10))
st.plotly_chart(fig_lines, use_container_width=True)

# 3.4 Heatmap município × período
st.markdown(f"##### 3.4 · Heatmap — Top {top_n} Municípios × Período")
df_heat = df[df['municipio'].isin(top_mun_names)]
pivot = (df_heat.groupby(['municipio','periodo','ano','mes'])['valor']
                .sum().reset_index().sort_values(['ano','mes']))
pivot_wide = pivot.pivot_table(index='municipio', columns='periodo',
                                values='valor', aggfunc='sum')
cols_ord = [p for p in periodo_order if p in pivot_wide.columns]
pivot_wide = pivot_wide[cols_ord]

fig_heat = go.Figure(go.Heatmap(
    z=pivot_wide.values,
    x=pivot_wide.columns.tolist(),
    y=pivot_wide.index.tolist(),
    colorscale='Blues',
    colorbar=dict(title=unidade),
))
fig_heat.update_layout(
    height=max(350, top_n*35),
    xaxis=dict(tickangle=-45, tickfont_size=9),
    yaxis=dict(tickfont_size=10),
    plot_bgcolor='white', paper_bgcolor='white',
)
st.plotly_chart(fig_heat, use_container_width=True)

# 3.5 Comparativo anual (barras agrupadas)
st.markdown(f"##### 3.5 · Comparativo Anual — Top {top_n} Municípios")
anual = (df[df['municipio'].isin(top_mun_names)]
           .groupby(['municipio','ano'])['valor'].sum().reset_index())
fig_anual = px.bar(anual, x='municipio', y='valor', color='ano',
                   barmode='group',
                   labels={'valor': unidade, 'municipio':'Município'},
                   color_discrete_sequence=PALETTE)
fig_anual.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                        xaxis=dict(tickangle=-30), yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_anual, use_container_width=True)

# 3.6 Participação por município (pizza)
st.markdown("##### 3.6 · Participação por Município")
fig_pie = px.pie(top_mun, names='municipio', values='valor',
                 color_discrete_sequence=PALETTE, hole=0.4)
fig_pie.update_traces(textposition='outside', textinfo='percent+label',
                      textfont_size=10)
fig_pie.update_layout(height=420, showlegend=False, paper_bgcolor='white')
st.plotly_chart(fig_pie, use_container_width=True)

# 3.7 Crescimento mês a mês (variação %)
st.markdown("##### 3.7 · Variação Mensal (%) — Top Municípios")
df_var = df_top.copy().sort_values(['municipio','ano','mes'])
df_var['variacao'] = df_var.groupby('municipio')['valor'].pct_change() * 100
df_var = df_var.dropna(subset=['variacao'])
fig_var = px.line(df_var, x='periodo', y='variacao', color='municipio',
                  labels={'variacao':'Variação (%)', 'periodo':'Período',
                          'municipio':'Município'},
                  color_discrete_sequence=PALETTE, markers=True,
                  category_orders={'periodo': periodo_order})
fig_var.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.5)
fig_var.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                      xaxis=dict(tickangle=-45, tickfont_size=9),
                      yaxis=dict(gridcolor='#eee'),
                      legend=dict(font_size=10))
st.plotly_chart(fig_var, use_container_width=True)

st.markdown("---")
st.markdown("""<p style="font-size:0.75rem;color:#999;text-align:center">
  Fonte: <a href="https://tabnet.datasus.gov.br" target="_blank">DATASUS · SIH/SUS</a>
  · Produção Hospitalar · Dados Detalhados de AIH (SP) · Brasil Município
</p>""", unsafe_allow_html=True)
