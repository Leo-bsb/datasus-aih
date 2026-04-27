"""
DATASUS - Dashboard SIH/SUS
Lê exclusivamente as tabelas pré-computadas pelo datasus_loader.py.
Nenhum CSV é processado em runtime.
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
PALETTE = px.colors.qualitative.Bold

TIPO_LABEL = {
    "Qtd_aprovada": "Quantidade Aprovada (AIH)",
    "Val_aprovado": "Valor Aprovado (R$)",
}
TIPO_UNIDADE = {
    "Qtd_aprovada": "AIH",
    "Val_aprovado": "R$",
}

MESES_NOME = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def fmt(v: float, unidade: str) -> str:
    """Formata números com escala legível + unidade."""
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:,.2f} bi {unidade}"
    if v >= 1_000_000:
        return f"{v / 1_000_000:,.2f} mi {unidade}"
    if v >= 1_000:
        return f"{v / 1_000:,.1f} mil {unidade}"
    return f"{v:,.0f} {unidade}"


def fmt_num(v: float) -> str:
    """Formata número sem unidade para tabelas."""
    if v >= 1_000_000:
        return f"{v / 1_000_000:,.2f} mi"
    if v >= 1_000:
        return f"{v / 1_000:,.1f} mil"
    return f"{v:,.0f}"


# ── Conexão ────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error("Banco `datasus.db` não encontrado. Execute `python datasus_loader.py` primeiro.")
        st.stop()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data(ttl=600)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn(), params=params)


# ── Carregar listas para filtros ───────────────────────────────────────────────

@st.cache_data(ttl=600)
def load_meta():
    tipos = query(
        "SELECT DISTINCT tipo FROM ranking_municipios ORDER BY tipo"
    )['tipo'].tolist()

    municipios = query("""
        SELECT DISTINCT municipio
        FROM ranking_municipios
        WHERE lower(municipio) NOT LIKE '%ignorado%'
        ORDER BY municipio
    """)['municipio'].tolist()

    anos = query("""
        SELECT DISTINCT ano FROM serie_temporal
        WHERE ano BETWEEN 2024 AND 2026
        ORDER BY ano
    """)['ano'].tolist()

    periodos = query("""
        SELECT DISTINCT periodo, ano, mes
        FROM serie_temporal
        ORDER BY ano, mes
    """)

    return tipos, municipios, anos, periodos


tipos_disp, municipios_disp, anos_disp, periodos_meta = load_meta()

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  h1, h2, h3, h4, h5 { font-family: 'Space Mono', monospace; }
  .block-container { padding-top: 1.5rem; }

  .kpi-card {
    background: linear-gradient(135deg, #0D1B2A 0%, #1a3a5c 100%);
    border-radius: 12px;
    padding: 1.1rem 1.4rem;
    color: white;
    border-left: 4px solid #00C49A;
    margin-bottom: 0.5rem;
    min-height: 90px;
  }
  .kpi-card .kpi-label {
    font-size: 0.7rem;
    opacity: 0.7;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 4px;
  }
  .kpi-card .kpi-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.55rem;
    font-weight: 700;
    color: #00C49A;
    line-height: 1.2;
  }
  .kpi-card .kpi-sub {
    font-size: 0.7rem;
    opacity: 0.55;
    margin-top: 4px;
  }

  .section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #0057B8;
    border-bottom: 2px solid #0057B8;
    padding-bottom: 0.3rem;
    margin: 1.8rem 0 1rem 0;
  }

  .context-note {
    font-size: 0.78rem;
    color: #666;
    background: #f0f4f8;
    border-radius: 6px;
    padding: 0.5rem 0.8rem;
    margin-bottom: 0.8rem;
  }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#0D1B2A,#0057B8);color:white;
                padding:1rem 1.2rem;border-radius:8px;text-align:center;margin-bottom:1.2rem">
      <div style="font-family:'Space Mono',monospace;color:#00C49A;font-size:1rem;font-weight:700">
        🏥 SIH / SUS
      </div>
      <div style="font-size:0.73rem;opacity:0.75;margin-top:4px">
        Produção Hospitalar<br>Brasil Município · Jan/2024–Jan/2026
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Conteúdo
    tipo_sel = st.radio(
        "Conteúdo",
        tipos_disp,
        format_func=lambda x: TIPO_LABEL.get(x, x),
    )
    unidade = TIPO_UNIDADE.get(tipo_sel, "")

    st.divider()

    # Ano
    anos_sel = st.multiselect("Ano", anos_disp, default=anos_disp)

    st.divider()

    # Municípios
    todos_mun = st.checkbox("Todos os municípios", value=True)

    if todos_mun:
        mun_sel = municipios_disp
    else:
        mun_default = municipios_disp[:10] if len(municipios_disp) >= 10 else municipios_disp
        mun_sel = st.multiselect(
            "Municípios",
            municipios_disp,
            default=mun_default,
            help="Selecione um ou mais municípios para filtrar os gráficos.",
        )

    st.divider()

    top_n = st.slider("Top N municípios nos gráficos", 5, 30, 10)

    st.caption(
        f"**{len(mun_sel)}** município(s) selecionado(s)"
    )


# ── Validação ──────────────────────────────────────────────────────────────────
if not mun_sel or not anos_sel:
    st.warning("Selecione ao menos um município e um ano no painel lateral.")
    st.stop()

mun_ph  = ",".join(["?"] * len(mun_sel))
ano_ph  = ",".join(["?"] * len(anos_sel))
params_base = list(mun_sel) + [int(a) for a in anos_sel]


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<h1 style="font-family:'Space Mono',monospace;font-size:1.5rem;color:#0D1B2A;margin-bottom:0.1rem">
  📊 DATASUS · Produção Hospitalar
</h1>
<p style="color:#666;margin-top:0;font-size:0.88rem">
  Dados Detalhados de AIH (SP) · Internação · Brasil Município · Jan/2024 – Jan/2026
</p>
""", unsafe_allow_html=True)


# ── KPIs principais ────────────────────────────────────────────────────────────
# Busca totais do ranking para os municípios e anos selecionados
df_rank_sel = query(f"""
    SELECT r.municipio, r.tipo, r.total, r.media_mensal, r.n_periodos
    FROM ranking_municipios r
    WHERE r.tipo = ?
      AND r.municipio IN ({mun_ph})
""", [tipo_sel] + list(mun_sel))

# Filtrar por anos (precisamos da granular para isso)
df_gran_sel = query(f"""
    SELECT municipio, periodo, mes, ano, valor
    FROM producao_hospitalar
    WHERE tipo = ?
      AND municipio IN ({mun_ph})
      AND ano IN ({ano_ph})
""", [tipo_sel] + params_base)

if df_gran_sel.empty:
    st.warning("Nenhum dado para os filtros selecionados.")
    st.stop()

total_geral      = df_gran_sel['valor'].sum()
media_por_mun    = df_gran_sel.groupby('municipio')['valor'].sum().mean()
n_mun_real       = df_gran_sel['municipio'].nunique()
n_periodos_real  = df_gran_sel['periodo'].nunique()
total_aih_equiv  = total_geral  # pode ser R$ ou AIH dependendo do tipo

# Período de referência
per_min = df_gran_sel.sort_values(['ano', 'mes']).iloc[0]['periodo']
per_max = df_gran_sel.sort_values(['ano', 'mes']).iloc[-1]['periodo']

st.markdown(f"""
<div class="context-note">
  Exibindo <strong>{TIPO_LABEL[tipo_sel]}</strong> —
  {n_mun_real:,} município(s) selecionado(s) |
  {n_periodos_real} período(s): <strong>{per_min} → {per_max}</strong>
</div>
""", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)

kpis = [
    (c1, f"Total ({unidade})", fmt(total_geral, unidade),
     f"Soma de todos os {n_periodos_real} meses e {n_mun_real} municípios selecionados",
     "#00C49A"),
    (c2, f"Média por município ({unidade})",
     fmt(media_por_mun, unidade),
     f"Média da soma total de cada município no período completo",
     "#4da8ff"),
    (c3, "Municípios", f"{n_mun_real:,}",
     "Com pelo menos 1 registro no filtro atual",
     "#FF6B35"),
    (c4, "Períodos", f"{n_periodos_real}",
     f"Meses com dados: {per_min} até {per_max}",
     "#a78bfa"),
]

for col, label, value, sub, color in kpis:
    with col:
        st.markdown(f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value" style="color:{color}">{value}</div>
          <div class="kpi-sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)


# Ordenação temporal dos períodos para eixos X
periodo_order = (
    df_gran_sel[['periodo', 'ano', 'mes']]
    .drop_duplicates()
    .sort_values(['ano', 'mes'])['periodo']
    .tolist()
)


# ── 1 · Lista de dados ─────────────────────────────────────────────────────────
st.markdown('<div class="section-title">1 · Lista dos Dados Armazenados</div>',
            unsafe_allow_html=True)

with st.expander("Clique para expandir a tabela completa", expanded=False):
    df_lista = (
        df_gran_sel
        .groupby(['municipio', 'periodo', 'ano', 'mes'], as_index=False)['valor']
        .sum()
        .sort_values(['ano', 'mes', 'municipio'])
    )
    label_v = "Valor (R$)" if tipo_sel == "Val_aprovado" else "Quantidade (AIH)"
    df_lista = df_lista.rename(columns={
        'valor': label_v, 'municipio': 'Município',
        'periodo': 'Período', 'ano': 'Ano', 'mes': 'Mês',
    })
    st.dataframe(df_lista, use_container_width=True, height=420)

    col_dl1, col_dl2 = st.columns([1, 5])
    with col_dl1:
        st.download_button(
            "⬇️ Baixar CSV",
            df_lista.to_csv(index=False, sep=';').encode('utf-8-sig'),
            "datasus_filtrado.csv",
            "text/csv",
        )
    with col_dl2:
        st.caption(f"{len(df_lista):,} linhas · {n_mun_real} municípios · {n_periodos_real} períodos")


# ── 2 · Estatísticas descritivas ───────────────────────────────────────────────
st.markdown('<div class="section-title">2 · Estatísticas Descritivas</div>',
            unsafe_allow_html=True)

# Totais por município no filtro atual (para stats ao vivo)
tot_mun = df_gran_sel.groupby('municipio')['valor'].sum()
desc = tot_mun.describe()

st.markdown(f"""
<div class="context-note">
  As métricas abaixo descrevem a <em>soma acumulada por município</em>
  no período selecionado ({per_min} a {per_max}).
  Ex.: "Média" = média entre os totais de cada município.
</div>
""", unsafe_allow_html=True)

tab_geral, tab_mun, tab_per, tab_anual = st.tabs(
    ["Geral", "Por Município", "Por Período", "Por Ano"]
)

with tab_geral:
    g1, g2, g3, g4 = st.columns(4)
    metricas_gerais = [
        (g1, f"Nº municípios com dados", f"{int(desc['count']):,}", ""),
        (g2, f"Média por município", fmt(desc['mean'], unidade),
         "Média da soma total por município"),
        (g3, f"Desvio padrão", fmt(desc['std'], unidade),
         "Dispersão entre municípios"),
        (g4, f"Maior município", fmt(desc['max'], unidade),
         f"Município: {tot_mun.idxmax()}"),
    ]
    for col, lbl, val, sub in metricas_gerais:
        col.metric(lbl, val, help=sub)

   

with tab_mun:
    df_by_mun = (
        df_gran_sel
        .groupby('municipio')['valor']
        .agg(['sum', 'mean', 'max', 'min', 'count'])
        .reset_index()
        .sort_values('sum', ascending=False)
        .rename(columns={
            'municipio': 'Município',
            'sum': f'Total ({unidade})',
            'mean': f'Média mensal ({unidade})',
            'max': f'Maior mês ({unidade})',
            'min': f'Menor mês ({unidade})',
            'count': 'Meses c/ dados',
        })
    )
    st.dataframe(
        df_by_mun.style.format({
            f'Total ({unidade})': '{:,.0f}',
            f'Média mensal ({unidade})': '{:,.0f}',
            f'Maior mês ({unidade})': '{:,.0f}',
            f'Menor mês ({unidade})': '{:,.0f}',
        }),
        use_container_width=True, height=420,
    )
    st.caption("'Média mensal' = média dos meses com dados para aquele município.")

with tab_per:
    df_by_per = (
        df_gran_sel
        .groupby(['periodo', 'ano', 'mes'])['valor']
        .agg(total='sum', media='mean', municipios='count')
        .reset_index()
        .sort_values(['ano', 'mes'])
        .rename(columns={
            'periodo': 'Período', 'ano': 'Ano', 'mes': 'Mês',
            'total': f'Total ({unidade})',
            'media': f'Média por município ({unidade})',
            'municipios': 'Municípios c/ dados',
        })
    )
    st.dataframe(
        df_by_per.style.format({
            f'Total ({unidade})': '{:,.0f}',
            f'Média por município ({unidade})': '{:,.0f}',
        }),
        use_container_width=True, height=420,
    )
    st.caption("'Média por município' = total do mês ÷ nº de municípios com dados naquele mês.")

with tab_anual:
    df_anual_stats = (
        df_gran_sel
        .groupby(['ano', 'municipio'])['valor']
        .sum()
        .reset_index()
        .groupby('ano')['valor']
        .agg(['sum', 'mean', 'std', 'max', 'min', 'count'])
        .reset_index()
        .rename(columns={
            'ano': 'Ano',
            'sum': f'Total ({unidade})',
            'mean': f'Média por município ({unidade})',
            'std': 'Desvio padrão',
            'max': f'Maior município ({unidade})',
            'min': f'Menor município ({unidade})',
            'count': 'Municípios',
        })
    )
    st.dataframe(
        df_anual_stats.style.format({
            f'Total ({unidade})': '{:,.0f}',
            f'Média por município ({unidade})': '{:,.0f}',
            'Desvio padrão': '{:,.0f}',
            f'Maior município ({unidade})': '{:,.0f}',
            f'Menor município ({unidade})': '{:,.0f}',
        }),
        use_container_width=True,
    )
    st.caption("'Média por município' = total anual ÷ municípios com dados naquele ano.")


# ── 3 · Gráficos ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">3 · Gráficos</div>',
            unsafe_allow_html=True)

# Dados do Top N para gráficos filtrados
top_nomes = (
    df_gran_sel.groupby('municipio')['valor'].sum()
    .nlargest(top_n).index.tolist()
)
df_top = (
    df_gran_sel[df_gran_sel['municipio'].isin(top_nomes)]
    .groupby(['municipio', 'periodo', 'ano', 'mes'], as_index=False)['valor']
    .sum()
    .sort_values(['ano', 'mes'])
)


# 3.1 Evolução mensal total (série temporal — todos municípios selecionados)
st.markdown("##### 3.1 · Evolução Mensal — Total de Todos os Municípios Selecionados")
st.caption(f"Soma de todos os {n_mun_real} municípios selecionados por mês.")

serie = (
    df_gran_sel
    .groupby(['periodo', 'ano', 'mes'], as_index=False)['valor'].sum()
    .sort_values(['ano', 'mes'])
)
fig_area = px.area(
    serie, x='periodo', y='valor',
    labels={'valor': f'Total ({unidade})', 'periodo': 'Período'},
    color_discrete_sequence=['#0057B8'],
    category_orders={'periodo': periodo_order},
)
fig_area.update_traces(line_width=2.5, fillcolor='rgba(0,87,184,0.12)')
fig_area.update_layout(
    height=360, plot_bgcolor='white', paper_bgcolor='white',
    xaxis=dict(tickangle=-45, tickfont_size=10, showgrid=False),
    yaxis=dict(gridcolor='#eee', title=f'Total ({unidade})'),
)
st.plotly_chart(fig_area, use_container_width=True)


# 3.2 Top N municípios — total do período
st.markdown(f"##### 3.2 · Top {top_n} Municípios — Total Acumulado no Período")
st.caption(f"Soma de {per_min} a {per_max} por município. Mostra os {top_n} maiores.")

df_rank_top = (
    df_gran_sel.groupby('municipio')['valor'].sum()
    .nlargest(top_n).reset_index()
    .sort_values('valor')
    .rename(columns={'municipio': 'Município', 'valor': f'Total ({unidade})'})
)
fig_bar = px.bar(
    df_rank_top, x=f'Total ({unidade})', y='Município',
    orientation='h',
    color=f'Total ({unidade})', color_continuous_scale='Blues',
    text=df_rank_top[f'Total ({unidade})'].apply(lambda v: fmt_num(v)),
)
fig_bar.update_traces(textposition='outside')
fig_bar.update_layout(
    height=max(380, top_n * 40),
    plot_bgcolor='white', paper_bgcolor='white',
    coloraxis_showscale=False,
)
st.plotly_chart(fig_bar, use_container_width=True)


# 3.3 Evolução mensal por município (Top N)
st.markdown(f"##### 3.3 · Evolução Mensal — Top {top_n} Municípios")
st.caption(f"Valores mensais dos {top_n} municípios com maior total no período.")

fig_lines = px.line(
    df_top, x='periodo', y='valor', color='municipio',
    labels={'valor': f'{unidade}/mês', 'periodo': 'Período', 'municipio': 'Município'},
    color_discrete_sequence=PALETTE,
    markers=True,
    category_orders={'periodo': periodo_order},
)
fig_lines.update_layout(
    height=460, plot_bgcolor='white', paper_bgcolor='white',
    xaxis=dict(tickangle=-45, tickfont_size=9),
    yaxis=dict(gridcolor='#eee'),
    legend=dict(font_size=10),
)
st.plotly_chart(fig_lines, use_container_width=True)


# 3.4 Heatmap
st.markdown(f"##### 3.4 · Heatmap — Top {top_n} Municípios × Período")
st.caption("Intensidade de cor = volume mensal. Permite ver sazonalidade e outliers.")

pivot_wide = df_top.pivot_table(
    index='municipio', columns='periodo', values='valor', aggfunc='sum'
)
cols_ord = [p for p in periodo_order if p in pivot_wide.columns]
fig_heat = go.Figure(go.Heatmap(
    z=pivot_wide[cols_ord].values,
    x=cols_ord,
    y=pivot_wide.index.tolist(),
    colorscale='Blues',
    colorbar=dict(title=unidade),
))
fig_heat.update_layout(
    height=max(380, top_n * 38),
    plot_bgcolor='white', paper_bgcolor='white',
    xaxis=dict(tickangle=-45, tickfont_size=9),
    yaxis=dict(tickfont_size=10),
)
st.plotly_chart(fig_heat, use_container_width=True)


# 3.5 Comparativo anual por município
st.markdown(f"##### 3.5 · Comparativo Anual — Top {top_n} Municípios")
st.caption("Total por ano. Permite comparar crescimento/queda entre anos.")

df_anual_top = (
    df_top.groupby(['municipio', 'ano'], as_index=False)['valor'].sum()
)
fig_anual = px.bar(
    df_anual_top, x='municipio', y='valor', color='ano',
    barmode='group',
    labels={'valor': f'Total anual ({unidade})', 'municipio': 'Município', 'ano': 'Ano'},
    color_discrete_sequence=PALETTE,
)
fig_anual.update_layout(
    height=440, plot_bgcolor='white', paper_bgcolor='white',
    xaxis=dict(tickangle=-30),
    yaxis=dict(gridcolor='#eee'),
)
st.plotly_chart(fig_anual, use_container_width=True)


# ── Rodapé ─────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "<p style='font-size:0.75rem;color:#999;text-align:center'>"
    "Fonte: <a href='https://tabnet.datasus.gov.br' target='_blank'>"
    "DATASUS · SIH/SUS</a> · Produção Hospitalar · "
    "Dados Detalhados de AIH (SP) · Brasil Município · Jan/2024–Jan/2026"
    "</p>",
    unsafe_allow_html=True,
)