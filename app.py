"""
DATASUS - Dashboard SIH/SUS
Ao iniciar, constrói o banco SQLite a partir dos CSVs se necessário.
Compatível com Streamlit Cloud (sem banco pré-existente).
"""

import sqlite3
import re
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
CSV_DIR = Path("baixados")

PALETTE   = px.colors.qualitative.Bold
MESES_PT  = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,
             'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}
MESES_NOME= {1:'Jan',2:'Fev',3:'Mar',4:'Abr',5:'Mai',6:'Jun',
             7:'Jul',8:'Ago',9:'Set',10:'Out',11:'Nov',12:'Dez'}
ANOS_ALVO = {2024, 2025, 2026}

RE_AAAA_MMM = re.compile(
    r'^(\d{4})[/\-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)$',
    re.IGNORECASE)
RE_MMM_AAAA = re.compile(
    r'^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[/\-_](\d{4})$',
    re.IGNORECASE)


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_periodo_col(col):
    return bool(RE_AAAA_MMM.match(col.strip()))


def parse_periodo(texto):
    s = str(texto).strip()
    m = RE_AAAA_MMM.match(s)
    if m:
        return MESES_PT.get(m.group(2).lower(), 0), int(m.group(1))
    m = RE_MMM_AAAA.match(s)
    if m:
        return MESES_PT.get(m.group(1).lower(), 0), int(m.group(2))
    return None, None


def normalizar_periodo(mes, ano):
    if mes and ano:
        return f"{MESES_NOME.get(int(mes), '?')}/{int(ano)}"
    return None


def to_float(v):
    s = str(v).strip() if pd.notna(v) else ''
    if s in ('', '-', 'nd', 'ND'):
        return 0.0
    try:
        return float(s.replace('.', '').replace(',', '.'))
    except ValueError:
        return None


def inferir_tipo(path):
    return 'Val_aprovado' if 'valor' in path.stem.lower() else 'Qtd_aprovada'


def inferir_periodo_nome(path):
    m = re.search(r'_([A-Za-z]{3})_(\d{4})', path.stem)
    return f"{m.group(1).capitalize()}/{m.group(2)}" if m else None


def processar_csv(path: Path) -> pd.DataFrame:
    raw = path.read_bytes()
    enc = 'utf-8-sig' if raw[:3] == b'\xef\xbb\xbf' else 'latin-1'
    first_line = raw.decode(enc, errors='replace').split('\n')[0]
    sep = ';' if first_line.count(';') >= first_line.count(',') else ','

    df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, on_bad_lines='skip')
    df.columns = [c.strip() for c in df.columns]
    col0 = df.columns[0]

    df = df[~df[col0].fillna('').str.strip().str.lower()
              .isin(['total', 'nan', '', 'total geral'])]
    df = df.drop(columns=[c for c in df.columns
                           if c.strip().lower() == 'total'], errors='ignore')

    outras = [c for c in df.columns if c != col0]
    periodo_cols = [c for c in outras if is_periodo_col(c)]
    tipo = inferir_tipo(path)

    if periodo_cols:
        # Wide-período: Município × [2024/Jan, 2024/Fev, ...]
        alvo = [c for c in periodo_cols if parse_periodo(c)[1] in ANOS_ALVO]
        df_long = df.melt(id_vars=[col0], value_vars=alvo,
                          var_name='periodo', value_name='valor')
        df_long['subgrupo_procedimento'] = 'Todos os subgrupos'
    else:
        # Wide-subgrupo: Município × [0201 Coleta, 0202 Diag, ...]
        periodo_label = inferir_periodo_nome(path)
        df_long = df.melt(id_vars=[col0], value_vars=outras,
                          var_name='subgrupo_procedimento', value_name='valor')
        df_long['periodo'] = periodo_label

    df_long = df_long.rename(columns={col0: 'municipio'})
    df_long['tipo']  = tipo
    df_long['valor'] = df_long['valor'].apply(to_float)

    parsed = df_long['periodo'].apply(lambda x: pd.Series(parse_periodo(x)))
    df_long['mes'] = parsed[0].astype('Int64')
    df_long['ano'] = parsed[1].astype('Int64')
    df_long['periodo'] = df_long.apply(
        lambda r: normalizar_periodo(r['mes'], r['ano']) or r['periodo'], axis=1)

    df_long = df_long[
        df_long['valor'].notna() &
        df_long['mes'].notna() &
        (df_long['valor'] > 0) &
        df_long['ano'].isin(ANOS_ALVO)
    ]
    cols = ['municipio','subgrupo_procedimento','periodo','mes','ano','tipo','valor']
    return df_long[[c for c in cols if c in df_long.columns]]


# ── Construir banco se necessário ──────────────────────────────────────────────

@st.cache_resource
def build_db():
    """Executado uma vez por sessão. Cria o banco se não existir."""
    if DB_PATH.exists():
        return True

    csvs = (sorted(CSV_DIR.glob("datasus_*.csv")) +
            (sorted((CSV_DIR / "parciais").glob("*.csv"))
             if (CSV_DIR / "parciais").exists() else []))

    if not csvs:
        return False

    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS producao_hospitalar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            municipio TEXT NOT NULL,
            subgrupo_procedimento TEXT NOT NULL,
            periodo TEXT, mes INTEGER, ano INTEGER,
            tipo TEXT NOT NULL, valor REAL
        );
        CREATE INDEX IF NOT EXISTS idx_mun  ON producao_hospitalar(municipio);
        CREATE INDEX IF NOT EXISTS idx_ano  ON producao_hospitalar(ano);
        CREATE INDEX IF NOT EXISTS idx_tipo ON producao_hospitalar(tipo);
        CREATE INDEX IF NOT EXISTS idx_per  ON producao_hospitalar(periodo);
    """)

    with st.spinner(f"Inicializando banco de dados ({len(csvs)} arquivos)..."):
        for path in csvs:
            try:
                df = processar_csv(path)
                if not df.empty:
                    df.to_sql('producao_hospitalar', conn,
                              if_exists='append', index=False)
            except Exception as e:
                st.warning(f"Erro ao carregar {path.name}: {e}")

    conn.close()
    return True


ok = build_db()

if not ok:
    st.error("CSVs não encontrados. Adicione os arquivos em `baixados/` no repositório.")
    st.info("Execute localmente: `python datasus_scraper.py` e depois `python datasus_loader.py`")
    st.stop()


# ── DB queries ─────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
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
        WHERE ano BETWEEN 2024 AND 2026 ORDER BY ano
    """)['ano'].tolist()

    tipos = query(
        "SELECT DISTINCT tipo FROM producao_hospitalar ORDER BY tipo"
    )['tipo'].tolist()

    periodos = query("""
        SELECT DISTINCT periodo, ano, mes FROM producao_hospitalar
        WHERE ano BETWEEN 2024 AND 2026 ORDER BY ano, mes
    """)
    return municipios, anos, tipos, periodos


municipios, anos_disp, tipos_disp, periodos_df = load_filters()

# ── CSS ────────────────────────────────────────────────────────────────────────
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
  .metric-card .label { font-size:0.75rem;opacity:0.7;text-transform:uppercase;letter-spacing:0.1em; }
  .metric-card .value { font-family:'Space Mono',monospace;font-size:1.8rem;font-weight:700;color:#00C49A; }
  .section-title {
    font-family:'Space Mono',monospace;font-size:0.85rem;letter-spacing:0.15em;
    text-transform:uppercase;color:#0057B8;border-bottom:2px solid #0057B8;
    padding-bottom:0.3rem;margin:1.5rem 0 1rem 0;
  }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#0D1B2A,#0057B8);color:white;
                padding:1rem;border-radius:8px;text-align:center;margin-bottom:1rem">
        <div style="font-family:'Space Mono',monospace;color:#00C49A;font-size:0.9rem">🏥 SIH / SUS</div>
        <div style="font-size:0.75rem;opacity:0.7">Produção Hospitalar · SP<br>Jan/2024 – Jan/2026</div>
    </div>
    """, unsafe_allow_html=True)

    anos_sel  = st.multiselect("Ano", anos_disp, default=anos_disp)
    mun_default = municipios[:10] if len(municipios) >= 10 else municipios
    mun_sel   = st.multiselect("Municípios", municipios, default=mun_default)
    tipo_label = {"Qtd_aprovada": "Quantidade Aprovada", "Val_aprovado": "Valor Aprovado (R$)"}
    tipo_sel  = st.radio("Conteúdo", tipos_disp, format_func=lambda x: tipo_label.get(x, x))
    st.divider()
    top_n = st.slider("Top N municípios", 5, 20, 10)


# ── Query principal ────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data(tipo, muns, anos):
    if not muns or not anos:
        return pd.DataFrame()
    mun_ph = ",".join(["?"]*len(muns))
    ano_ph = ",".join(["?"]*len(anos))
    sql = f"""
        SELECT municipio, periodo, mes, ano, tipo, SUM(valor) as valor
        FROM producao_hospitalar
        WHERE tipo=? AND municipio IN ({mun_ph}) AND ano IN ({ano_ph})
        GROUP BY municipio, periodo, mes, ano, tipo
    """
    return query(sql, [tipo] + list(muns) + [int(a) for a in anos])


df = load_data(tipo_sel, tuple(mun_sel), tuple(anos_sel))

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
    if v >= 1e9: return f"{v/1e9:.2f} bi"
    if v >= 1e6: return f"{v/1e6:.2f} mi"
    if v >= 1e3: return f"{v/1e3:.1f} mil"
    return f"{v:,.0f}"

periodo_order = (df[['periodo','ano','mes']].drop_duplicates()
                   .sort_values(['ano','mes'])['periodo'].tolist())

total_val = df['valor'].sum()
media_val = df.groupby('municipio')['valor'].sum().mean()
n_mun     = df['municipio'].nunique()
n_per     = df['periodo'].nunique()

c1, c2, c3, c4 = st.columns(4)
for col, label, value, color in [
    (c1, f"Total {unidade}", fmt(total_val), "#00C49A"),
    (c2, "Média por município", fmt(media_val), "#00C49A"),
    (c3, "Municípios", str(n_mun), "#4da8ff"),
    (c4, "Períodos", str(n_per), "#FF6B35"),
]:
    with col:
        st.markdown(f"""<div class="metric-card">
            <div class="label">{label}</div>
            <div class="value" style="color:{color}">{value}</div>
        </div>""", unsafe_allow_html=True)


# ── Seção 1: Lista ─────────────────────────────────────────────────────────────
with st.expander("📋 1 · Lista dos Dados Armazenados", expanded=False):
    tabela = df.sort_values(['ano','mes','municipio'])[
        ['municipio','periodo','ano','mes','valor']].copy()
    label_v = "Valor (R$)" if tipo_sel == "Val_aprovado" else "Quantidade"
    tabela = tabela.rename(columns={'valor':label_v,'municipio':'Município',
                                    'periodo':'Período','ano':'Ano','mes':'Mês'})
    st.dataframe(tabela, use_container_width=True, height=400)
    st.download_button("⬇️ Baixar CSV",
                       tabela.to_csv(index=False, sep=';').encode('utf-8-sig'),
                       "datasus_filtrado.csv", "text/csv")


# ── Seção 2: Estatísticas ──────────────────────────────────────────────────────
st.markdown('<div class="section-title">2 · Estatísticas Descritivas</div>',
            unsafe_allow_html=True)

tot_mun = df.groupby('municipio')['valor'].sum()
stats   = tot_mun.describe()

tab_geral, tab_mun, tab_per = st.tabs(["Geral","Por Município","Por Período"])

with tab_geral:
    cols_s = st.columns(4)
    for i, (k, lbl) in enumerate([("count","Municípios"),("mean","Média"),
                                    ("std","Desvio Padrão"),("max","Máximo")]):
        cols_s[i].metric(lbl, f"{stats[k]:,.0f}")
    fig_box = px.box(df, x='ano', y='valor', color='ano',
                     labels={'valor':unidade,'ano':'Ano'},
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


# ── Seção 3: Gráficos ──────────────────────────────────────────────────────────
st.markdown('<div class="section-title">3 · Gráficos</div>',
            unsafe_allow_html=True)

top_mun_names = (df.groupby('municipio')['valor'].sum()
                   .nlargest(top_n).index.tolist())
top_mun_df = (df.groupby('municipio')['valor'].sum()
                .nlargest(top_n).reset_index().sort_values('valor'))

# 3.1 Evolução mensal
st.markdown("##### 3.1 · Evolução Mensal Total")
serie = (df.groupby(['ano','mes','periodo'])['valor'].sum()
           .reset_index().sort_values(['ano','mes']))
fig_area = px.area(serie, x='periodo', y='valor',
                   labels={'valor':unidade,'periodo':'Período'},
                   color_discrete_sequence=['#0057B8'],
                   category_orders={'periodo':periodo_order})
fig_area.update_traces(line_width=2.5, fillcolor='rgba(0,87,184,0.15)')
fig_area.update_layout(height=360, plot_bgcolor='white', paper_bgcolor='white',
                       xaxis=dict(tickangle=-45,tickfont_size=10,showgrid=False),
                       yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_area, use_container_width=True)

# 3.2 Top N barras
st.markdown(f"##### 3.2 · Top {top_n} Municípios — Total do Período")
fig_bar = px.bar(top_mun_df, x='valor', y='municipio', orientation='h',
                 labels={'valor':unidade,'municipio':''},
                 color='valor', color_continuous_scale='Blues')
fig_bar.update_layout(height=max(350,top_n*38), plot_bgcolor='white',
                      paper_bgcolor='white', coloraxis_showscale=False)
st.plotly_chart(fig_bar, use_container_width=True)

# 3.3 Linhas por município
st.markdown("##### 3.3 · Evolução Mensal por Município")
df_top = (df[df['municipio'].isin(top_mun_names)]
            .groupby(['municipio','periodo','ano','mes'])['valor'].sum()
            .reset_index().sort_values(['ano','mes']))
fig_lines = px.line(df_top, x='periodo', y='valor', color='municipio',
                    labels={'valor':unidade,'periodo':'Período','municipio':'Município'},
                    color_discrete_sequence=PALETTE, markers=True,
                    category_orders={'periodo':periodo_order})
fig_lines.update_layout(height=450, plot_bgcolor='white', paper_bgcolor='white',
                        xaxis=dict(tickangle=-45,tickfont_size=9),
                        yaxis=dict(gridcolor='#eee'),
                        legend=dict(font_size=10))
st.plotly_chart(fig_lines, use_container_width=True)

# 3.4 Heatmap
st.markdown(f"##### 3.4 · Heatmap — Top {top_n} Municípios × Período")
pivot = (df[df['municipio'].isin(top_mun_names)]
           .groupby(['municipio','periodo','ano','mes'])['valor'].sum()
           .reset_index())
pivot_wide = pivot.pivot_table(index='municipio', columns='periodo',
                                values='valor', aggfunc='sum')
cols_ord = [p for p in periodo_order if p in pivot_wide.columns]
fig_heat = go.Figure(go.Heatmap(
    z=pivot_wide[cols_ord].values,
    x=cols_ord,
    y=pivot_wide.index.tolist(),
    colorscale='Blues', colorbar=dict(title=unidade),
))
fig_heat.update_layout(height=max(350,top_n*35), plot_bgcolor='white',
                       paper_bgcolor='white',
                       xaxis=dict(tickangle=-45,tickfont_size=9),
                       yaxis=dict(tickfont_size=10))
st.plotly_chart(fig_heat, use_container_width=True)

# 3.5 Comparativo anual
st.markdown(f"##### 3.5 · Comparativo Anual — Top {top_n} Municípios")
anual = (df[df['municipio'].isin(top_mun_names)]
           .groupby(['municipio','ano'])['valor'].sum().reset_index())
fig_anual = px.bar(anual, x='municipio', y='valor', color='ano',
                   barmode='group',
                   labels={'valor':unidade,'municipio':'Município'},
                   color_discrete_sequence=PALETTE)
fig_anual.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                        xaxis=dict(tickangle=-30), yaxis=dict(gridcolor='#eee'))
st.plotly_chart(fig_anual, use_container_width=True)

# 3.6 Pizza
st.markdown("##### 3.6 · Participação por Município")
fig_pie = px.pie(top_mun_df, names='municipio', values='valor',
                 color_discrete_sequence=PALETTE, hole=0.4)
fig_pie.update_traces(textposition='outside', textinfo='percent+label',
                      textfont_size=10)
fig_pie.update_layout(height=420, showlegend=False, paper_bgcolor='white')
st.plotly_chart(fig_pie, use_container_width=True)

# 3.7 Variação mensal %
st.markdown("##### 3.7 · Variação Mensal (%) — Top Municípios")
df_var = df_top.copy().sort_values(['municipio','ano','mes'])
df_var['variacao'] = df_var.groupby('municipio')['valor'].pct_change() * 100
df_var = df_var.dropna(subset=['variacao'])
if not df_var.empty:
    fig_var = px.line(df_var, x='periodo', y='variacao', color='municipio',
                      labels={'variacao':'Variação (%)','periodo':'Período',
                              'municipio':'Município'},
                      color_discrete_sequence=PALETTE, markers=True,
                      category_orders={'periodo':periodo_order})
    fig_var.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.5)
    fig_var.update_layout(height=420, plot_bgcolor='white', paper_bgcolor='white',
                          xaxis=dict(tickangle=-45,tickfont_size=9),
                          yaxis=dict(gridcolor='#eee'), legend=dict(font_size=10))
    st.plotly_chart(fig_var, use_container_width=True)

# ── Rodapé ─────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "<p style='font-size:0.75rem;color:#999;text-align:center'>"
    "Fonte: <a href='https://tabnet.datasus.gov.br' target='_blank'>"
    "DATASUS · SIH/SUS</a> · Produção Hospitalar · "
    "Dados Detalhados de AIH (SP) · Brasil Município</p>",
    unsafe_allow_html=True
)