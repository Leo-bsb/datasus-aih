"""
DATASUS - Carregador + Pré-computador de Métricas

Lê os CSVs extraídos pelo scraper e popula o banco SQLite com:
  1. producao_hospitalar  — dados granulares (município × período × tipo)
  2. metricas_resumo      — totais/médias pré-calculados por município × tipo
  3. serie_temporal       — totais mensais agregados (todos os municípios)
  4. ranking_municipios   — ranking total do período por tipo
  5. stats_descritivas    — estatísticas descritivas (count, mean, std, min, max, etc.)

O dashboard só faz SELECTs nessas tabelas — sem processar CSVs em runtime.

Suporta dois formatos de CSV do DATASUS:
  A) Wide-período  : Município ; 2024/Jan ; 2024/Fev ; ...  (novo scraper)
  B) Wide-subgrupo : Município ; 0201 Coleta ; 0202 Diag... (scraper antigo)
"""

import sqlite3
import re
import sys
import pandas as pd
import numpy as np
from pathlib import Path

DB_PATH = Path("datasus.db")

MESES_PT = {
    'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12,
}
MESES_NOME = {v: k.capitalize() for k, v in MESES_PT.items()}

ANOS_ALVO = {2024, 2025, 2026}

# DATASUS gera colunas no formato AAAA/Mmm  ex: 2024/Jan
RE_PERIODO = re.compile(
    r'^(\d{4})[/\-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)$',
    re.IGNORECASE,
)
# Formato alternativo: Mmm/AAAA  ex: Jan/2024
RE_PERIODO_ALT = re.compile(
    r'^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[/\-_](\d{4})$',
    re.IGNORECASE,
)


def is_periodo_col(col: str) -> bool:
    return bool(RE_PERIODO.match(col.strip()))


def parse_periodo(texto) -> tuple:
    """Retorna (mes: int, ano: int) ou (None, None)."""
    s = str(texto).strip()
    m = RE_PERIODO.match(s)
    if m:
        return MESES_PT.get(m.group(2).lower(), 0), int(m.group(1))
    m = RE_PERIODO_ALT.match(s)
    if m:
        return MESES_PT.get(m.group(1).lower(), 0), int(m.group(2))
    return None, None


def to_float(v) -> float | None:
    """Converte string do DATASUS (ponto=milhar, vírgula=decimal) para float."""
    s = str(v).strip() if pd.notna(v) else ''
    if s in ('', '-', 'nd', 'ND', 'nan'):
        return 0.0
    # Remove ponto de milhar, troca vírgula decimal por ponto
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def inferir_tipo(path: Path) -> str:
    nome = path.stem.lower()
    if 'valor' in nome:
        return 'Val_aprovado'
    return 'Qtd_aprovada'


def inferir_periodo_do_nome(path: Path) -> str | None:
    m = re.search(r'_([A-Za-z]{3})_(\d{4})', path.stem)
    if m:
        return f"{m.group(1).capitalize()}/{m.group(2)}"
    return None


# ── Banco de dados ─────────────────────────────────────────────────────────────

def criar_banco(conn: sqlite3.Connection):
    conn.executescript("""
        -- Dados granulares
        CREATE TABLE IF NOT EXISTS producao_hospitalar (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            municipio             TEXT    NOT NULL,
            subgrupo_procedimento TEXT    NOT NULL,
            periodo               TEXT,
            mes                   INTEGER,
            ano                   INTEGER,
            tipo                  TEXT    NOT NULL,
            valor                 REAL
        );

        -- Série temporal agregada (todos municípios, por período)
        CREATE TABLE IF NOT EXISTS serie_temporal (
            periodo   TEXT    NOT NULL,
            mes       INTEGER NOT NULL,
            ano       INTEGER NOT NULL,
            tipo      TEXT    NOT NULL,
            total     REAL,
            media     REAL,
            n_municipios INTEGER,
            PRIMARY KEY (periodo, tipo)
        );

        -- Totais por município no período completo
        CREATE TABLE IF NOT EXISTS ranking_municipios (
            municipio TEXT    NOT NULL,
            tipo      TEXT    NOT NULL,
            total     REAL,
            media_mensal REAL,
            max_mensal   REAL,
            min_mensal   REAL,
            n_periodos   INTEGER,
            PRIMARY KEY (municipio, tipo)
        );

        -- Estatísticas descritivas gerais
        CREATE TABLE IF NOT EXISTS stats_descritivas (
            tipo        TEXT NOT NULL,
            ano         INTEGER,           -- NULL = todos os anos
            metrica     TEXT NOT NULL,
            valor       REAL,
            PRIMARY KEY (tipo, ano, metrica)
        );

        -- Índices
        CREATE INDEX IF NOT EXISTS idx_ph_mun   ON producao_hospitalar(municipio);
        CREATE INDEX IF NOT EXISTS idx_ph_tipo  ON producao_hospitalar(tipo);
        CREATE INDEX IF NOT EXISTS idx_ph_ano   ON producao_hospitalar(ano);
        CREATE INDEX IF NOT EXISTS idx_ph_per   ON producao_hospitalar(periodo);
        CREATE INDEX IF NOT EXISTS idx_ph_mun_tipo ON producao_hospitalar(municipio, tipo);
    """)
    conn.commit()


def limpar_banco(conn: sqlite3.Connection):
    conn.executescript("""
        DELETE FROM producao_hospitalar;
        DELETE FROM serie_temporal;
        DELETE FROM ranking_municipios;
        DELETE FROM stats_descritivas;
    """)
    conn.commit()


# ── Leitura e normalização de CSVs ─────────────────────────────────────────────

def ler_csv(path: Path) -> pd.DataFrame:
    """Lê o CSV detectando encoding e separador automaticamente."""
    # Detectar encoding
    raw = path.read_bytes()
    enc = 'utf-8-sig' if raw[:3] == b'\xef\xbb\xbf' else 'latin-1'

    # Detectar separador na primeira linha
    try:
        first_line = raw.decode(enc, errors='replace').split('\n')[0]
    except Exception:
        first_line = ''
    sep = ';' if first_line.count(';') >= first_line.count(',') else ','

    print(f"    enc={enc!r}  sep={sep!r}")

    for enc2 in (enc, 'utf-8', 'latin-1', 'cp1252'):
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc2,
                             on_bad_lines='skip')
            if not df.empty:
                return df
        except Exception:
            continue

    raise ValueError(f"Não foi possível ler {path.name}")


def processar_csv(path: Path) -> pd.DataFrame:
    df = ler_csv(path)
    df.columns = [c.strip() for c in df.columns]
    col0 = df.columns[0]

    print(f"    Shape bruto: {df.shape}")
    outras = [c for c in df.columns if c != col0]
    print(f"    Colunas ex: {outras[:4]}")

    # Remover linhas de rodapé
    df = df[~df[col0].fillna('').str.strip().str.lower()
              .isin(['total', 'nan', '', 'total geral'])]
    df = df.drop(
        columns=[c for c in df.columns if c.strip().lower() == 'total'],
        errors='ignore',
    )

    outras_cols = [c for c in df.columns if c != col0]
    tipo = inferir_tipo(path)

    # ── Detectar formato ──────────────────────────────────────────────────────
    periodo_cols  = [c for c in outras_cols if is_periodo_col(c)]
    subgrupo_cols = [c for c in outras_cols if not is_periodo_col(c)]

    if periodo_cols:
        # Formato A: wide-período (scraper atual)
        alvo = [c for c in periodo_cols if parse_periodo(c)[1] in ANOS_ALVO]
        print(f"    Formato A (wide-período): {len(periodo_cols)} colunas, "
              f"{len(alvo)} no intervalo alvo")
        df_long = df.melt(id_vars=[col0], value_vars=alvo,
                          var_name='periodo', value_name='valor')
        df_long['subgrupo_procedimento'] = 'Todos os subgrupos'
    else:
        # Formato B: wide-subgrupo (scraper antigo)
        periodo_label = inferir_periodo_do_nome(path)
        print(f"    Formato B (wide-subgrupo): {len(subgrupo_cols)} subgrupos, "
              f"período={periodo_label}")
        df_long = df.melt(id_vars=[col0], value_vars=subgrupo_cols,
                          var_name='subgrupo_procedimento', value_name='valor')
        df_long['periodo'] = periodo_label

    # ── Finalizar ─────────────────────────────────────────────────────────────
    df_long = df_long.rename(columns={col0: 'municipio'})
    df_long['tipo']  = tipo
    df_long['valor'] = df_long['valor'].apply(to_float)

    parsed = df_long['periodo'].apply(lambda x: pd.Series(parse_periodo(x)))
    df_long['mes'] = parsed[0].astype('Int64')
    df_long['ano'] = parsed[1].astype('Int64')

    # Normalizar período para "Jan/2024"
    df_long['periodo'] = df_long.apply(
        lambda r: (f"{MESES_NOME.get(int(r['mes']), '?')}/{int(r['ano'])}"
                   if pd.notna(r['mes']) and pd.notna(r['ano'])
                   else r['periodo']),
        axis=1,
    )

    # Filtrar: apenas valores > 0 e anos alvo
    df_long = df_long[
        df_long['valor'].notna() &
        df_long['mes'].notna() &
        (df_long['valor'] > 0) &
        df_long['ano'].isin(ANOS_ALVO)
    ]

    cols = ['municipio', 'subgrupo_procedimento', 'periodo', 'mes', 'ano', 'tipo', 'valor']
    resultado = df_long[[c for c in cols if c in df_long.columns]].copy()
    print(f"    Shape final: {resultado.shape}")
    return resultado


# ── Pré-computação de métricas ─────────────────────────────────────────────────

def calcular_metricas(conn: sqlite3.Connection):
    """Calcula e persiste todas as métricas derivadas."""
    print("\n  Calculando métricas pré-computadas...")

    df_all = pd.read_sql_query(
        "SELECT municipio, periodo, mes, ano, tipo, valor FROM producao_hospitalar",
        conn,
    )

    if df_all.empty:
        print("  [WARN] Nenhum dado para calcular métricas.")
        return

    # ── Série temporal (agregação mensal de todos os municípios) ──────────────
    serie = (
        df_all
        .groupby(['periodo', 'mes', 'ano', 'tipo'], as_index=False)
        .agg(
            total        =('valor', 'sum'),
            media        =('valor', 'mean'),
            n_municipios =('municipio', 'nunique'),
        )
        .sort_values(['ano', 'mes'])
    )
    serie.to_sql('serie_temporal', conn, if_exists='replace', index=False)
    print(f"  ✓ serie_temporal: {len(serie)} linhas")

    # ── Ranking de municípios ─────────────────────────────────────────────────
    rank = (
        df_all
        .groupby(['municipio', 'tipo'], as_index=False)
        .agg(
            total        =('valor', 'sum'),
            media_mensal =('valor', 'mean'),
            max_mensal   =('valor', 'max'),
            min_mensal   =('valor', 'min'),
            n_periodos   =('periodo', 'nunique'),
        )
        .sort_values('total', ascending=False)
    )
    rank.to_sql('ranking_municipios', conn, if_exists='replace', index=False)
    print(f"  ✓ ranking_municipios: {len(rank)} linhas")

    # ── Estatísticas descritivas ──────────────────────────────────────────────
    # Por tipo × (todos anos + cada ano separado)
    stats_rows = []

    for tipo in df_all['tipo'].unique():
        df_t = df_all[df_all['tipo'] == tipo]

        # Totais por município no período completo
        tot_mun = df_t.groupby('municipio')['valor'].sum()

        # Geral (todos os anos)
        for metrica, valor in tot_mun.describe().items():
            stats_rows.append({
                'tipo': tipo, 'ano': None,
                'metrica': metrica, 'valor': float(valor),
            })
        stats_rows.append({
            'tipo': tipo, 'ano': None,
            'metrica': 'sum', 'valor': float(tot_mun.sum()),
        })

        # Por ano
        for ano in df_t['ano'].unique():
            df_ta = df_t[df_t['ano'] == ano]
            tot_mun_ano = df_ta.groupby('municipio')['valor'].sum()
            for metrica, valor in tot_mun_ano.describe().items():
                stats_rows.append({
                    'tipo': tipo, 'ano': int(ano),
                    'metrica': metrica, 'valor': float(valor),
                })
            stats_rows.append({
                'tipo': tipo, 'ano': int(ano),
                'metrica': 'sum', 'valor': float(tot_mun_ano.sum()),
            })

    df_stats = pd.DataFrame(stats_rows)
    df_stats.to_sql('stats_descritivas', conn, if_exists='replace', index=False)
    print(f"  ✓ stats_descritivas: {len(df_stats)} linhas")

    conn.commit()


# ── Main ───────────────────────────────────────────────────────────────────────

def carregar():
    baixados = Path("baixados")
    parciais = baixados / "parciais"

    csvs = (
        sorted(baixados.glob("datasus_*.csv")) +
        (sorted(parciais.glob("*.csv")) if parciais.exists() else [])
    )

    if not csvs:
        print("Nenhum CSV encontrado em 'baixados/'.")
        print("Execute primeiro: python datasus_scraper.py")
        sys.exit(1)

    print(f"CSVs encontrados: {[p.name for p in csvs]}")

    conn = sqlite3.connect(DB_PATH)
    criar_banco(conn)
    limpar_banco(conn)

    total_linhas, erros = 0, 0

    for path in csvs:
        origem = "parcial" if "parciais" in str(path) else "consolidado"
        print(f"\n[{origem}] {path.name}  ({path.stat().st_size:,} bytes)")
        try:
            df = processar_csv(path)
            if df.empty:
                print("  → vazio, pulando")
                continue
            df.to_sql('producao_hospitalar', conn, if_exists='append', index=False)
            total_linhas += len(df)
            print(f"  → {len(df):,} registros inseridos")
        except Exception as e:
            print(f"  → ERRO: {e}")
            erros += 1

    # Pré-computar métricas
    if total_linhas > 0:
        calcular_metricas(conn)

    conn.close()

    # ── Resumo final ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Total inserido : {total_linhas:,} registros")
    print(f"Erros          : {erros}")
    print(f"Banco          : {DB_PATH.resolve()}")

    conn2 = sqlite3.connect(DB_PATH)

    print("\n─── producao_hospitalar ───")
    res = conn2.execute("""
        SELECT tipo,
               COUNT(*)                  AS registros,
               COUNT(DISTINCT municipio) AS municipios,
               COUNT(DISTINCT periodo)   AS periodos
        FROM producao_hospitalar
        GROUP BY tipo
    """).fetchall()
    for r in res:
        print(f"  {r[0]:20s} | {r[1]:>10,} registros | "
              f"{r[2]:>5} municípios | {r[3]:>3} períodos")

    print("\n─── serie_temporal ───")
    n = conn2.execute("SELECT COUNT(*) FROM serie_temporal").fetchone()[0]
    print(f"  {n} linhas")

    print("\n─── ranking_municipios ───")
    n = conn2.execute("SELECT COUNT(*) FROM ranking_municipios").fetchone()[0]
    print(f"  {n} linhas")

    print("\n─── stats_descritivas ───")
    n = conn2.execute("SELECT COUNT(*) FROM stats_descritivas").fetchone()[0]
    print(f"  {n} linhas")

    conn2.close()
    print(f"\n{'='*60}")


if __name__ == '__main__':
    carregar()