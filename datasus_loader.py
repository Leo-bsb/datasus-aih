"""
DATASUS - Carregador de Banco de Dados

Suporta 2 formatos:
  A) Wide-período: Município | Jan/2024 | Fev/2024 | ...   (novo scraper, 2 reqs)
  B) Wide-subgrupo: Município | 0201 Coleta | 0202 Diag... (scraper antigo, parciais)
"""

import sqlite3
import re
import sys
import pandas as pd
from pathlib import Path

DB_PATH = Path("datasus.db")

MESES_PT = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,
            'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}

# DATASUS gera colunas no formato AAAA/Mmm  ex: 2024/Jan  2025/Dez
RE_PERIODO = re.compile(
    r'^(\d{4})[/\-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)$',
    re.IGNORECASE)

def is_periodo_col(col: str) -> bool:
    return bool(RE_PERIODO.match(col.strip()))

def parse_periodo(texto):
    """Retorna (mes, ano) de strings como '2024/Jan' ou 'Jan/2024'."""
    s = str(texto).strip()
    # Formato DATASUS: AAAA/Mmm
    m = RE_PERIODO.match(s)
    if m:
        return MESES_PT.get(m.group(2).lower(), 0), int(m.group(1))
    # Formato alternativo: Mmm/AAAA
    m2 = re.match(r'^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[/\-_](\d{4})$',
                  s, re.IGNORECASE)
    if m2:
        return MESES_PT.get(m2.group(1).lower(), 0), int(m2.group(2))
    return None, None

def to_float(v):
    s = str(v).strip() if pd.notna(v) else ''
    if s in ('', '-', 'nd', 'ND'):
        return 0.0
    try:
        return float(s.replace('.', '').replace(',', '.'))
    except ValueError:
        return None

def inferir_tipo(path: Path) -> str:
    nome = path.stem.lower()
    if 'valor' in nome:
        return 'Val_aprovado'
    return 'Qtd_aprovada'

def inferir_periodo_do_nome(path: Path):
    """Extrai 'Jan/2025' de Quantidade_aprovada_Jan_2025.csv"""
    m = re.search(r'_([A-Za-z]{3})_(\d{4})', path.stem)
    if m:
        return f"{m.group(1).capitalize()}/{m.group(2)}"
    return None

def criar_banco(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS producao_hospitalar (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            municipio             TEXT NOT NULL,
            subgrupo_procedimento TEXT NOT NULL,
            periodo               TEXT,
            mes                   INTEGER,
            ano                   INTEGER,
            tipo                  TEXT NOT NULL,
            valor                 REAL
        );
        CREATE INDEX IF NOT EXISTS idx_mun  ON producao_hospitalar(municipio);
        CREATE INDEX IF NOT EXISTS idx_sg   ON producao_hospitalar(subgrupo_procedimento);
        CREATE INDEX IF NOT EXISTS idx_ano  ON producao_hospitalar(ano);
        CREATE INDEX IF NOT EXISTS idx_tipo ON producao_hospitalar(tipo);
        CREATE INDEX IF NOT EXISTS idx_per  ON producao_hospitalar(periodo);
    """)
    conn.commit()


def processar_csv(path: Path) -> pd.DataFrame:
    # Detectar separador real lendo a primeira linha
    raw_line = None
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                raw_line = f.readline()
            break
        except Exception:
            continue

    sep = ';' if raw_line and raw_line.count(';') >= raw_line.count(',') else ','

    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc,
                             on_bad_lines='skip')
            break
        except Exception:
            continue
    else:
        raise ValueError(f"Nao foi possivel ler {path}")

    df.columns = [c.strip() for c in df.columns]
    col0 = df.columns[0]  # sempre Município

    # Debug: mostrar primeiras colunas para diagnóstico
    outras = [c for c in df.columns if c != col0]
    print(f"\n    sep={sep!r} | {len(df.columns)} colunas | "
          f"ex: {outras[:3]}")

    # Remover rodapé
    df = df[~df[col0].fillna('').str.strip().str.lower()
              .isin(['total', 'nan', '', 'total geral'])]
    df = df.drop(columns=[c for c in df.columns
                           if c.strip().lower() == 'total'], errors='ignore')

    outras_cols = [c for c in df.columns if c != col0]
    tipo = inferir_tipo(path)

    # ── Detectar formato ──────────────────────────────────────────────────────
    periodo_cols  = [c for c in outras_cols if is_periodo_col(c)]
    subgrupo_cols = [c for c in outras_cols if not is_periodo_col(c)]

    if periodo_cols:
        # Formato A: Município × períodos  (novo scraper)
        # Filtrar apenas os períodos alvo (2024/Jan a 2026/Jan)
        ANOS_ALVO = {2024, 2025, 2026}
        periodo_cols_alvo = [
            c for c in periodo_cols
            if parse_periodo(c)[1] in ANOS_ALVO
        ]
        print(f"    formato: wide-período ({len(periodo_cols)} total, "
              f"{len(periodo_cols_alvo)} no intervalo alvo)")
        df_long = df.melt(id_vars=[col0], value_vars=periodo_cols_alvo,
                          var_name='periodo', value_name='valor')
        df_long = df_long.rename(columns={col0: 'municipio'})
        df_long['subgrupo_procedimento'] = 'Todos os subgrupos'

    else:
        # Formato B: Município × subgrupos  (scraper antigo / parciais)
        periodo_label = inferir_periodo_do_nome(path)
        print(f"    formato: wide-subgrupo ({len(subgrupo_cols)} subgrupos, "
              f"período={periodo_label})")
        df_long = df.melt(id_vars=[col0], value_vars=subgrupo_cols,
                          var_name='subgrupo_procedimento', value_name='valor')
        df_long = df_long.rename(columns={col0: 'municipio'})
        df_long['periodo'] = periodo_label

    # ── Finalizar ─────────────────────────────────────────────────────────────
    df_long['tipo']  = tipo
    df_long['valor'] = df_long['valor'].apply(to_float)

    parsed = df_long['periodo'].apply(lambda x: pd.Series(parse_periodo(x)))
    df_long['mes'] = parsed[0].astype('Int64')
    df_long['ano'] = parsed[1].astype('Int64')

    # Normalizar periodo para formato legível: Jan/2024
    MESES_NOME = {1:'Jan',2:'Fev',3:'Mar',4:'Abr',5:'Mai',6:'Jun',
                  7:'Jul',8:'Ago',9:'Set',10:'Out',11:'Nov',12:'Dez'}
    df_long['periodo'] = df_long.apply(
        lambda r: f"{MESES_NOME.get(r['mes'], '?')}/{r['ano']}"
                  if pd.notna(r['mes']) and pd.notna(r['ano']) else r['periodo'],
        axis=1
    )

    df_long = df_long[df_long['valor'].notna() & df_long['mes'].notna() & (df_long['valor'] > 0)]

    # Manter apenas o intervalo de interesse: 2024 a 2026
    df_long = df_long[df_long['ano'].isin([2024, 2025, 2026])]

    cols = ['municipio','subgrupo_procedimento','periodo','mes','ano','tipo','valor']
    return df_long[[c for c in cols if c in df_long.columns]]


def carregar():
    baixados = Path("baixados")
    parciais = baixados / "parciais"

    csvs = (sorted(baixados.glob("datasus_*.csv")) +
            (sorted(parciais.glob("*.csv")) if parciais.exists() else []))

    if not csvs:
        print("Nenhum CSV encontrado.")
        print("Execute primeiro: python datasus_scraper.py")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    criar_banco(conn)
    conn.execute("DELETE FROM producao_hospitalar")
    conn.commit()

    total, erros = 0, 0

    for path in csvs:
        origem = "parcial" if "parciais" in str(path) else "consolidado"
        print(f"[{origem}] {path.name} ...", end=' ', flush=True)
        try:
            df = processar_csv(path)
            if df.empty:
                print("vazio")
                continue
            df.to_sql('producao_hospitalar', conn, if_exists='append', index=False)
            total += len(df)
            print(f"{len(df):,} linhas")
        except Exception as e:
            print(f"ERRO: {e}")
            erros += 1

    conn.close()

    print(f"\n{'='*55}")
    print(f"Total inserido : {total:,} registros")
    print(f"Erros          : {erros}")
    print(f"Banco          : {DB_PATH.resolve()}")

    conn2 = sqlite3.connect(DB_PATH)
    res = conn2.execute("""
        SELECT tipo,
               COUNT(*)                  AS registros,
               COUNT(DISTINCT municipio) AS municipios,
               COUNT(DISTINCT periodo)   AS periodos
        FROM producao_hospitalar GROUP BY tipo
    """).fetchall()
    conn2.close()
    if res:
        print("\nResumo:")
        for r in res:
            print(f"  {r[0]}: {r[1]:,} registros | "
                  f"{r[2]} municípios | {r[3]} períodos")


if __name__ == '__main__':
    carregar()