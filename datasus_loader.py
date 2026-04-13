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

RE_PERIODO = re.compile(r'^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[/\-_](\d{4})$',
                        re.IGNORECASE)

def is_periodo_col(col: str) -> bool:
    """Retorna True se o nome da coluna parece um período (Jan/2024, Fev/2025...)."""
    return bool(RE_PERIODO.match(col.strip()))

def parse_periodo(texto):
    m = RE_PERIODO.match(str(texto).strip())
    if m:
        return MESES_PT.get(m.group(1).lower(), 0), int(m.group(2))
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
    try:
        df = pd.read_csv(path, sep=';', dtype=str, encoding='utf-8-sig',
                         on_bad_lines='skip')
    except Exception:
        df = pd.read_csv(path, sep=';', dtype=str, encoding='latin-1',
                         on_bad_lines='skip')

    df.columns = [c.strip() for c in df.columns]
    col0 = df.columns[0]  # sempre Município

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
        # Cada coluna é um período (Jan/2024, Fev/2024...) — sem subgrupo
        # O subgrupo_procedimento fica como 'Todos' pois não está discriminado
        print(f"    formato: wide-período ({len(periodo_cols)} períodos)")
        df_long = df.melt(id_vars=[col0], value_vars=periodo_cols,
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

    df_long = df_long[df_long['valor'].notna() & df_long['mes'].notna()]

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