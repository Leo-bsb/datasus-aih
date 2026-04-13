"""
Gera dados de amostra para testar a aplicação Streamlit sem precisar do scraper.
"""
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

DB_PATH = Path("datasus.db")

MUNICIPIOS = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador", "Fortaleza",
    "Manaus", "Curitiba", "Recife", "Porto Alegre", "Belém",
    "Goiânia", "Guarulhos", "Campinas", "São Luís", "Maceió",
    "Natal", "Teresina", "Campo Grande", "João Pessoa", "Aracaju",
]

SUBGRUPOS = [
    "Procedimentos clínicos", "Procedimentos cirúrgicos",
    "Transplantes de órgãos, tecidos e células",
    "Ações complementares da atenção à saúde",
    "Diagnose", "Terapias especializadas",
    "Parto e nascimento", "Internação",
    "Atenção à saúde do idoso", "Oncologia",
]

MESES = [
    ("Jan/2024", 1, 2024), ("Fev/2024", 2, 2024), ("Mar/2024", 3, 2024),
    ("Abr/2024", 4, 2024), ("Mai/2024", 5, 2024), ("Jun/2024", 6, 2024),
    ("Jul/2024", 7, 2024), ("Ago/2024", 8, 2024), ("Set/2024", 9, 2024),
    ("Out/2024", 10, 2024), ("Nov/2024", 11, 2024), ("Dez/2024", 12, 2024),
    ("Jan/2025", 1, 2025), ("Fev/2025", 2, 2025), ("Mar/2025", 3, 2025),
    ("Abr/2025", 4, 2025), ("Mai/2025", 5, 2025), ("Jun/2025", 6, 2025),
    ("Jul/2025", 7, 2025), ("Ago/2025", 8, 2025), ("Set/2025", 9, 2025),
    ("Out/2025", 10, 2025), ("Nov/2025", 11, 2025), ("Dez/2025", 12, 2025),
    ("Jan/2026", 1, 2026),
]

np.random.seed(42)
rows = []
for mun in MUNICIPIOS:
    size_factor = np.random.uniform(0.3, 3.0)
    for sg in SUBGRUPOS:
        for periodo, mes, ano in MESES:
            qtd = int(np.random.poisson(500 * size_factor) * np.random.uniform(0.8, 1.2))
            val = qtd * np.random.uniform(800, 3500)
            rows.append({
                'municipio': mun,
                'subgrupo_procedimento': sg,
                'periodo': periodo,
                'mes': mes,
                'ano': ano,
                'tipo': 'Qtd_aprovada',
                'valor': float(qtd),
            })
            rows.append({
                'municipio': mun,
                'subgrupo_procedimento': sg,
                'periodo': periodo,
                'mes': mes,
                'ano': ano,
                'tipo': 'Val_aprovado',
                'valor': round(val, 2),
            })

df = pd.DataFrame(rows)

conn = sqlite3.connect(DB_PATH)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS producao_hospitalar (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        municipio       TEXT NOT NULL,
        subgrupo_procedimento TEXT NOT NULL,
        periodo         TEXT,
        mes             INTEGER,
        ano             INTEGER,
        tipo            TEXT NOT NULL,
        valor           REAL
    );
    CREATE INDEX IF NOT EXISTS idx_mun  ON producao_hospitalar(municipio);
    CREATE INDEX IF NOT EXISTS idx_sg   ON producao_hospitalar(subgrupo_procedimento);
    CREATE INDEX IF NOT EXISTS idx_ano  ON producao_hospitalar(ano);
    CREATE INDEX IF NOT EXISTS idx_tipo ON producao_hospitalar(tipo);
""")
conn.execute("DELETE FROM producao_hospitalar")
df.to_sql('producao_hospitalar', conn, if_exists='append', index=False)
conn.commit()
conn.close()

print(f"✓ Banco de dados criado com {len(df):,} registros de amostra.")
print(f"  Arquivo: {DB_PATH.resolve()}")
