"""
DATASUS - SIH/SUS Web Scraper  (2 requisições no total)

Linha   = Município
Coluna  = Ano/mês atendimento  → cada período vira uma coluna
Períodos = todos de uma vez
Conteúdo = Quantidade aprovada | Valor aprovado

Resultado: Município × [Jan/2024, Fev/2024, ..., Jan/2026]
O loader faz o melt para normalizar.
"""

import asyncio
import re
import io
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

URL        = "https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def"
OUTPUT_DIR = Path("baixados")
MAX_RETRIES = 3

MESES_ALVO = [
    "Jan/2024","Fev/2024","Mar/2024","Abr/2024","Mai/2024","Jun/2024",
    "Jul/2024","Ago/2024","Set/2024","Out/2024","Nov/2024","Dez/2024",
    "Jan/2025","Fev/2025","Mar/2025","Abr/2025","Mai/2025","Jun/2025",
    "Jul/2025","Ago/2025","Set/2025","Out/2025","Nov/2025","Dez/2025",
    "Jan/2026",
]

CONTEUDOS = ["Quantidade aprovada", "Valor aprovado"]


# ── Página ─────────────────────────────────────────────────────────────────────

async def load_page(page, max_attempts=5):
    for attempt in range(1, max_attempts + 1):
        print(f"  Carregando pagina (tentativa {attempt}/{max_attempts})...")
        try:
            await page.goto(URL, wait_until='load', timeout=90_000)
            return
        except Exception as e:
            print(f"  [WARN] {e.__class__.__name__}")
            if attempt < max_attempts:
                w = attempt * 5
                print(f"  Aguardando {w}s...")
                await page.wait_for_timeout(w * 1_000)
            else:
                raise RuntimeError("Nao foi possivel carregar o DATASUS.")


async def wait_for_form(page):
    try:
        await page.wait_for_selector('#L', timeout=30_000)
        return page
    except PlaywrightTimeout:
        pass
    for frame in page.frames:
        try:
            await frame.wait_for_selector('#L', timeout=5_000)
            return frame
        except PlaywrightTimeout:
            continue
    raise RuntimeError("Formulario nao encontrado")


async def select_by_text(ctx, selector, text):
    sel = ctx.locator(selector)
    for opt in await sel.locator('option').all():
        t = (await opt.inner_text()).strip()
        if text.lower() in t.lower():
            await sel.select_option(value=await opt.get_attribute('value'))
            return t
    available = [(await o.inner_text()).strip()
                 for o in await sel.locator('option').all()]
    raise ValueError(f"'{text}' nao encontrado. Disponiveis: {available}")


async def get_period_options(ctx):
    opts = await ctx.locator('#A').locator('option').all()
    return [((await o.inner_text()).strip(), await o.get_attribute('value'))
            for o in opts if (await o.inner_text()).strip()]


async def setup_form(page, conteudo: str):
    ctx = await wait_for_form(page)

    # Linha: Município
    txt = await select_by_text(ctx, '#L', 'Município')
    print(f"  v Linha    -> {txt}")

    # Coluna: Ano/mês atendimento  ← todos os períodos viram colunas
    txt = await select_by_text(ctx, '#C', 'Ano/mês atendimento')
    print(f"  v Coluna   -> {txt}")

    # Conteúdo
    txt = await select_by_text(ctx, '#I', conteudo)
    print(f"  v Conteúdo -> {txt}")

    # Linhas zeradas
    chk = ctx.locator('#Z')
    if await chk.count() > 0 and not await chk.is_checked():
        await chk.check()
        print("  v Linhas zeradas")

    # Separador ";" → name=formato value=prn
    sep = ctx.locator('input[type="radio"][name="formato"][value="prn"]')
    if await sep.count() > 0:
        await sep.first.check()
        print('  v Separador ";"')
    else:
        print("  [WARN] Separador nao encontrado")

    return ctx


async def select_all_periods(ctx, periodo_options):
    """Seleciona todos os meses alvo de uma vez no <select multiple>."""
    values, skipped = [], []
    for mes in MESES_ALVO:
        match = [v for t, v in periodo_options if mes.lower() in t.lower()]
        if match:
            values.append(match[0])
        else:
            skipped.append(mes)

    if skipped:
        print(f"  [SKIP] Não encontrados: {skipped}")

    await ctx.locator('#A').select_option(value=values)
    print(f"  v {len(values)} períodos selecionados de uma vez")


# ── Resultado ──────────────────────────────────────────────────────────────────

async def click_mostra(page, ctx):
    btn = ctx.locator('input[name="mostre"][type="submit"]')
    if await btn.count() == 0:
        btn = ctx.locator('input[value="Mostra"]')
    if await btn.count() == 0:
        btn = ctx.locator('input[type="submit"]').first

    try:
        async with page.expect_popup(timeout=120_000) as info:
            await btn.click()
        result = await info.value
    except Exception:
        async with page.context.expect_page(timeout=120_000) as info:
            await btn.click()
        result = await info.value

    await result.wait_for_load_state('domcontentloaded', timeout=120_000)
    return result


async def extract_pre(result_page) -> str:
    pre = result_page.locator('pre')
    await pre.wait_for(state='visible', timeout=300_000)
    await result_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await result_page.wait_for_timeout(2_000)
    return await pre.inner_text()


def parse_csv(raw: str) -> pd.DataFrame:
    lines = []
    for line in raw.strip().split('\n'):
        s = line.strip()
        if not s:
            continue
        if re.match(r'^(Fonte|Nota)[:\s]', s, re.IGNORECASE):
            break
        lines.append(s)

    if len(lines) < 2:
        return pd.DataFrame()

    sep = ';' if lines[0].count(';') >= lines[0].count(',') else ','
    try:
        df = pd.read_csv(io.StringIO('\n'.join(lines)), sep=sep,
                         dtype=str, on_bad_lines='skip')
    except Exception as e:
        print(f"  [WARN] parse: {e}")
        return pd.DataFrame()

    col0 = df.columns[0]
    df = df[~df[col0].fillna('').str.strip().str.lower()
              .isin(['total', 'nan', '', 'total geral'])]
    df = df.drop(columns=[c for c in df.columns
                           if c.strip().lower() == 'total'], errors='ignore')
    return df


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),
        )
        page = await context.new_page()

        print("=" * 60)
        print("DATASUS - SIH/SUS Scraper  (2 requisições no total)")
        print("=" * 60)

        for conteudo in CONTEUDOS:
            print(f"\n{'─'*60}")
            print(f"Conteúdo: {conteudo}")
            print(f"{'─'*60}")

            df_final = pd.DataFrame()

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await load_page(page)
                    ctx = await setup_form(page, conteudo)

                    periodo_options = await get_period_options(ctx)
                    print(f"  v {len(periodo_options)} períodos disponíveis "
                          f"({periodo_options[0][0]} ... {periodo_options[-1][0]})")

                    await select_all_periods(ctx, periodo_options)

                    print("  Clicando Mostra (pode demorar alguns minutos)...",
                          flush=True)
                    result_page = await click_mostra(page, ctx)

                    print("  Extraindo dados...", flush=True)
                    raw = await extract_pre(result_page)
                    await result_page.close()

                    df_final = parse_csv(raw)

                    if df_final.empty:
                        print("  [WARN] DataFrame vazio, tentando novamente...")
                        continue

                    print(f"  v {df_final.shape[0]} municípios × "
                          f"{df_final.shape[1]-1} colunas de período")
                    break

                except Exception as e:
                    print(f"  [ERRO] tentativa {attempt}/{MAX_RETRIES}: {e}")
                    for p2 in page.context.pages[1:]:
                        await p2.close()
                    await page.wait_for_timeout(4_000)

            if df_final.empty:
                print(f"  [WARN] Nenhum dado para '{conteudo}'")
                continue

            safe = re.sub(r'[^\w]', '_', conteudo).strip('_')
            out  = OUTPUT_DIR / f"datasus_{safe}.csv"
            df_final.to_csv(out, index=False, sep=';', encoding='utf-8-sig')
            print(f"  v Salvo: {out}")

        await browser.close()

    print("\n" + "=" * 60)
    print("Extração concluída!")
    print("Próximo passo: python datasus_loader.py")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())