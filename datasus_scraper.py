"""
DATASUS - SIH/SUS Web Scraper  (2 requisicoes no total)

CORRECAO PRINCIPAL:
  O select #I (Conteudo) tem atributo multiple="".
  O Playwright select_option em selects multiplos NAO deseleciona opcoes
  ja marcadas — ele apenas adiciona. Como "Quantidade aprovada" vem
  selected por padrao, ela continuava ativa nas duas requisicoes.

  Solucao: usar page.evaluate() para manipular o DOM diretamente,
  desmarcando todas as opcoes e marcando apenas a desejada, depois
  disparar o evento 'change' para o formulario reagir.
"""

import asyncio
import re
import io
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

URL         = "https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def"
OUTPUT_DIR  = Path("baixados")
MAX_RETRIES = 3

MESES_ALVO = [
    "Jan/2024","Fev/2024","Mar/2024","Abr/2024","Mai/2024","Jun/2024",
    "Jul/2024","Ago/2024","Set/2024","Out/2024","Nov/2024","Dez/2024",
    "Jan/2025","Fev/2025","Mar/2025","Abr/2025","Mai/2025","Jun/2025",
    "Jul/2025","Ago/2025","Set/2025","Out/2025","Nov/2025","Dez/2025",
    "Jan/2026",
]

# Valores exatos dos <option value="..."> conforme inspecao do HTML real
CONTEUDOS = [
    ("Quantidade_aprovada", "Quantidade aprovada", "datasus_Quantidade_aprovada.csv"),
    ("Valor_aprovado",      "Valor aprovado",      "datasus_Valor_aprovado.csv"),
]

RE_PERIODO = re.compile(
    r"^(\d{4})[/\-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)$",
    re.IGNORECASE,
)


# ---- Pagina ------------------------------------------------------------------

async def load_page(page, max_attempts=5):
    for attempt in range(1, max_attempts + 1):
        print(f"  Carregando pagina (tentativa {attempt}/{max_attempts})...")
        try:
            await page.goto(URL, wait_until="load", timeout=90_000)
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
        await page.wait_for_selector("#L", timeout=30_000)
        return page
    except PlaywrightTimeout:
        pass
    for frame in page.frames:
        try:
            await frame.wait_for_selector("#L", timeout=5_000)
            return frame
        except PlaywrightTimeout:
            continue
    raise RuntimeError("Formulario nao encontrado")


async def select_by_text(ctx, selector, text):
    """Seleciona opcao pelo texto (para selects simples como #L e #C)."""
    sel = ctx.locator(selector)
    for opt in await sel.locator("option").all():
        t = (await opt.inner_text()).strip()
        if text.lower() in t.lower():
            val = await opt.get_attribute("value")
            await sel.select_option(value=val)
            return t
    available = [(await o.inner_text()).strip()
                 for o in await sel.locator("option").all()]
    raise ValueError(f"'{text}' nao encontrado em {selector}. Disponiveis: {available}")


async def set_conteudo_exclusivo(ctx, page, value: str, label: str):
    """
    Seleciona EXCLUSIVAMENTE o value informado no select multiplo #I,
    desmarcando todas as outras opcoes.

    Usa evaluate() para manipular o DOM diretamente e dispara o evento
    'change' para o formulario processar a selecao.
    """
    # Identificar se estamos num frame ou na pagina principal
    # ctx pode ser page ou frame — precisamos do objeto JS correto
    js = """
        (value) => {
            const sel = document.getElementById('I');
            if (!sel) return {ok: false, error: 'elemento #I nao encontrado'};

            // Desmarcar todas
            for (const opt of sel.options) {
                opt.selected = false;
            }

            // Marcar apenas o valor desejado
            let found = false;
            for (const opt of sel.options) {
                if (opt.value === value) {
                    opt.selected = true;
                    found = true;
                }
            }

            if (!found) {
                const vals = Array.from(sel.options).map(o => o.value);
                return {ok: false, error: 'value nao encontrado: ' + value + '. Disponiveis: ' + vals.join(', ')};
            }

            // Disparar evento change para o formulario reagir
            sel.dispatchEvent(new Event('change', {bubbles: true}));

            // Confirmar selecao
            const selected = Array.from(sel.options)
                .filter(o => o.selected)
                .map(o => o.value);
            return {ok: true, selected: selected};
        }
    """

    # Tentar no contexto do frame se ctx nao for a page principal
    try:
        result = await ctx.evaluate(js, value)
    except Exception:
        result = await page.evaluate(js, value)

    if not result.get("ok"):
        raise RuntimeError(f"set_conteudo_exclusivo falhou: {result.get('error')}")

    selected = result.get("selected", [])
    print(f"  OK Conteudo #I -> value='{value}' label='{label}' | selecionados={selected}")

    if value not in selected:
        raise RuntimeError(
            f"Verificacao falhou: '{value}' nao esta em selected={selected}"
        )
    if len(selected) > 1:
        print(f"  WARN Mais de um conteudo selecionado: {selected}. Apenas '{value}' era esperado.")


async def get_period_options(ctx):
    opts = await ctx.locator("#A").locator("option").all()
    return [
        ((await o.inner_text()).strip(), await o.get_attribute("value"))
        for o in opts
        if (await o.inner_text()).strip()
    ]


async def setup_form(page, conteudo_value: str, conteudo_label: str):
    ctx = await wait_for_form(page)

    # Linha: Municipio
    txt = await select_by_text(ctx, "#L", "Municipio")
    print(f"  OK Linha    -> {txt}")

    # Coluna: Ano/mes atendimento
    txt = await select_by_text(ctx, "#C", "Ano/mes atendimento")
    print(f"  OK Coluna   -> {txt}")

    # Pequena pausa para o formulario reagir ao change de Linha/Coluna
    # antes de tentar selecionar Conteudo
    await page.wait_for_timeout(1_000)

    # Conteudo — usando JS direto para select multiple
    await set_conteudo_exclusivo(ctx, page, conteudo_value, conteudo_label)

    # Linhas zeradas
    chk = ctx.locator("#Z")
    if await chk.count() > 0 and not await chk.is_checked():
        await chk.check()
        print("  OK Linhas zeradas marcado")

    # Separador ";"
    sep = ctx.locator('input[type="radio"][name="formato"][value="prn"]')
    if await sep.count() > 0:
        await sep.first.check()
        print('  OK Separador ";" selecionado')
    else:
        print("  WARN Separador nao encontrado")

    return ctx


async def select_all_periods(ctx, periodo_options):
    values, skipped = [], []
    for mes in MESES_ALVO:
        match = [v for t, v in periodo_options if mes.lower() in t.lower()]
        if match:
            values.append(match[0])
        else:
            skipped.append(mes)
    if skipped:
        print(f"  SKIP Periodos nao encontrados: {skipped}")
    await ctx.locator("#A").select_option(value=values)
    print(f"  OK {len(values)} periodos selecionados")
    return len(values)


# ---- Resultado ---------------------------------------------------------------

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

    await result.wait_for_load_state("domcontentloaded", timeout=120_000)
    return result


async def extract_pre(result_page) -> str:
    pre = result_page.locator("pre")
    await pre.wait_for(state="visible", timeout=300_000)
    await result_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await result_page.wait_for_timeout(3_000)
    text = await pre.inner_text()
    print(f"  OK Texto bruto: {len(text)} caracteres")
    return text


def _to_float_raw(v) -> float:
    s = str(v).strip()
    if s in ("", "-", "nd", "ND", "nan"):
        return 0.0
    try:
        return float(s.replace(".", "").replace(",", "."))
    except Exception:
        return 0.0


def parse_csv(raw: str) -> pd.DataFrame:
    lines = []
    for line in raw.strip().split("\n"):
        s = line.strip()
        if not s:
            continue
        if re.match(r"^(Fonte|Nota)[:\s]", s, re.IGNORECASE):
            break
        lines.append(s)

    if len(lines) < 2:
        print(f"  WARN Apenas {len(lines)} linhas validas")
        return pd.DataFrame()

    sep = ";" if lines[0].count(";") >= lines[0].count(",") else ","
    print(f"  Separador: '{sep}' | Linhas: {len(lines)}")
    print(f"  Cabecalho (120 chars): {lines[0][:120]}")

    try:
        df = pd.read_csv(
            io.StringIO("\n".join(lines)),
            sep=sep, dtype=str, on_bad_lines="skip", encoding="utf-8",
        )
    except Exception as e:
        print(f"  WARN parse erro: {e}")
        return pd.DataFrame()

    print(f"  Shape bruto: {df.shape}")
    col0 = df.columns[0]
    df = df[~df[col0].fillna("").str.strip().str.lower()
              .isin(["total", "nan", "", "total geral"])]
    df = df.drop(
        columns=[c for c in df.columns if c.strip().lower() == "total"],
        errors="ignore",
    )
    print(f"  Shape final: {df.shape}")
    return df


def validar_sanidade(df: pd.DataFrame, conteudo_label: str):
    cols_2024 = [
        c for c in df.columns
        if RE_PERIODO.match(c.strip()) and int(c[:4]) >= 2024
    ]
    if not cols_2024:
        print("  WARN Nenhuma coluna 2024+ para validacao")
        return

    vals_pos = [
        _to_float_raw(v)
        for c in cols_2024
        for v in df[c].values
        if _to_float_raw(v) > 0
    ]
    soma  = sum(vals_pos)
    media = soma / max(1, len(df) * len(cols_2024))
    print(f"  Sanidade: {len(cols_2024)} colunas 2024+ | obs>0={len(vals_pos)} | "
          f"soma={soma:,.0f} | media/mun/mes={media:,.1f}")

    if "valor" in conteudo_label.lower() and media < 500:
        print(f"  !! ALERTA: media {media:.1f} muito baixa para Valor aprovado.")
        print(f"  !! Esperado > 500 R$/mun/mes. Verifique se #I foi selecionado corretamente.")
    elif "quantidade" in conteudo_label.lower() and media > 50_000:
        print(f"  !! ALERTA: media {media:.1f} muito alta para Quantidade.")


# ---- Main --------------------------------------------------------------------

async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        print("=" * 65)
        print("DATASUS - SIH/SUS Scraper  (2 requisicoes no total)")
        print("=" * 65)

        results = {}

        for conteudo_value, conteudo_label, filename in CONTEUDOS:
            print(f"\n{'-'*65}")
            print(f"Conteudo: {conteudo_label} (value='{conteudo_value}')  ->  {filename}")
            print(f"{'-'*65}")

            df_final = pd.DataFrame()

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await load_page(page)
                    ctx = await setup_form(page, conteudo_value, conteudo_label)

                    periodo_options = await get_period_options(ctx)
                    print(f"  OK {len(periodo_options)} periodos disponiveis")

                    n_sel = await select_all_periods(ctx, periodo_options)
                    if n_sel == 0:
                        raise RuntimeError("Nenhum periodo selecionado.")

                    print("  Clicando Mostra (pode demorar minutos)...", flush=True)
                    result_page = await click_mostra(page, ctx)

                    print("  Extraindo dados...", flush=True)
                    raw = await extract_pre(result_page)
                    await result_page.close()

                    if not raw.strip():
                        print("  WARN Resposta vazia, tentando novamente...")
                        continue

                    df_final = parse_csv(raw)
                    if df_final.empty:
                        print("  WARN DataFrame vazio, tentando novamente...")
                        continue

                    validar_sanidade(df_final, conteudo_label)
                    break

                except Exception as e:
                    print(f"  ERRO tentativa {attempt}/{MAX_RETRIES}: {e}")
                    for p2 in page.context.pages[1:]:
                        await p2.close()
                    await page.wait_for_timeout(5_000)

            if df_final.empty:
                print(f"  WARN Nenhum dado para '{conteudo_label}'")
                continue

            out = OUTPUT_DIR / filename
            df_final.to_csv(out, index=False, sep=";", encoding="utf-8-sig")
            results[conteudo_label] = df_final
            print(f"  OK Salvo: {out}  ({out.stat().st_size:,} bytes)")

        # Validacao cruzada
        if len(results) == 2:
            dfs   = list(results.values())
            nomes = list(results.keys())
            cols_test = [
                c for c in dfs[0].columns
                if RE_PERIODO.match(c.strip()) and int(c[:4]) >= 2024
            ]
            if cols_test and len(dfs[0]) > 1 and len(dfs[1]) > 1:
                c  = cols_test[0]
                v0 = dfs[0][c].iloc[1] if c in dfs[0].columns else "N/A"
                v1 = dfs[1][c].iloc[1] if c in dfs[1].columns else "N/A"
                print(f"\nValidacao cruzada -- coluna {c}, linha 2:")
                print(f"  {nomes[0]}: {v0}")
                print(f"  {nomes[1]}: {v1}")
                if str(v0) == str(v1):
                    print("  !! ATENCAO: valores ainda identicos!")
                    print("  !! Inspecione o formulario manualmente para confirmar")
                    print("  !! o value exato das opcoes de #I.")
                else:
                    print("  OK Valores distintos -- conteudos diferentes confirmados.")

        await browser.close()

    print("\n" + "=" * 65)
    print("Extracao concluida!")
    print("Proximo passo: python datasus_loader.py")
    print("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())