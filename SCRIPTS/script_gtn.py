from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
import pandas as pd
import subprocess
import os
import time
import traceback

load_dotenv()

GTN_URL = os.getenv("GTN_URL", "https://gtn.ninecon.com.br/ords/r/gtn/gtn/login")
GTN_USER = os.getenv("GTN_USER")
GTN_PASS = os.getenv("GTN_PASS")

INTERVALO_MINUTOS = 60

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DOWNLOAD_DIR = BASE_DIR / "downloads"
DASHBOARD_DIR = BASE_DIR / "dashboard_data"
LOCK_FILE = BASE_DIR / "rodando.lock"
LOG_FILE = BASE_DIR / "execucao.log"

OUTPUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_DIR.mkdir(exist_ok=True)
DASHBOARD_DIR.mkdir(exist_ok=True)


def log(mensagem: str):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{agora}] {mensagem}"
    print(linha)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


def validar_env():
    faltando = []
    if not GTN_USER:
        faltando.append("GTN_USER")
    if not GTN_PASS:
        faltando.append("GTN_PASS")

    if faltando:
        raise ValueError(f"Variáveis ausentes no .env: {', '.join(faltando)}")


def salvar_debug(page, nome_base="debug"):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    screenshot_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.png"
    html_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.html"

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        html_path.write_text(page.content(), encoding="utf-8")
        log(f"📸 Screenshot salvo em: {screenshot_path}")
        log(f"📝 HTML salvo em: {html_path}")
    except Exception as e:
        log(f"⚠️ Não consegui salvar debug: {e}")


def fazer_login(page):
    log("🌐 Abrindo login...")
    page.goto(GTN_URL, wait_until="domcontentloaded", timeout=60000)

    log("🔐 Preenchendo credenciais...")
    page.get_by_role("textbox", name="Usuário").fill(GTN_USER)
    page.get_by_role("textbox", name="Senha").fill(GTN_PASS)

    log("➡️ Clicando em Acessar...")
    page.get_by_role("button", name="Acessar").click()

    page.wait_for_load_state("networkidle", timeout=30000)
    log(f"✅ Login concluído. URL atual: {page.url}")


def abrir_execucao_testes(page):
    log("📂 Abrindo grupo do programa...")
    page.get_by_label("Exibição em Grade").get_by_role(
        "link",
        name="GRUPO PLUMA - PROGRAMA CONECTA"
    ).click()

    page.wait_for_load_state("networkidle", timeout=20000)

    log("🧭 Abrindo navegação principal...")
    page.get_by_role("button", name="Navegação Principal").click()

    log("🌲 Expandindo menu...")
    page.locator(".a-TreeView-toggle").first.click()

    log("🧪 Entrando em Execução de Testes...")
    page.get_by_role("treeitem", name="Execução de Testes").click()

    page.wait_for_load_state("networkidle", timeout=30000)
    log(f"✅ Tela de execução aberta: {page.url}")


def exportar_csv(page) -> Path:
    log("⚙️ Abrindo opções do relatório...")
    page.get_by_role("button", name="Editar Relatório").click()
    page.get_by_role("button", name="Aplicar").click()

    log("📤 Abrindo menu Ações...")
    page.get_by_role("button", name="Ações").click()
    page.get_by_role("menuitem", name="Fazer Download").click()
    page.get_by_role("option", name="CSV").click()

    log("⬇️ Baixando CSV...")
    with page.expect_download(timeout=60000) as download_info:
        page.get_by_role("button", name="Fazer Download").click()

    download = download_info.value

    erro_download = download.failure()
    if erro_download:
        raise RuntimeError(f"Falha no download: {erro_download}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_original = download.suggested_filename or "Cenarios_Consolidados.csv"
    nome_limpo = nome_original.replace(" ", "_")
    destino = DOWNLOAD_DIR / f"{timestamp}_{nome_limpo}"

    download.save_as(str(destino))
    log(f"✅ CSV salvo em: {destino}")
    return destino


def atualizar_csv_dashboard(arquivo_entrada: Path) -> Path:
    log("🧹 Tratando arquivo para dashboard...")

    tentativas = [
        {"sep": ";", "encoding": "latin1"},
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "latin1"},
    ]

    ultimo_erro = None
    df = None

    for tentativa in tentativas:
        try:
            df = pd.read_csv(
                arquivo_entrada,
                sep=tentativa["sep"],
                encoding=tentativa["encoding"]
            )
            log(
                f"✅ Leitura realizada com sep='{tentativa['sep']}' "
                f"e encoding='{tentativa['encoding']}'"
            )
            break
        except Exception as e:
            ultimo_erro = e

    if df is None:
        raise RuntimeError(f"Não consegui ler o CSV baixado. Último erro: {ultimo_erro}")

    gerado_em = datetime.now().strftime("%Y-%m-%d %H:%M")
    df["Gerado em"] = gerado_em

    arquivo_saida = DASHBOARD_DIR / "Cenarios_Consolidados_atualizado.csv"
    df.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8-sig")

    log(f"✅ Arquivo final gerado: {arquivo_saida}")
    log(f"🕒 Coluna 'Gerado em' preenchida com: {gerado_em}")

    return arquivo_saida


def rodar_git(args, cwd: Path):
    resultado = subprocess.run(
        ["git"] + args,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace"
    )

    if resultado.returncode != 0:
        raise RuntimeError(
            f"Erro no git {' '.join(args)}\n"
            f"STDOUT:\n{resultado.stdout}\n"
            f"STDERR:\n{resultado.stderr}"
        )

    return resultado.stdout.strip()


def commitar_e_enviar_arquivo(repo_dir: Path, arquivo: Path):
    rel_path = arquivo.relative_to(repo_dir)

    log(f"📌 Adicionando arquivo ao git: {rel_path}")
    rodar_git(["add", str(rel_path)], repo_dir)

    status = rodar_git(["status", "--porcelain", str(rel_path)], repo_dir)
    if not status.strip():
        log("ℹ️ Nenhuma alteração detectada. Nada para commitar.")
        return

    mensagem = f"Atualiza dashboard GTN em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    log(f"📝 Criando commit: {mensagem}")
    rodar_git(["commit", "-m", mensagem], repo_dir)

    log("🚀 Enviando para o GitHub...")
    rodar_git(["push"], repo_dir)

    log("✅ Commit e push realizados com sucesso.")


def executar_fluxo():
    validar_env()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            fazer_login(page)
            abrir_execucao_testes(page)
            arquivo_csv_baixado = exportar_csv(page)
            arquivo_dashboard = atualizar_csv_dashboard(arquivo_csv_baixado)

            salvar_debug(page, "final_sucesso")
            commitar_e_enviar_arquivo(BASE_DIR, arquivo_dashboard)

            log("🎯 Processo concluído com sucesso.")
            log(f"📥 Arquivo bruto: {arquivo_csv_baixado}")
            log(f"📊 Arquivo dashboard: {arquivo_dashboard}")

        except PlaywrightTimeoutError as e:
            log(f"⏰ Timeout: {e}")
            salvar_debug(page, "timeout")
            raise

        except Exception as e:
            log(f"❌ Erro: {e}")
            salvar_debug(page, "erro")
            raise

        finally:
            context.close()
            browser.close()


def executar_com_controle():
    if LOCK_FILE.exists():
        log("⚠️ Já existe uma execução em andamento. Pulando esta rodada.")
        return

    try:
        LOCK_FILE.touch()

        log("==================================================")
        log("🚀 Iniciando nova execução")
        log("==================================================")

        executar_fluxo()

        log("✅ Execução encerrada com sucesso.")

    except Exception as e:
        log(f"❌ Falha geral na execução: {e}")
        log(traceback.format_exc())

    finally:
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except Exception as e:
            log(f"⚠️ Não consegui remover lock file: {e}")


def loop_principal():
    log(f"🔁 Script configurado para rodar a cada {INTERVALO_MINUTOS} minutos.")

    while True:
        inicio = time.time()

        executar_com_controle()

        duracao = time.time() - inicio
        espera = max(0, INTERVALO_MINUTOS * 60 - duracao)

        log(f"⏳ Aguardando {int(espera)} segundos para a próxima execução...\n")
        time.sleep(espera)


if __name__ == "__main__":
    loop_principal()