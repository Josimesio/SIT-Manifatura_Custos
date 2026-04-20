from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
import pandas as pd
import subprocess
import os
import time
import traceback
import smtplib
from email.message import EmailMessage

load_dotenv()

GTN_URL = os.getenv("GTN_URL", "https://gtn.ninecon.com.br/ords/r/gtn/gtn/login")
GTN_HOME_URL = os.getenv("GTN_HOME_URL", "https://gtn.ninecon.com.br/ords/r/gtn/gtn/home")
GTN_USER = os.getenv("GTN_USER")
GTN_PASS = os.getenv("GTN_PASS")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "sim"}
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", SMTP_USER or "")

LIDERES_FILTRO = os.getenv(
    "GTN_LIDERES",
    "Wilson Alves,Walceir Hernandes,Camila Lorena Maciel"
)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DOWNLOAD_DIR = BASE_DIR / "downloads"
DASHBOARD_DIR = BASE_DIR / "dashboard_data"
LOCK_FILE = BASE_DIR / "rodando.lock"
LOG_FILE = BASE_DIR / "execucao_pontual.log"

OUTPUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_DIR.mkdir(exist_ok=True)
DASHBOARD_DIR.mkdir(exist_ok=True)


def log(msg: str) -> None:
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{agora}] {msg}"
    print(linha)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


def validar_env() -> None:
    faltando = []
    if not GTN_USER:
        faltando.append("GTN_USER")
    if not GTN_PASS:
        faltando.append("GTN_PASS")

    if faltando:
        raise ValueError(f"Variáveis ausentes no .env: {', '.join(faltando)}")


def salvar_debug(page, nome_base: str) -> None:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    screenshot_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.png"
    html_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.html"

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        html_path.write_text(page.content(), encoding="utf-8")
        log(f"📸 Screenshot salvo em: {screenshot_path}")
        log(f"📝 HTML salvo em: {html_path}")
    except Exception as e:
        log(f"⚠️ Falha ao salvar debug: {e}")


def fazer_login(page) -> None:
    log("🌐 Abrindo tela de login...")
    page.goto(GTN_URL, wait_until="domcontentloaded", timeout=60000)

    log("🔐 Preenchendo credenciais...")
    page.get_by_role("textbox", name="Usuário").click()
    page.get_by_role("textbox", name="Usuário").fill(GTN_USER)
    page.get_by_role("textbox", name="Senha").fill(GTN_PASS)

    log("➡️ Clicando em Acessar...")
    page.get_by_role("button", name="Acessar").click()

    page.wait_for_load_state("networkidle", timeout=60000)
    log(f"✅ Login concluído. URL atual: {page.url}")

    if "login" in page.url.lower():
        log("ℹ️ Ainda na tela de login. Tentando abrir home...")
        page.goto(GTN_HOME_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        log(f"✅ Home aberta. URL atual: {page.url}")


def abrir_execucao_testes(page) -> None:
    log("📂 Abrindo grupo do programa...")
    page.get_by_label("Exibição em Grade").get_by_role(
        "link",
        name="GRUPO PLUMA - PROGRAMA CONECTA"
    ).click()

    page.wait_for_load_state("networkidle", timeout=30000)

    log("🧭 Abrindo navegação principal...")
    page.get_by_role("button", name="Navegação Principal").click()

    log("🌲 Expandindo árvore...")
    page.locator(".a-TreeView-toggle").first.click()

    log("🧪 Entrando em Execução de Testes...")
    page.get_by_role("treeitem", name="Execução de Testes").click()

    page.wait_for_load_state("networkidle", timeout=60000)
    log(f"✅ Tela de execução carregada: {page.url}")


def aplicar_filtro(page) -> None:
    log("🔎 Abrindo filtro...")
    page.get_by_role("button", name="Ações").click()
    page.get_by_role("menuitem", name="Filtrar").click()

    log("🧩 Selecionando coluna LIDER_CENARIO...")
    page.locator("#R35932200234408468_column_name").select_option("LIDER_CENARIO")

    log("🧩 Selecionando operador IN...")
    page.locator("#R35932200234408468_STRING_OPT").select_option("in")

    log(f"🧾 Preenchendo expressão: {LIDERES_FILTRO}")
    page.get_by_role("textbox", name="Expressão").click()
    page.get_by_role("textbox", name="Expressão").fill(LIDERES_FILTRO)

    log("✅ Aplicando filtro...")
    page.get_by_role("button", name="Aplicar").click()
    page.wait_for_load_state("networkidle", timeout=30000)


def ajustar_quantidade_linhas(page) -> None:
    log("📄 Ajustando quantidade de linhas para 100000...")
    page.get_by_label("Linhas", exact=True).select_option("100000")
    page.wait_for_load_state("networkidle", timeout=30000)


def exportar_csv(page) -> Path:
    log("📤 Abrindo menu de download...")
    page.get_by_role("button", name="Ações").click()
    page.get_by_role("menuitem", name="Fazer Download").click()

    log("📄 Selecionando formato CSV...")
    page.get_by_role("option", name="CSV").click()

    log("⬇️ Iniciando download...")
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

    try:
        page.get_by_role("button", name="Fechar").click(timeout=3000)
        log("🪟 Janela de download fechada.")
    except Exception:
        log("ℹ️ Botão Fechar não apareceu, seguindo o fluxo.")

    return destino


def tratar_csv_para_dashboard(arquivo_entrada: Path) -> Path:
    log("🧹 Tratando CSV para o dashboard...")

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
                f"✅ CSV lido com sep='{tentativa['sep']}' "
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


def rodar_git(args, cwd: Path) -> str:
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


def commitar_e_enviar_arquivo(repo_dir: Path, arquivo: Path) -> None:
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


def ler_ultimas_linhas_log(qtd: int = 80) -> str:
    try:
        if not LOG_FILE.exists():
            return "Log ainda não foi criado."
        linhas = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(linhas[-qtd:])
    except Exception as e:
        return f"Não foi possível ler o log: {e}"


def enviar_email_falha(assunto: str, corpo: str) -> None:
    if not ALERT_EMAIL_TO:
        log("ℹ️ ALERT_EMAIL_TO não configurado. E-mail de falha não será enviado.")
        return

    faltando = []
    if not SMTP_HOST:
        faltando.append("SMTP_HOST")
    if not SMTP_USER:
        faltando.append("SMTP_USER")
    if not SMTP_PASS:
        faltando.append("SMTP_PASS")
    if not ALERT_EMAIL_FROM:
        faltando.append("ALERT_EMAIL_FROM")

    if faltando:
        log(
            "⚠️ Configuração de e-mail incompleta. Variáveis ausentes: "
            + ", ".join(faltando)
        )
        return

    msg = EmailMessage()
    msg["Subject"] = assunto
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = ALERT_EMAIL_TO
    msg.set_content(corpo)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.ehlo()
        if SMTP_USE_TLS:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

    log(f"📧 E-mail de falha enviado para: {ALERT_EMAIL_TO}")


def executar_fluxo() -> None:
    validar_env()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            fazer_login(page)
            abrir_execucao_testes(page)
            aplicar_filtro(page)
            ajustar_quantidade_linhas(page)

            arquivo_csv_baixado = exportar_csv(page)
            arquivo_dashboard = tratar_csv_para_dashboard(arquivo_csv_baixado)

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


def executar() -> None:
    if LOCK_FILE.exists():
        log("⚠️ Já existe uma execução em andamento. Encerrando.")
        return

    try:
        LOCK_FILE.touch()

        log("==================================================")
        log("🚀 Iniciando execução pontual do GTN")
        log("==================================================")

        executar_fluxo()

        log("✅ Execução encerrada com sucesso.")

    except Exception as e:
        detalhes_erro = traceback.format_exc()
        log(f"❌ Falha geral na execução: {e}")
        log(detalhes_erro)

        try:
            assunto = "Falha na atualização do dashboard GTN"
            corpo = (
                "A execução automática do script GTN falhou.\n\n"
                f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Erro: {e}\n\n"
                "Traceback:\n"
                f"{detalhes_erro}\n\n"
                "Últimas linhas do log:\n"
                f"{ler_ultimas_linhas_log()}"
            )
            enviar_email_falha(assunto, corpo)
        except Exception as email_error:
            log(f"⚠️ Falha ao enviar e-mail de alerta: {email_error}")

    finally:
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except Exception as e:
            log(f"⚠️ Não consegui remover lock file: {e}")


if __name__ == "__main__":
    executar()