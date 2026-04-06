from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import time

load_dotenv()

GTN_URL = os.getenv("GTN_URL")
GTN_USER = os.getenv("GTN_USER")
GTN_PASS = os.getenv("GTN_PASS")

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def salvar_debug(page, nome_base="debug"):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    screenshot_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.png"
    html_path = OUTPUT_DIR / f"{nome_base}_{timestamp}.html"

    page.screenshot(path=str(screenshot_path), full_page=True)
    html_path.write_text(page.content(), encoding="utf-8")

    print(f"📸 Screenshot salvo em: {screenshot_path}")
    print(f"📝 HTML salvo em: {html_path}")

def validar_env():
    faltando = []
    if not GTN_URL:
        faltando.append("GTN_URL")
    if not GTN_USER:
        faltando.append("GTN_USER")
    if not GTN_PASS:
        faltando.append("GTN_PASS")

    if faltando:
        raise ValueError(f"Variáveis ausentes no .env: {', '.join(faltando)}")

def tentar_preencher_login(page):
    # Tentativas comuns por label, placeholder e seletores genéricos
    candidatos_usuario = [
        page.get_by_label("Usuário"),
        page.locator("input[type='text']").first,
        page.locator("input[name*='USER' i]").first,
        page.locator("input[id*='USER' i]").first,
    ]

    candidatos_senha = [
        page.get_by_label("Senha"),
        page.locator("input[type='password']").first,
        page.locator("input[name*='PASS' i]").first,
        page.locator("input[id*='PASS' i]").first,
    ]

    preenchido_user = False
    preenchido_pass = False

    for campo in candidatos_usuario:
        try:
            campo.wait_for(state="visible", timeout=3000)
            campo.fill(GTN_USER)
            preenchido_user = True
            break
        except Exception:
            pass

    for campo in candidatos_senha:
        try:
            campo.wait_for(state="visible", timeout=3000)
            campo.fill(GTN_PASS)
            preenchido_pass = True
            break
        except Exception:
            pass

    if not preenchido_user:
        raise RuntimeError("Não consegui localizar o campo de usuário.")

    if not preenchido_pass:
        raise RuntimeError("Não consegui localizar o campo de senha.")

def tentar_clicar_acessar(page):
    candidatos_botao = [
        page.get_by_role("button", name="Acessar"),
        page.get_by_text("Acessar", exact=True),
        page.locator("button").filter(has_text="Acessar").first,
        page.locator("input[type='submit']").first,
        page.locator("button[type='submit']").first,
    ]

    for botao in candidatos_botao:
        try:
            botao.wait_for(state="visible", timeout=3000)
            botao.click()
            return
        except Exception:
            pass

    raise RuntimeError("Não consegui localizar o botão Acessar.")

def login_gtn():
    validar_env()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # deixe False para ver acontecer
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            print(f"🌐 Acessando: {GTN_URL}")
            page.goto(GTN_URL, wait_until="domcontentloaded", timeout=60000)

            print("🔐 Preenchendo login...")
            tentar_preencher_login(page)

            print("➡️ Clicando em Acessar...")
            tentar_clicar_acessar(page)

            page.wait_for_load_state("networkidle", timeout=20000)
            time.sleep(3)

            url_atual = page.url
            print(f"✅ URL após login: {url_atual}")

            salvar_debug(page, "apos_login")

            # Heurística simples
            if "login" in url_atual.lower():
                print("⚠️ Ainda parece estar na tela de login. Verifique se houve erro de autenticação.")
            else:
                print("✅ Login aparentemente realizado com sucesso.")

        except PlaywrightTimeoutError as e:
            print(f"⏰ Timeout: {e}")
            salvar_debug(page, "timeout")
        except Exception as e:
            print(f"❌ Erro: {e}")
            salvar_debug(page, "erro")
        finally:
            input("Pressione ENTER para fechar o navegador...")
            browser.close()

if __name__ == "__main__":
    login_gtn()