const AUTH_CONFIG = {
  usuario: "conecta",
  senha: "pluma@2026",
  chaveAuth: "dashboard_auth"
};

function jaAutenticado() {
  return sessionStorage.getItem(AUTH_CONFIG.chaveAuth) === "ok";
}

function fazerLogin(usuario, senha) {
  if (usuario !== AUTH_CONFIG.usuario) {
    return { sucesso: false, erro: "usuario" };
  }

  if (senha !== AUTH_CONFIG.senha) {
    return { sucesso: false, erro: "senha" };
  }

  sessionStorage.setItem(AUTH_CONFIG.chaveAuth, "ok");
  return { sucesso: true };
}

function fazerLogout() {
  sessionStorage.removeItem(AUTH_CONFIG.chaveAuth);
  window.location.replace("login.html");
}

function protegerPagina(destinoLogin = "login.html") {
  if (!jaAutenticado()) {
    window.location.replace(destinoLogin);
  }
}

function limparSessao() {
  sessionStorage.removeItem(AUTH_CONFIG.chaveAuth);
}