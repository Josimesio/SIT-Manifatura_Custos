const els = {
  dataAtualizacao: document.getElementById('dataAtualizacao'),
  globalPercent: document.getElementById('globalPercent'),
  ringProgress: document.getElementById('ringProgress'),
  motivationalTag: document.getElementById('motivationalTag'),
  totalCenarios: document.getElementById('totalCenarios'),
  totalConcluidos: document.getElementById('totalConcluidos'),
  totalEmAndamento: document.getElementById('totalEmAndamento'),
  totalNaoIniciado: document.getElementById('totalNaoIniciado'),
  headlineCallout: document.getElementById('headlineCallout'),
  headlinePill: document.getElementById('headlinePill'),
  leaderboard: document.getElementById('leaderboard'),
  statusBars: document.getElementById('statusBars'),
  areaBoard: document.getElementById('areaBoard'),
  focusTable: document.getElementById('focusTable')
};

const RING_CIRCUMFERENCE = 301.59;
const AUTO_CSV_NAME = 'dashboard_data/Cenarios_Consolidados_atualizado.csv';
const AUTO_REFRESH_INTERVAL_MS = 180000; // 3 minutos

const motivationalMessages = [
  { threshold: 0, tag: 'Aquecendo os motores', title: 'O jogo começou. Agora é sair do planejamento e entrar na execução.', pill: 'Sem moleza' },
  { threshold: 20, tag: 'Ritmo ganhando corpo', title: 'Tem avanço na pista. Agora é apertar o passo e reduzir o estoque de cenário parado.', pill: 'Subindo de nível' },
  { threshold: 40, tag: 'Competição de verdade', title: 'O time já mostrou serviço. Quem acelerar agora começa a cheirar pódio.', pill: 'Olho no ranking' },
  { threshold: 60, tag: 'Sprint valendo respeito', title: 'A reta ficou bonita. A turma que concluir agora vira referência, não desculpa.', pill: 'Fase quente' },
  { threshold: 80, tag: 'Cheiro de vitória', title: 'A meta está no radar. Faltam poucos golpes certeiros para transformar esforço em resultado.', pill: 'Pódio à vista' },
  { threshold: 100, tag: 'Missão cumprida', title: 'Todos os cenários concluídos. Aqui não teve conversa: teve entrega.', pill: 'Lenda desbloqueada' }
];

document.addEventListener('DOMContentLoaded', () => {
  tryAutoLoadCsv();

  setInterval(() => {
    tryAutoLoadCsv();
  }, AUTO_REFRESH_INTERVAL_MS);
});

async function tryAutoLoadCsv() {
  setGeneratedAt(`Lendo ${AUTO_CSV_NAME}...`);

  try {
    const response = await fetch(`${AUTO_CSV_NAME}?t=${Date.now()}`, {
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const buffer = await response.arrayBuffer();
    const text = new TextDecoder('utf-8').decode(buffer);
    parseCsvText(text);
  } catch (error) {
    console.warn('Falha no carregamento automático do CSV:', error);
    setGeneratedAt(`Arquivo processado não encontrado em ${AUTO_CSV_NAME}. Gere o CSV atualizado pelo script Python.`);
  }
}

function parseCsvText(text) {
  const parsed = Papa.parse(text, {
    header: true,
    skipEmptyLines: true,
    delimiter: ';'
  });

  const rows = parsed.data || [];
  renderDashboard(rows);
  updateGeneratedAt(rows);
}

function updateGeneratedAt(rows) {
  if (!els.dataAtualizacao) return;

  if (!rows.length) {
    setGeneratedAt('CSV sem dados para exibir.');
    return;
  }

  const generatedAt = getValue(rows[0], 'Gerado em');
  if (generatedAt) {
    setGeneratedAt(generatedAt);
  } else {
    setGeneratedAt("Coluna 'Gerado em' não encontrada no CSV processado.");
  }
}

function setGeneratedAt(message) {
  if (els.dataAtualizacao) {
    els.dataAtualizacao.textContent = message;
  }
}

function normalize(text = '') {
  return String(text)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function findKey(row, target) {
  const entries = Object.keys(row).map(key => [key, normalize(key)]);
  const hit = entries.find(([, norm]) => norm === normalize(target));
  return hit ? hit[0] : null;
}

function getValue(row, target, fallback = '') {
  const key = findKey(row, target);
  return key ? row[key] : fallback;
}

function renderDashboard(rows) {
  const cleanRows = rows
    .filter(row => Object.values(row).some(v => String(v).trim() !== ''))
    .map(row => ({
      identificador: getValue(row, 'Identificador'),
      cenario: getValue(row, 'Cenário') || getValue(row, 'Cenario'),
      area: getValue(row, 'Área de Negócio') || getValue(row, 'Area de Negocio') || 'Não informada',
      lider: getValue(row, 'Lider do Cenário') || getValue(row, 'Líder do Cenário') || 'Sem líder',
      statusOriginal: getValue(row, 'Status') || 'Sem status',
      prioridade: getValue(row, 'Grupo de Prioridade'),
      execucoes: Number(String(getValue(row, 'Qtde. Execuções Concluídas') || '0').replace(',', '.')) || 0
    }));

  const total = cleanRows.length;
  const concluded = cleanRows.filter(row => isConcluded(row.statusOriginal)).length;
  const inProgress = cleanRows.filter(row => isInProgress(row.statusOriginal)).length;
  const notStarted = cleanRows.filter(row => isNotStarted(row.statusOriginal)).length;
  const percent = total ? Math.round((concluded / total) * 100) : 0;

  updateSummary(total, concluded, inProgress, notStarted, percent);
  renderLeaderboard(cleanRows);
  renderStatusBars(total, concluded, inProgress, notStarted);
  renderAreaBoard(cleanRows);
  renderFocusTable(cleanRows);
}

function updateSummary(total, concluded, inProgress, notStarted, percent) {
  els.totalCenarios.textContent = total.toLocaleString('pt-BR');
  els.totalConcluidos.textContent = concluded.toLocaleString('pt-BR');
  els.totalEmAndamento.textContent = inProgress.toLocaleString('pt-BR');
  els.totalNaoIniciado.textContent = notStarted.toLocaleString('pt-BR');
  els.globalPercent.textContent = `${percent}%`;
  els.ringProgress.style.strokeDashoffset = `${RING_CIRCUMFERENCE * (1 - percent / 100)}`;

  const currentMessage = [...motivationalMessages].reverse().find(item => percent >= item.threshold) || motivationalMessages[0];
  els.motivationalTag.textContent = currentMessage.tag;
  els.headlineCallout.textContent = currentMessage.title;
  els.headlinePill.textContent = currentMessage.pill;
}

function renderLeaderboard(rows) {
  if (!rows.length) {
    els.leaderboard.innerHTML = 'Nenhum dado disponível.';
    return;
  }

  const grouped = new Map();

  rows.forEach(row => {
    const key = row.lider || 'Sem líder';
    if (!grouped.has(key)) {
      grouped.set(key, { lider: key, total: 0, concluded: 0, inProgress: 0 });
    }
    const item = grouped.get(key);
    item.total += 1;
    if (isConcluded(row.statusOriginal)) item.concluded += 1;
    if (isInProgress(row.statusOriginal)) item.inProgress += 1;
  });

  const ranking = [...grouped.values()]
    .map(item => ({ ...item, percent: item.total ? Math.round((item.concluded / item.total) * 100) : 0 }))
    .sort((a, b) => b.concluded - a.concluded || b.percent - a.percent || a.lider.localeCompare(b.lider, 'pt-BR'))
    .slice(0, 20);

  els.leaderboard.innerHTML = ranking.map((item, index) => `
    <div class="leader-row">
      <div class="place ${index < 3 ? `top-${index + 1}` : ''}">${index + 1}</div>
      <div>
        <div class="leader-name">${escapeHtml(item.lider)}</div>
        <div class="leader-meta">${item.concluded} concluídos de ${item.total} cenários · ${item.inProgress} em andamento</div>
      </div>
      <div class="leader-score">
        <strong>${item.percent}%</strong>
        <span>aproveitamento</span>
      </div>
    </div>
  `).join('');
}

function renderStatusBars(total, concluded, inProgress, notStarted) {
  const other = Math.max(total - concluded - inProgress - notStarted, 0);
  const statuses = [
    { label: 'Concluído', value: concluded, percent: getPercent(concluded, total), color: 'linear-gradient(90deg, #14d3a6, #7dffd8)' },
    { label: 'Em andamento', value: inProgress, percent: getPercent(inProgress, total), color: 'linear-gradient(90deg, #ffb84d, #ffd88d)' },
    { label: 'Não iniciado', value: notStarted, percent: getPercent(notStarted, total), color: 'linear-gradient(90deg, #ff6b7a, #ff9daa)' },
    { label: 'Outros', value: other, percent: getPercent(other, total), color: 'linear-gradient(90deg, #98a7d8, #cad5ff)' }
  ];

  els.statusBars.innerHTML = statuses.map(item => `
    <div class="status-item">
      <div class="status-head">
        <strong>${item.label}</strong>
        <span>${item.value} · ${item.percent}%</span>
      </div>
      <div class="status-track">
        <div class="status-fill" style="width:${item.percent}%; background:${item.color}"></div>
      </div>
    </div>
  `).join('');
}

function renderAreaBoard(rows) {
  if (!rows.length) {
    els.areaBoard.innerHTML = 'Nenhum dado disponível.';
    return;
  }

  const grouped = new Map();
  rows.forEach(row => {
    const key = row.area || 'Não informada';
    if (!grouped.has(key)) {
      grouped.set(key, { area: key, total: 0, concluded: 0, leaders: new Set() });
    }
    const item = grouped.get(key);
    item.total += 1;
    if (isConcluded(row.statusOriginal)) item.concluded += 1;
    item.leaders.add(row.lider || 'Sem líder');
  });

  const cards = [...grouped.values()]
    .map(item => ({ ...item, percent: getPercent(item.concluded, item.total), leaderCount: item.leaders.size }))
    .sort((a, b) => b.percent - a.percent || b.concluded - a.concluded || a.area.localeCompare(b.area, 'pt-BR'))
    .slice(0, 12);

  els.areaBoard.innerHTML = cards.map(item => `
    <div class="area-card">
      <div class="area-top">
        <div class="area-title">${escapeHtml(item.area)}</div>
        <div class="area-badge">${item.percent}%</div>
      </div>
      <div class="status-track" style="margin-top:12px;">
        <div class="status-fill" style="width:${item.percent}%; background: linear-gradient(90deg, #7c5cff, #14d3a6);"></div>
      </div>
      <div class="area-stats">
        <span>${item.concluded}/${item.total} concluídos</span>
        <span>${item.leaderCount} líder(es)</span>
      </div>
    </div>
  `).join('');
}

function renderFocusTable(rows) {
  const priorityRows = rows
    .filter(row => !isConcluded(row.statusOriginal))
    .sort((a, b) => comparePriority(a.prioridade, b.prioridade) || b.execucoes - a.execucoes)
    .slice(0, 10);

  if (!priorityRows.length) {
    els.focusTable.innerHTML = '<tr><td colspan="5" class="empty-cell">Nada pendente. Isso aqui ficou bonito.</td></tr>';
    return;
  }

  els.focusTable.innerHTML = priorityRows.map(row => `
    <tr>
      <td>${escapeHtml(row.identificador || '-')}</td>
      <td>${escapeHtml(row.cenario || '-')}</td>
      <td>${escapeHtml(row.lider || '-')}</td>
      <td>${escapeHtml(row.area || '-')}</td>
      <td>${statusPill(row.statusOriginal)}</td>
    </tr>
  `).join('');
}

function isConcluded(status) {
  return normalize(status).includes('concluido');
}

function isInProgress(status) {
  const s = normalize(status);
  return s.includes('andamento') || s.includes('em execucao') || s.includes('em progresso');
}

function isNotStarted(status) {
  const s = normalize(status);
  return s.includes('nao iniciado');
}

function getPercent(value, total) {
  return total ? Math.round((value / total) * 100) : 0;
}

function comparePriority(a, b) {
  const pa = extractPriorityNumber(a);
  const pb = extractPriorityNumber(b);
  return pb - pa;
}

function extractPriorityNumber(value) {
  const match = String(value || '').match(/\d+/);
  return match ? Number(match[0]) : -1;
}

function statusPill(status) {
  const label = escapeHtml(status || '-');
  let className = 'neutral';

  if (isConcluded(status)) className = 'done';
  else if (isInProgress(status)) className = 'progress';
  else if (isNotStarted(status)) className = 'not-started';

  return `<span class="status-pill ${className}">${label}</span>`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}