#!/usr/bin/env python3
"""
Disparador de Campanhas WhatsApp via Evolution API (Enterprise: até 50 Chips Simultâneos + 1 Conta Admin)
- Conta Principal Administradora (conta_admin): NUNCA prospecta, apenas administra e encaminha leads/alertas.
- Pool de até 50 Chips de Prospecção (conta_1 a conta_50) com balanceamento Round-Robin.
- Sistema de Quarentena / Cooldown com Timer Regressivo automático em caso de restrição/erro.
- Log dedicado de erros e relatório de restrições com sugestões automáticas de mitigação.
- Proteção dupla anti-reenvio: Cofre Permanente + consulta ao histórico real do chip (/chat/findMessages).
"""

import argparse
import csv
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

CSV_LOCK = threading.RLock()
MAX_WHATSAPP_ACCOUNTS = 50

# Configuração Global da Evolution API
EVOLUTION_API_URL = os.getenv("EVOLUTION_API_URL", "http://localhost:8081").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "")
WEBHOOK_INTERNAL_URL = os.getenv("WEBHOOK_INTERNAL_URL", "http://host.docker.internal:5000/webhook")

# Campaign & Anti-Ban Settings
DEFAULT_DELAY = float(os.getenv("DEFAULT_DELAY", 15.0))
MIN_DELAY = float(os.getenv("MIN_DELAY", 15.0))
MAX_DELAY = float(os.getenv("MAX_DELAY", 35.0))
TYPING_MS = int(os.getenv("TYPING_MS", 1200))
DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
DAILY_MESSAGE_LIMIT = int(os.getenv("DAILY_MESSAGE_LIMIT", 60))
BUSINESS_START_HOUR = int(os.getenv("BUSINESS_START_HOUR", 8))
BUSINESS_END_HOUR = int(os.getenv("BUSINESS_END_HOUR", 18))
BUSINESS_DAYS_ONLY = os.getenv("BUSINESS_DAYS_ONLY", "True").lower() == "true"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_default_leads_rel = "data/leads.local.csv" if os.path.exists(os.path.join(BASE_DIR, "data/leads.local.csv")) else "data/leads.csv"
LEADS_FILE = os.path.join(BASE_DIR, os.getenv("LEADS_FILE", _default_leads_rel))
ACCOUNTS_FILE = os.path.join(BASE_DIR, os.getenv("ACCOUNTS_FILE", "data/whatsapp_accounts.json"))
HISTORICO_ENVIADOS_FILE = os.path.join(BASE_DIR, "data/historico_enviados_permanente.txt")
ENCAMINHAMENTOS_FILE = os.path.join(BASE_DIR, "data/encaminhamentos_permanente.jsonl")
RESTRICTIONS_LOG_FILE = os.path.join(BASE_DIR, "data/restrictions_errors.log")
ROUTING_CONFIG_FILE = os.path.join(BASE_DIR, "data/routing_config.json")

MENSAGEM_PADRAO_PROSPECCAO = (
    "Olá, tudo bem? Atuo com consultoria de planos empresariais B2B.\n\n"
    "Vi que você possui um CNPJ ativo — e muita gente não sabe, mas a modalidade "
    "empresarial costuma ser bem mais acessível que a individual.\n\n"
    "Posso te enviar uma cotação sem compromisso?\n\n"
    "Digite:\n"
    "1️⃣ para receber um orçamento\n"
    "2️⃣ para não receber mais mensagens"
)

COLUNAS_PADRAO = [
    "id",
    "nome",
    "telefone",
    "status",
    "opt_out",
    "conta_id",
    "message_id",
    "ultima_interacao_em",
    "ultima_resposta",
    "encaminhado_para",
    "encaminhado_em",
]

SAUDACOES_VARIADAS = [
    "Olá, tudo bem?",
    "Oi, tudo bem?",
    "Olá! Tudo bem por aí?",
    "Oi! Tudo certo?",
    "Olá, como vai?",
]

WARMUP_TIERS = {
    "dia_1_3": {"label": "🌱 Aquecimento Inicial (Dias 1–3)", "max_daily": 10},
    "dia_4_7": {"label": "🌿 Aquecimento Médio (Dias 4–7)", "max_daily": 25},
    "dia_8_14": {"label": "🌳 Aquecimento Avançado (Dias 8–14)", "max_daily": 45},
    "producao": {"label": "🚀 Produção Plena (15+ dias)", "max_daily": 60},
}

WARMUP_PING_FRASES = [
    "Bom dia! Tudo certo por aí?",
    "Oi, tudo bem? Passando só para confirmar o horário de hoje.",
    "Olá! Conseguiu ver aquele documento que combinamos?",
    "Tudo ótimo por aqui, e com você?",
    "Show! Qualquer novidade me avisa por aqui 👍",
]


def calcular_limite_efetivo_chip(account: Dict) -> int:
    """Retorna o menor valor entre daily_limit configurado e o teto seguro do estágio de Warm-Up."""
    cfg_lim = int(account.get("daily_limit", DAILY_MESSAGE_LIMIT))
    tier = str(account.get("warmup_tier") or "producao").strip().lower()
    if tier in WARMUP_TIERS:
        return min(cfg_lim, WARMUP_TIERS[tier]["max_daily"])
    return cfg_lim


def calcular_saude_chip(account: Dict, metrica: Optional[Dict] = None) -> int:
    """Calcula o Health Score Anti-Ban (0 a 100) com base em erros, uso da cota, cooldown e engajamento."""
    score = 100
    if conta_em_quarentena(account) > 0:
        score -= 45
    erros = int(account.get("erros_hoje", 0))
    score -= min(40, erros * 15)
    lim = max(1, calcular_limite_efetivo_chip(account))
    uso = int(account.get("enviados_hoje", 0)) / lim
    if uso > 0.9:
        score -= 15
    elif uso > 0.75:
        score -= 5
    if str(account.get("connection_status", "")).lower() != "open":
        score -= 20
    if metrica:
        resp = int(metrica.get("interessados", 0)) + int(metrica.get("respondeu", 0))
        opt = int(metrica.get("opt_out", 0))
        score += min(10, resp * 2)
        score -= min(15, opt * 3)
    return max(0, min(100, score))


def executar_aquecimento_cruzado(contas_conectadas: List[Dict]) -> Dict:
    """
    Executa 1 troca de mensagem natural entre 2 chips de prospecção conectados (que possuam número conhecido)
    para gerar histórico bidirecional orgânico (método validado de Warm-Up).
    """
    elegiveis = [
        c for c in contas_conectadas
        if not c.get("admin_only")
        and c.get("id") != "conta_admin"
        and str(c.get("connection_status", "")).lower() == "open"
        and conta_em_quarentena(c) == 0
        and normalize_phone(c.get("telefone_exibicao", ""))
    ]
    if len(elegiveis) < 2:
        return {
            "ok": False,
            "error": "São necessários pelo menos 2 chips de prospecção 🟢 CONECTADOS com o campo 'Número do Chip' preenchido para o aquecimento cruzado.",
        }

    remetente, destinatario = random.sample(elegiveis, 2)
    tel_dest = normalize_phone(destinatario.get("telefone_exibicao", ""))
    frase_1 = random.choice(WARMUP_PING_FRASES)
    inst_1 = remetente.get("instance_name")
    base = _evo_base_url()

    try:
        r1 = requests.post(
            f"{base}/message/sendText/{inst_1}",
            headers=_evo_headers(remetente),
            json={"number": tel_dest, "text": frase_1, "delay": 1500, "linkPreview": False},
            timeout=15,
        )
        r1.raise_for_status()
        return {
            "ok": True,
            "from_instance": inst_1,
            "to_instance": destinatario.get("instance_name"),
            "to_phone": tel_dest,
            "message": frase_1,
        }
    except Exception as e:
        return {"ok": False, "error": f"Falha no ping de aquecimento ({inst_1} -> {destinatario.get('instance_name')}): {e}"}


def normalize_phone(phone: str) -> str:
    """Remove caracteres não numéricos e garante o DDI 55 (Brasil) para telefones de 10 ou 11 dígitos."""
    digits = re.sub(r"\D", "", str(phone or "").split("@")[0])
    if not digits:
        return ""
    if len(digits) in (10, 11) and not digits.startswith("55"):
        return f"55{digits}"
    return digits


def phone_dedup_key(phone: str) -> str:
    """Chave única DDD + 8 últimos dígitos (imune à variação do 9º dígito no WhatsApp)."""
    norm = normalize_phone(phone)
    if len(norm) >= 12 and norm.startswith("55"):
        return f"{norm[:4]}_{norm[-8:]}"
    return norm


def is_business_hours() -> bool:
    """Verifica se o horário atual está dentro da janela comercial configurada."""
    now = datetime.now()
    if BUSINESS_DAYS_ONLY and now.weekday() > 4:
        return False
    return BUSINESS_START_HOUR <= now.hour < BUSINESS_END_HOUR


def formatar_mensagem_lead(template_msg: str, nome_lead: str = "", usar_variacao: bool = True) -> str:
    """
    Substitui variáveis {nome} e {primeiro_nome}, processa blocos Spintax {op1|op2}
    e aplica micro-variação automática na saudação para evitar hash repetido (Anti-Ban).
    """
    texto = template_msg or MENSAGEM_PADRAO_PROSPECCAO
    nome_limpo = (nome_lead or "").strip()
    if nome_limpo.lower().startswith(("lead ", "contato ", "empresa ")) or not nome_limpo:
        primeiro = "tudo bem"
        completo = "Cliente"
    else:
        primeiro = nome_limpo.split()[0].capitalize()
        completo = nome_limpo

    texto = texto.replace("{primeiro_nome}", primeiro).replace("{nome}", completo)

    def _resolver_spintax(match: re.Match) -> str:
        opcoes = [op.strip() for op in match.group(1).split("|") if op.strip()]
        return random.choice(opcoes) if opcoes else ""

    texto = re.sub(r"\{([^{}]*\|[^{}]*)\}", _resolver_spintax, texto)

    if usar_variacao and texto.startswith("Olá, tudo bem?"):
        saudacao = random.choice(SAUDACOES_VARIADAS)
        texto = saudacao + texto[len("Olá, tudo bem?"):]

    return texto


def diagnosticar_erro_e_sugerir(codigo_erro: str, detalhe: str = "", enviados_hoje: int = 0) -> Dict:
    """
    Analisa a falha/restrição ocorrida no chip, define o tempo ideal de Cooldown (Quarentena)
    e gera uma recomendação clara e acionável para o operador.
    """
    det_lower = (detalhe or "").lower()
    cod_lower = (codigo_erro or "").lower()

    if "428" in cod_lower or "428" in det_lower or "precondition" in det_lower or "connection closed" in det_lower:
        return {
            "cooldown_minutos": 30,
            "nivel_risco": "ALTO",
            "titulo": "Queda de Sessão / Rate Limit de Socket (HTTP 428)",
            "sugestao": (
                "O WhatsApp fechou temporariamente a conexão por excesso de requisições seguidas. "
                "Recomendação: Mantenha o chip em pausa até o fim do timer de 30 min, aumente o Jitter para 20s–45s "
                "e envie lotes menores (máx. 5 a 10 por rodada)."
            ),
        }
    if "401" in cod_lower or "403" in cod_lower or "logged out" in det_lower or "refused" in det_lower:
        return {
            "cooldown_minutos": 120,
            "nivel_risco": "CRÍTICO",
            "titulo": "Sessão Desconectada ou Restrição Temporária (401/403)",
            "sugestao": (
                "O aparelho desconectou ou sofreu restrição preventiva. "
                "Recomendação: Verifique o WhatsApp no aparelho físico, aguarde o timer de 2h antes de reconectar o QR Code "
                "e faça 5 conversas manuais entre seus próprios chips para aquecer (Warm-Up)."
            ),
        }
    if "404" in cod_lower:
        return {
            "cooldown_minutos": 10,
            "nivel_risco": "MÉDIO",
            "titulo": "Instância Não Inicializada na Evolution API (HTTP 404)",
            "sugestao": (
                "Este chip está marcado como Ativo no pool, mas sua instância ainda não foi conectada via QR Code. "
                "Recomendação: Clique em '📱 QR Code' no card deste chip para parear o aparelho ou deixe-o PAUSADO."
            ),
        }
    if "500" in cod_lower or "internal server error" in det_lower:
        return {
            "cooldown_minutos": 20,
            "nivel_risco": "ALTO",
            "titulo": "Falha Interna de Envio Baileys (HTTP 500)",
            "sugestao": (
                "A instância não conseguiu entregar a mensagem (possível queda momentânea do socket ou número inválido). "
                "Recomendação: Aguarde o cooldown automático de 20 min e clique em '📡 Sincronizar Status' para validar a sessão."
            ),
        }
    return {
        "cooldown_minutos": 15,
        "nivel_risco": "MÉDIO",
        "titulo": f"Alerta Operacional ({codigo_erro})",
        "sugestao": (
            "Falha de comunicação detectada. Recomendação: Verifique se o container Docker da Evolution API (porta 8081) "
            "está ativo e mantenha o Jitter acima de 15 segundos."
        ),
    }


def registrar_log_restricao(
    account: Dict,
    codigo_erro: str,
    detalhe: str,
    diagnostico: Dict,
    telefone_alvo: str = "",
) -> Dict:
    """Registra evento de erro/restrição em data/restrictions_errors.log com diagnóstico e timer."""
    os.makedirs(os.path.dirname(RESTRICTIONS_LOG_FILE) or ".", exist_ok=True)
    agora = datetime.now()
    expira_em = (agora + timedelta(minutes=int(diagnostico.get("cooldown_minutos", 15)))).isoformat()
    registro = {
        "timestamp": agora.isoformat(),
        "account_id": account.get("id", "conta_1"),
        "slot": account.get("slot", 1),
        "account_name": account.get("nome", ""),
        "instance_name": account.get("instance_name", ""),
        "target_phone": normalize_phone(telefone_alvo),
        "error_code": codigo_erro,
        "detail": (detalhe or "")[:240],
        "risk_level": diagnostico.get("nivel_risco", "MÉDIO"),
        "title": diagnostico.get("titulo", codigo_erro),
        "suggestion": diagnostico.get("sugestao", ""),
        "cooldown_minutes": diagnostico.get("cooldown_minutos", 15),
        "cooldown_until": expira_em,
    }
    try:
        with open(RESTRICTIONS_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
    except Exception:
        pass
    return registro


def ler_logs_restricao(limit: int = 40) -> List[Dict]:
    """Lê os últimos registros de restrição e erros do arquivo data/restrictions_errors.log."""
    if not os.path.exists(RESTRICTIONS_LOG_FILE):
        return []
    saida: List[Dict] = []
    try:
        with open(RESTRICTIONS_LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            linhas = f.readlines()[-limit:]
            for ln in reversed(linhas):
                ln = ln.strip()
                if ln:
                    try:
                        saida.append(json.loads(ln))
                    except Exception:
                        pass
    except Exception:
        pass
    return saida


def aplicar_quarentena_conta(account_id: str, codigo_erro: str, detalhe: str = "", telefone_alvo: str = "") -> Dict:
    """Coloca um chip em quarentena com timer regressivo e salva a sugestão de mitigação."""
    contas = load_whatsapp_accounts()
    diag = {}
    reg = {}
    for c in contas:
        if c.get("id") == account_id or c.get("instance_name") == account_id:
            diag = diagnosticar_erro_e_sugerir(codigo_erro, detalhe, int(c.get("enviados_hoje", 0)))
            ate = (datetime.now() + timedelta(minutes=int(diag["cooldown_minutos"]))).isoformat()
            c["restrito_ate"] = ate
            c["motivo_restricao"] = diag["titulo"]
            c["sugestao_restricao"] = diag["sugestao"]
            c["nivel_risco"] = diag["nivel_risco"]
            c["erros_hoje"] = int(c.get("erros_hoje", 0)) + 1
            c["ultimo_erro_em"] = datetime.now().isoformat()
            c["ultimo_erro_detalhe"] = (detalhe or codigo_erro)[:180]
            reg = registrar_log_restricao(c, codigo_erro, detalhe, diag, telefone_alvo)
            break
    save_whatsapp_accounts(contas)
    return reg


def limpar_quarentena_conta(account_id: str) -> None:
    """Remove manualmente o timer de restrição de um chip e o reabilita."""
    contas = load_whatsapp_accounts()
    for c in contas:
        if c.get("id") == account_id:
            c["restrito_ate"] = ""
            c["motivo_restricao"] = ""
            c["sugestao_restricao"] = ""
            c["nivel_risco"] = "BAIXO"
            break
    save_whatsapp_accounts(contas)


def conta_em_quarentena(account: Dict) -> int:
    """Retorna quantos segundos restam de quarentena/restrição para o chip (0 se estiver livre)."""
    ate_str = str(account.get("restrito_ate") or "").strip()
    if not ate_str:
        return 0
    try:
        dt_ate = datetime.fromisoformat(ate_str)
        restante = int((dt_ate - datetime.now()).total_seconds())
        return max(0, restante)
    except Exception:
        return 0


def load_routing_config() -> Dict:
    """Carrega as configurações de encaminhamento de leads e da Conta Principal Administradora."""
    padrao = {
        "modo_encaminhamento": "fixo_por_chip",  # 'fixo_por_chip' | 'round_robin_consultores' | 'webhook_crm' | 'apenas_painel'
        "usar_conta_admin_para_alertas": True,
        "notificar_restricoes_no_admin": True,
        "som_notificacao_ativo": True,
        "enviar_msg_opt_out_manual": True,
        "mensagem_opt_out": "Entendido! Agradecemos o retorno e não enviaremos mais mensagens por aqui. Tenha um ótimo dia!",
        "consultor_padrao": normalize_phone(os.getenv("CONSULTANT_PHONE_NUMBER", "")),
        "fila_consultores": [
            {"nome": "Consultor Principal", "telefone": normalize_phone(os.getenv("CONSULTANT_PHONE_NUMBER", "")), "ativo": True, "recebidos": 0}
        ],
        "rr_consultor_idx": 0,
        "webhook_crm_url": os.getenv("CRM_WEBHOOK_URL", ""),
    }
    if not os.path.exists(ROUTING_CONFIG_FILE):
        save_routing_config(padrao)
        return padrao
    try:
        with open(ROUTING_CONFIG_FILE, "r", encoding="utf-8") as f:
            salvo = json.load(f)
        if isinstance(salvo, dict):
            padrao.update(salvo)
        return padrao
    except Exception:
        return padrao


def save_routing_config(cfg: Dict) -> None:
    """Salva a configuração de encaminhamento em data/routing_config.json."""
    os.makedirs(os.path.dirname(ROUTING_CONFIG_FILE) or ".", exist_ok=True)
    tmp = f"{ROUTING_CONFIG_FILE}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, ROUTING_CONFIG_FILE)


def escolher_consultor_destino(conta_origem: Dict) -> Dict:
    """
    Determina para qual consultor o lead quente deve ser encaminhado segundo a regra ativa:
    - 'fixo_por_chip': usa o consultant_phone do chip (ou consultor_padrao).
    - 'round_robin_consultores': alterna entre a lista de consultores ativos cadastrados.
    """
    cfg = load_routing_config()
    modo = cfg.get("modo_encaminhamento", "fixo_por_chip")

    if modo == "round_robin_consultores":
        fila = [c for c in (cfg.get("fila_consultores") or []) if c.get("ativo") and normalize_phone(c.get("telefone", ""))]
        if fila:
            idx = int(cfg.get("rr_consultor_idx", 0)) % len(fila)
            escolhido = fila[idx]
            escolhido["recebidos"] = int(escolhido.get("recebidos", 0)) + 1
            cfg["rr_consultor_idx"] = idx + 1
            save_routing_config(cfg)
            return {
                "nome": escolhido.get("nome", "Consultor"),
                "telefone": normalize_phone(escolhido.get("telefone", "")),
                "modo": "round_robin_consultores",
            }

    tel_chip = normalize_phone(conta_origem.get("consultant_phone", ""))
    tel_padrao = normalize_phone(cfg.get("consultor_padrao", "") or os.getenv("CONSULTANT_PHONE_NUMBER", ""))
    return {
        "nome": f"Consultor ({conta_origem.get('nome', 'Chip')})",
        "telefone": tel_chip or tel_padrao,
        "modo": modo,
    }


def carregar_historico_permanente() -> set:
    """
    Carrega o cofre permanente de números que já receberam mensagens (ou já interagiram),
    cruzando data/historico_enviados_permanente.txt e data/events.log.
    """
    chaves = set()
    os.makedirs(os.path.dirname(HISTORICO_ENVIADOS_FILE) or ".", exist_ok=True)
    if os.path.exists(HISTORICO_ENVIADOS_FILE):
        try:
            with open(HISTORICO_ENVIADOS_FILE, "r", encoding="utf-8", errors="replace") as f:
                for ln in f:
                    k = ln.strip()
                    if k:
                        chaves.add(phone_dedup_key(k) if "_" not in k else k)
        except Exception:
            pass

    events_path = os.path.join(BASE_DIR, "data/events.log")
    if os.path.exists(events_path):
        try:
            with open(events_path, "r", encoding="utf-8", errors="replace") as ef:
                for ln in ef:
                    if '"campaign_sent"' in ln or '"message_received"' in ln:
                        ev = json.loads(ln)
                        ph = ev.get("phone", "")
                        if ph:
                            chaves.add(phone_dedup_key(ph))
        except Exception:
            pass

    return chaves


def registrar_historico_permanente(phone: str) -> None:
    """Grava imediatamente a chave do número no cofre permanente anti-reenvio."""
    chave = phone_dedup_key(phone)
    if not chave:
        return
    os.makedirs(os.path.dirname(HISTORICO_ENVIADOS_FILE) or ".", exist_ok=True)
    atuais = carregar_historico_permanente()
    if chave not in atuais:
        with open(HISTORICO_ENVIADOS_FILE, "a", encoding="utf-8") as f:
            f.write(f"{chave}\n")


def _default_admin_account() -> Dict:
    """Cria a estrutura da Conta Principal Administradora (Não Prospecta, Apenas Administra)."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    return {
        "id": "conta_admin",
        "slot": 0,
        "role": "admin",
        "admin_only": True,
        "nome": "👑 Conta Principal Administradora (Não Prospecta)",
        "instance_name": os.getenv("WHATSAPP_ADMIN_INSTANCE", "chip_admin"),
        "ativo": False,  # Nunca entra no pool de prospecção fria!
        "connection_status": "close",
        "telefone_exibicao": normalize_phone(os.getenv("WHATSAPP_ADMIN_PHONE", "")),
        "consultant_phone": normalize_phone(os.getenv("CONSULTANT_PHONE_NUMBER", "")),
        "apikey": "",
        "daily_limit": 500,
        "enviados_hoje": 0,
        "erros_hoje": 0,
        "data_contagem": hoje,
        "ultimo_disparo_em": "",
        "restrito_ate": "",
        "motivo_restricao": "",
        "sugestao_restricao": "",
        "nivel_risco": "BAIXO",
    }


def _default_accounts() -> List[Dict]:
    """Gera a Conta Principal Administradora (slot 0) + as 50 instâncias de prospecção (slots 1 a 50)."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    contas: List[Dict] = [_default_admin_account()]
    for idx in range(1, MAX_WHATSAPP_ACCOUNTS + 1):
        prefix = f"WHATSAPP_ACCOUNT_{idx}_"
        instance_name = os.getenv(f"{prefix}INSTANCE", f"chip_{idx}")
        consultant = os.getenv(f"{prefix}CONSULTANT_PHONE") or os.getenv("CONSULTANT_PHONE_NUMBER", "")
        display_phone = os.getenv(f"{prefix}DISPLAY_PHONE", "")
        nome = os.getenv(f"{prefix}NAME", f"Chip #{idx}" + (" (Líder Pool)" if idx == 1 else ""))
        limit = int(os.getenv(f"{prefix}DAILY_LIMIT", str(DAILY_MESSAGE_LIMIT)))
        instance_token = os.getenv(f"{prefix}API_KEY", "")
        ativo_env = os.getenv(f"{prefix}ACTIVE")
        if ativo_env is not None:
            ativo = ativo_env.strip().lower() == "true"
        else:
            ativo = True if idx == 1 else False

        contas.append({
            "id": f"conta_{idx}",
            "slot": idx,
            "role": "prospeccao",
            "admin_only": False,
            "nome": nome,
            "instance_name": instance_name,
            "ativo": ativo,
            "connection_status": "close",
            "telefone_exibicao": normalize_phone(display_phone) if display_phone else "",
            "consultant_phone": normalize_phone(consultant) if consultant else "",
            "apikey": instance_token,
            "daily_limit": limit,
            "enviados_hoje": 0,
            "erros_hoje": 0,
            "data_contagem": hoje,
            "ultimo_disparo_em": "",
            "restrito_ate": "",
            "motivo_restricao": "",
            "sugestao_restricao": "",
            "nivel_risco": "BAIXO",
        })
    return contas


def load_whatsapp_accounts() -> List[Dict]:
    """Carrega a Conta Admin (slot 0) + os 50 chips de prospecção (slots 1..50)."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    padrao = _default_accounts()

    if not os.path.exists(ACCOUNTS_FILE):
        save_whatsapp_accounts(padrao)
        return padrao

    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            salvas = json.load(f)
        if not isinstance(salvas, list):
            return padrao

        mapa_salvas = {str(c.get("id") or f"conta_{c.get('slot', i)}"): c for i, c in enumerate(salvas)}
        resultado: List[Dict] = []
        for item_padrao in padrao:
            cid = item_padrao["id"]
            slot = item_padrao["slot"]
            if cid in mapa_salvas:
                merged = dict(item_padrao)
                merged.update(mapa_salvas[cid])
                merged["id"] = cid
                merged["slot"] = slot
                if cid == "conta_admin":
                    merged["role"] = "admin"
                    merged["admin_only"] = True
                    merged["ativo"] = False  # Garante que conta_admin NUNCA prospecta!
                    if not merged.get("instance_name"):
                        merged["instance_name"] = "chip_admin"
                else:
                    merged["role"] = "prospeccao"
                    merged["admin_only"] = False
                    if not merged.get("instance_name"):
                        merged["instance_name"] = f"chip_{slot}"
                if merged.get("data_contagem") != hoje:
                    merged["enviados_hoje"] = 0
                    merged["erros_hoje"] = 0
                    merged["data_contagem"] = hoje
                resultado.append(merged)
            else:
                resultado.append(item_padrao)
        return resultado[: MAX_WHATSAPP_ACCOUNTS + 1]
    except Exception:
        return padrao


def save_whatsapp_accounts(accounts: List[Dict]) -> None:
    """Salva a Conta Admin + até 50 instâncias de prospecção em data/whatsapp_accounts.json."""
    os.makedirs(os.path.dirname(ACCOUNTS_FILE) or ".", exist_ok=True)
    normalizadas = accounts[: MAX_WHATSAPP_ACCOUNTS + 1]
    for c in normalizadas:
        if c.get("id") == "conta_admin" or c.get("admin_only"):
            c["ativo"] = False  # Conta Admin jamais entra em prospecção ativa
    temp_file = f"{ACCOUNTS_FILE}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(normalizadas, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, ACCOUNTS_FILE)


def get_admin_account() -> Dict:
    """Retorna especificamente a Conta Principal Administradora (conta_admin)."""
    todas = load_whatsapp_accounts()
    for c in todas:
        if c.get("id") == "conta_admin" or c.get("admin_only"):
            return c
    return _default_admin_account()


def _evo_headers(account: Optional[Dict] = None) -> Dict[str, str]:
    api_key = (
        (account.get("apikey") if account else "")
        or os.getenv("EVOLUTION_API_KEY", EVOLUTION_API_KEY)
    )
    return {
        "apikey": api_key,
        "Content-Type": "application/json",
    }


def _evo_base_url() -> str:
    return os.getenv("EVOLUTION_API_URL", EVOLUTION_API_URL).rstrip("/")


def parse_proxy_url(proxy_url: str) -> Optional[Dict]:
    """Converte URL de proxy (http://user:pass@host:port ou socks5://...) em payload Evolution API."""
    raw = str(proxy_url or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = f"http://{raw}"
    from urllib.parse import urlparse
    u = urlparse(raw)
    if not u.hostname or not u.port:
        return None
    return {
        "enabled": True,
        "host": u.hostname,
        "port": str(u.port),
        "protocol": (u.scheme or "http").lower(),
        "username": u.username or "",
        "password": u.password or "",
    }


def evolution_set_proxy(account: Dict, proxy_url: str) -> Dict:
    """Configura ou desativa o proxy de uma instância específica na Evolution API (/proxy/set/{instance})."""
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
    base = _evo_base_url()
    parsed = parse_proxy_url(proxy_url)
    payload = parsed or {"enabled": False, "host": "", "port": "", "protocol": "http", "username": "", "password": ""}
    try:
        resp = requests.post(
            f"{base}/proxy/set/{inst}",
            headers=_evo_headers(account),
            json=payload,
            timeout=6,
        )
        if resp.ok:
            return {"ok": True, "instance": inst, "enabled": bool(parsed), "host": parsed["host"] if parsed else ""}
        return {"ok": False, "instance": inst, "error": f"HTTP {resp.status_code}: {resp.text[:160]}"}
    except Exception as e:
        return {"ok": False, "instance": inst, "error": str(e)}


def intercalar_contas_por_aparelho(contas: List[Dict]) -> List[Dict]:
    """
    Reordena os chips intercalando por 'device_label' (Celular #1 -> Celular #2 -> ... -> Celular #7)
    para que envios consecutivos nunca concentrem rajadas no mesmo aparelho físico ou mesmo proxy/IP.
    """
    if len(contas) <= 1:
        return contas
    grupos: Dict[str, List[Dict]] = {}
    for c in contas:
        chave = str(c.get("device_label") or "").strip().lower() or str(c.get("id"))
        grupos.setdefault(chave, []).append(c)
    chaves = list(grupos.keys())
    intercaladas: List[Dict] = []
    idx = 0
    while True:
        adicionou = False
        for k in chaves:
            if idx < len(grupos[k]):
                intercaladas.append(grupos[k][idx])
                adicionou = True
        if not adicionou:
            break
        idx += 1
    return intercaladas


def evolution_check_connection(account: Dict) -> Dict:
    """Consulta o status real da instância na Evolution API (/instance/connectionState/{instance})."""
    inst = account.get("instance_name") or ("chip_admin" if account.get("id") == "conta_admin" else f"chip_{account.get('slot', 1)}")
    base = _evo_base_url()
    try:
        resp = requests.get(
            f"{base}/instance/connectionState/{inst}",
            headers=_evo_headers(account),
            timeout=2.5,
        )
        if resp.status_code == 200:
            data = resp.json() or {}
            state = (
                data.get("instance", {}).get("state")
                or data.get("state")
                or "close"
            )
            return {"ok": True, "online": True, "state": state, "instance": inst}
        if resp.status_code == 404:
            return {"ok": True, "online": True, "state": "not_created", "instance": inst}
        return {"ok": False, "online": True, "state": "error", "http_status": resp.status_code}
    except Exception as e:
        return {"ok": False, "online": False, "state": "evolution_offline", "error": str(e)}


def evolution_ja_tem_conversa_no_chip(account: Dict, formatted_number: str) -> bool:
    """
    Consulta diretamente a Evolution API (/chat/findMessages/{instance}) para verificar
    se este chip JÁ conversou ou JÁ enviou mensagem para este número anteriormente no aparelho.
    """
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
    base = _evo_base_url()
    headers = _evo_headers(account)

    variantes = {formatted_number}
    if len(formatted_number) == 13 and formatted_number.startswith("55") and formatted_number[4] == "9":
        variantes.add(formatted_number[:4] + formatted_number[5:])
    elif len(formatted_number) == 12 and formatted_number.startswith("55"):
        variantes.add(formatted_number[:4] + "9" + formatted_number[4:])

    for num_var in variantes:
        jid = f"{num_var}@s.whatsapp.net"
        try:
            resp = requests.post(
                f"{base}/chat/findMessages/{inst}",
                headers=headers,
                json={"where": {"key": {"remoteJid": jid}}, "limit": 1},
                timeout=4,
            )
            if resp.ok and resp.content:
                data = resp.json()
                if isinstance(data, dict):
                    msgs_obj = data.get("messages")
                    if isinstance(msgs_obj, dict):
                        recs = msgs_obj.get("records") or []
                        total = int(msgs_obj.get("total") or len(recs))
                        if total > 0 or len(recs) > 0:
                            return True
                    elif isinstance(msgs_obj, list) and len(msgs_obj) > 0:
                        return True
                elif isinstance(data, list) and len(data) > 0:
                    return True
        except Exception:
            pass
    return False


def _create_fresh_evolution_instance(base: str, inst: str, headers: Dict[str, str], account: Dict) -> Dict:
    """Recria a instância na Evolution API v2 garantindo geração imediata de um novo QR Code Base64."""
    try:
        requests.delete(f"{base}/instance/delete/{inst}", headers=headers, timeout=6)
    except Exception:
        pass

    create_payload: Dict = {
        "instanceName": inst,
        "qrcode": True,
        "integration": "WHATSAPP-BAILEYS",
    }
    if account.get("telefone_exibicao"):
        create_payload["number"] = normalize_phone(account["telefone_exibicao"])

    create_resp = requests.post(
        f"{base}/instance/create",
        headers=headers,
        json=create_payload,
        timeout=12,
    )
    data = create_resp.json() if create_resp.content else {}
    if create_resp.status_code >= 400:
        return {
            "ok": False,
            "instance": inst,
            "error": f"Falha ao criar instância {inst} (HTTP {create_resp.status_code}): {json.dumps(data, ensure_ascii=False)[:220]}",
        }

    qr_base64 = (
        (data.get("qrcode") or {}).get("base64")
        if isinstance(data.get("qrcode"), dict)
        else data.get("base64") or ""
    )
    pairing_code = (
        (data.get("qrcode") or {}).get("pairingCode")
        if isinstance(data.get("qrcode"), dict)
        else data.get("pairingCode") or ""
    )

    for _ in range(5):
        if qr_base64:
            break
        time.sleep(1.0)
        c_try = requests.get(f"{base}/instance/connect/{inst}", headers=headers, timeout=8)
        if c_try.ok and c_try.content:
            cdata = c_try.json() or {}
            qr_base64 = cdata.get("base64") or (cdata.get("qrcode") or {}).get("base64") or ""
            pairing_code = cdata.get("pairingCode") or (cdata.get("qrcode") or {}).get("pairingCode") or ""

    return {
        "ok": True,
        "instance": inst,
        "qrcode_base64": qr_base64,
        "pairing_code": pairing_code,
        "state": "connecting",
    }


def evolution_create_or_connect_instance(account: Dict) -> Dict:
    """
    Verifica o estado real da instância na Evolution API v2:
    - Se já estiver 'open' (conectada), informa que está conectada.
    - Se não existir ou se o QR Code anterior tiver expirado, recria a instância automaticamente.
    """
    inst = account.get("instance_name") or ("chip_admin" if account.get("id") == "conta_admin" else f"chip_{account.get('slot', 1)}")
    base = _evo_base_url()
    headers = _evo_headers(account)

    try:
        status_check = evolution_check_connection(account)
        if not status_check.get("online"):
            return {
                "ok": False,
                "online": False,
                "instance": inst,
                "error": (
                    f"Evolution API não encontrada em {base}. "
                    "Suba o container com: docker compose -f assistant-to-the-lead-manager/docker-compose.evolution.yml up -d"
                ),
            }

        real_state = str(status_check.get("state") or "close").lower()
        if real_state == "open":
            return {
                "ok": True,
                "instance": inst,
                "qrcode_base64": "",
                "pairing_code": "",
                "state": "open",
            }

        if real_state == "not_created":
            return _create_fresh_evolution_instance(base, inst, headers, account)

        connect_resp = requests.get(f"{base}/instance/connect/{inst}", headers=headers, timeout=8)
        if connect_resp.ok and connect_resp.content:
            cdata = connect_resp.json() or {}
            qr_base64 = cdata.get("base64") or (cdata.get("qrcode") or {}).get("base64") or ""
            pairing_code = cdata.get("pairingCode") or (cdata.get("qrcode") or {}).get("pairingCode") or ""
            if qr_base64:
                return {
                    "ok": True,
                    "instance": inst,
                    "qrcode_base64": qr_base64,
                    "pairing_code": pairing_code,
                    "state": "connecting",
                }

        return _create_fresh_evolution_instance(base, inst, headers, account)
    except Exception as e:
        return {
            "ok": False,
            "online": False,
            "instance": inst,
            "error": f"Evolution API offline em {base}.",
            "details": str(e),
        }


def evolution_logout_instance(account: Dict) -> Dict:
    """Desconecta (logout) uma instância na Evolution API."""
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
    base = _evo_base_url()
    try:
        resp = requests.delete(f"{base}/instance/logout/{inst}", headers=_evo_headers(account), timeout=6)
        return {"ok": resp.ok, "instance": inst, "status_code": resp.status_code}
    except Exception as e:
        return {"ok": False, "instance": inst, "error": str(e)}


def get_active_accounts(selected_account_id: str = "todas_ativas", dry_run: bool = True) -> List[Dict]:
    """
    Retorna a lista de instâncias da Evolution API aptas para PROSPECÇÃO (entre os 50 chips).
    - NUNCA inclui a 'conta_admin' (pois ela apenas administra e encaminha leads!).
    - NUNCA inclui chips que estejam com Timer de Restrição / Quarentena ativo (conta_em_quarentena > 0).
    - Em modo LIVE (dry_run=False), exige que a instância esteja com sessão 'open' na Evolution API.
    """
    todas = [
        c for c in load_whatsapp_accounts()
        if not c.get("admin_only") and c.get("id") != "conta_admin"
    ]

    if selected_account_id and selected_account_id not in ("todas_ativas", "all", "todas"):
        filtradas = [
            c for c in todas
            if c.get("id") == selected_account_id
            or str(c.get("slot")) == str(selected_account_id)
            or c.get("instance_name") == selected_account_id
        ]
    else:
        filtradas = [c for c in todas if c.get("ativo", False)]

    # Remove chips em quarentena/cooldown ativo
    filtradas = [c for c in filtradas if conta_em_quarentena(c) == 0]

    if not dry_run:
        conectadas = []
        for c in filtradas:
            if not str(c.get("instance_name", "")).strip():
                continue
            chk = evolution_check_connection(c)
            if str(chk.get("state", "")).lower() == "open":
                conectadas.append(c)
        filtradas = conectadas
    return intercalar_contas_por_aparelho(filtradas)


def registrar_disparo_conta(account_id: str) -> None:
    """Incrementa o contador diário de mensagens enviadas pela instância especificada."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    contas = load_whatsapp_accounts()
    for c in contas:
        if c.get("id") == account_id or c.get("instance_name") == account_id:
            if c.get("data_contagem") != hoje:
                c["enviados_hoje"] = 0
                c["erros_hoje"] = 0
                c["data_contagem"] = hoje
            c["enviados_hoje"] = int(c.get("enviados_hoje", 0)) + 1
            c["ultimo_disparo_em"] = datetime.now().isoformat()
            break
    save_whatsapp_accounts(contas)


def resolver_conta_whatsapp(account_id_or_instance: Optional[str] = None) -> Dict:
    """
    Localiza qualquer conta (incluindo conta_admin ou conta_1..conta_50) pelo id, slot ou instance_name.
    """
    contas = load_whatsapp_accounts()
    chave = str(account_id_or_instance or "").strip()
    if chave:
        for c in contas:
            if (
                c.get("id") == chave
                or str(c.get("slot")) == chave
                or str(c.get("instance_name", "")).lower() == chave.lower()
                or (c.get("telefone_exibicao") and normalize_phone(c.get("telefone_exibicao", "")) == normalize_phone(chave))
            ):
                return c
    prospeccao = [c for c in contas if not c.get("admin_only") and c.get("id") != "conta_admin"]
    ativas = [c for c in prospeccao if c.get("ativo")]
    return ativas[0] if ativas else (prospeccao[0] if prospeccao else contas[0])


def send_whatsapp_message(
    to_number: str,
    message_body: Optional[str] = None,
    is_template: bool = False,
    template_name: Optional[str] = None,
    components: Optional[List[Dict]] = None,
    dry_run: bool = DRY_RUN,
    force_outside_hours: bool = False,
    account: Optional[Dict] = None,
    lead_name: str = "",
) -> Optional[Dict]:
    """
    Envia mensagem de prospecção via Evolution API (/message/sendText/{instance}).
    Bloqueia automaticamente caso a conta seja 'conta_admin' (que não pode prospectar!).
    """
    formatted_number = normalize_phone(to_number)
    if not formatted_number:
        return None

    if not dry_run and not force_outside_hours and not is_business_hours():
        return None

    conta_ativa = account or resolver_conta_whatsapp()
    if conta_ativa.get("admin_only") or conta_ativa.get("id") == "conta_admin":
        print("[PROTEÇÃO ADMIN] A Conta Principal Administradora não realiza prospecção ativa!")
        return {"error": "admin_account_cannot_prospect", "fatal_connection": False}

    conta_id = conta_ativa.get("id", "conta_1")
    conta_nome = conta_ativa.get("nome", conta_id)
    instance_name = conta_ativa.get("instance_name") or f"chip_{conta_ativa.get('slot', 1)}"

    # Verifica se o chip está em Quarentena / Cooldown
    seg_restantes = conta_em_quarentena(conta_ativa)
    if seg_restantes > 0:
        return {
            "error": "chip_em_quarentena",
            "cooldown_seconds": seg_restantes,
            "fatal_connection": True,
            "account_id": conta_id,
            "instance_name": instance_name,
        }

    # TRAVA 1: Verifica no Cofre Permanente Anti-Reenvio (DDD + 8 dígitos)
    if phone_dedup_key(formatted_number) in carregar_historico_permanente():
        return {
            "error": "ja_enviado_historico",
            "fatal_connection": False,
            "account_id": conta_id,
            "instance_name": instance_name,
        }

    # TRAVA 2: Em modo Real (LIVE), pergunta ao próprio chip na Evolution API se já existe conversa com esse número!
    if not dry_run and evolution_ja_tem_conversa_no_chip(conta_ativa, formatted_number):
        registrar_historico_permanente(formatted_number)
        return {
            "error": "ja_conversado_no_chip",
            "fatal_connection": False,
            "account_id": conta_id,
            "instance_name": instance_name,
        }

    texto_final = formatar_mensagem_lead(message_body or MENSAGEM_PADRAO_PROSPECCAO, lead_name)

    if dry_run:
        return {
            "messages": [{"id": f"evo_dry_{instance_name}_{int(time.time() * 1000)}"}],
            "account_id": conta_id,
            "account_name": conta_nome,
            "instance_name": instance_name,
        }

    base = _evo_base_url()
    url = f"{base}/message/sendText/{instance_name}"
    headers = _evo_headers(conta_ativa)

    typing_delay = min(4500, max(TYPING_MS, len(texto_final) * 25))
    payload = {
        "number": formatted_number,
        "text": texto_final,
        "delay": typing_delay,
        "linkPreview": False,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        if response.status_code == 400 and ("exists" in response.text.lower() or "not found" in response.text.lower()):
            return {
                "error": "sem_whatsapp",
                "fatal_connection": False,
                "account_id": conta_id,
                "instance_name": instance_name,
            }
        if response.status_code >= 400:
            detalhe = response.text[:200] if response.text else f"HTTP {response.status_code}"
            reg_restricao = aplicar_quarentena_conta(conta_id, f"HTTP_{response.status_code}", detalhe, formatted_number)
            return {
                "error": f"http_{response.status_code}",
                "detail": detalhe,
                "restriction_info": reg_restricao,
                "fatal_connection": True,
                "account_id": conta_id,
                "instance_name": instance_name,
            }
        data = response.json() if response.content else {}
        msg_id = (
            data.get("key", {}).get("id")
            or data.get("message", {}).get("key", {}).get("id")
            or f"evo_{instance_name}_{int(time.time() * 1000)}"
        )
        registrar_historico_permanente(formatted_number)
        return {
            "messages": [{"id": msg_id}],
            "account_id": conta_id,
            "account_name": conta_nome,
            "instance_name": instance_name,
            "raw": data,
        }
    except requests.exceptions.RequestException as e:
        reg_restricao = aplicar_quarentena_conta(conta_id, "CONNECTION_ERROR", str(e), formatted_number)
        return {
            "error": "connection_exception",
            "detail": str(e),
            "restriction_info": reg_restricao,
            "fatal_connection": True,
            "account_id": conta_id,
            "instance_name": instance_name,
        }


STATUS_PRIORITY = {
    "novo": 0,
    "pendente": 0,
    "": 0,
    "sem_whatsapp": 1,
    "erro": 1,
    "enviado": 2,
    "respondeu": 3,
    "interessado": 4,
    "opt_out": 5,
}


def salvar_leads_csv(leads: List[Dict], fieldnames: List[str], allow_downgrade: bool = False) -> None:
    """
    Salva a lista completa de leads de forma atômica e segura contra condições de corrida (Race Condition):
    - Sob proteção de CSV_LOCK, verifica o estado atual no disco antes de gravar e NUNCA rebaixa um lead
      que já tenha respondido via Webhook ('interessado', 'respondeu', 'opt_out') de volta para 'enviado' ou 'novo',
      a menos que allow_downgrade=True (edição manual explícita pelo usuário).
    """
    with CSV_LOCK:
        colunas_finais = list(fieldnames)
        for col in COLUNAS_PADRAO:
            if col not in colunas_finais:
                colunas_finais.append(col)

        if not allow_downgrade and os.path.exists(LEADS_FILE):
            try:
                atuais_por_chave: Dict[str, Dict] = {}
                with open(LEADS_FILE, mode="r", encoding="utf-8", errors="replace") as f_cur:
                    for r_cur in csv.DictReader(f_cur):
                        r_cur.pop(None, None)
                        k = phone_dedup_key(r_cur.get("telefone", ""))
                        if k:
                            atuais_por_chave[k] = r_cur
                for row in leads:
                    k = phone_dedup_key(row.get("telefone", ""))
                    row_disco = atuais_por_chave.get(k) if k else None
                    if not row_disco:
                        continue
                    st_mem = str(row.get("status", "novo")).strip().lower()
                    st_dsk = str(row_disco.get("status", "novo")).strip().lower()
                    if STATUS_PRIORITY.get(st_dsk, 0) > STATUS_PRIORITY.get(st_mem, 0):
                        row["status"] = row_disco.get("status", st_dsk)
                        if row_disco.get("opt_out") == "true":
                            row["opt_out"] = "true"
                    if row_disco.get("ultima_resposta") and not row.get("ultima_resposta"):
                        row["ultima_resposta"] = row_disco["ultima_resposta"]
                    if row_disco.get("nome") and not str(row_disco.get("nome")).startswith("Lead ") and str(row.get("nome", "")).startswith("Lead "):
                        row["nome"] = row_disco["nome"]
                    if row_disco.get("ultima_interacao_em", "") > str(row.get("ultima_interacao_em", "")):
                        row["ultima_interacao_em"] = row_disco["ultima_interacao_em"]
                    if row_disco.get("conta_id") and st_dsk in ("interessado", "respondeu", "opt_out"):
                        row["conta_id"] = row_disco["conta_id"]
            except Exception:
                pass

        arquivo_temp = f"{LEADS_FILE}.tmp"
        with open(arquivo_temp, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=colunas_finais, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(leads)
        os.replace(arquivo_temp, LEADS_FILE)


def atualizar_lead_pos_disparo(
    telefone: str,
    conta_id: str,
    message_id: str = "",
    status_novo: str = "enviado",
) -> bool:
    """
    Atualiza cirurgicamente apenas a linha do lead disparado lendo o CSV fresco do disco sob CSV_LOCK,
    impedindo que um disparo em lote sobrescreva respostas de Webhook recebidas durante o intervalo Anti-Ban.
    """
    with CSV_LOCK:
        if not os.path.exists(LEADS_FILE):
            return False
        leads = []
        updated = False
        tel_norm = normalize_phone(telefone)
        with open(LEADS_FILE, mode="r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
            for row in reader:
                row.pop(None, None)
                if phones_match(row.get("telefone", ""), tel_norm):
                    st_atual = str(row.get("status", "novo")).strip().lower()
                    if STATUS_PRIORITY.get(status_novo, 2) >= STATUS_PRIORITY.get(st_atual, 0):
                        row["status"] = status_novo
                    if conta_id:
                        row["conta_id"] = conta_id
                    if message_id:
                        row["message_id"] = message_id
                    row["ultima_interacao_em"] = datetime.now().isoformat()
                    updated = True
                leads.append(row)
        if updated:
            salvar_leads_csv(leads, fieldnames, allow_downgrade=False)
        return updated


def reconciliar_eventos_no_csv(events_log_path: Optional[str] = None) -> int:
    """
    Reconcilia o arquivo de auditoria data/events.log com o CSV de leads, restaurando automaticamente
    qualquer resposta ('interessado', 'respondeu', 'opt_out') ou envio que tenha sido sobrescrito.
    """
    caminho_log = events_log_path or os.path.join(BASE_DIR, "data/events.log")
    if not os.path.exists(caminho_log) or not os.path.exists(LEADS_FILE):
        return 0

    with CSV_LOCK:
        eventos_por_chave: Dict[str, Dict] = {}
        try:
            with open(caminho_log, mode="r", encoding="utf-8", errors="replace") as fl:
                for line in fl:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue
                    ev_type = ev.get("type", "")
                    phone = normalize_phone(ev.get("phone", ""))
                    if not phone:
                        continue
                    k = phone_dedup_key(phone)
                    payload = ev.get("payload") or {}
                    ts = ev.get("timestamp", "")
                    atual = eventos_por_chave.get(k, {
                        "phone": phone,
                        "status": "novo",
                        "ultima_resposta": "",
                        "profile_name": "",
                        "conta_id": "",
                        "message_id": "",
                        "timestamp": "",
                    })
                    if ev_type == "campaign_sent":
                        if STATUS_PRIORITY.get("enviado", 2) > STATUS_PRIORITY.get(atual["status"], 0):
                            atual["status"] = "enviado"
                        if payload.get("account_id"):
                            atual["conta_id"] = payload["account_id"]
                        if payload.get("message_id"):
                            atual["message_id"] = payload["message_id"]
                        if ts > atual["timestamp"]:
                            atual["timestamp"] = ts
                    elif ev_type == "message_received":
                        new_st = str(payload.get("new_status", "respondeu")).strip().lower()
                        if STATUS_PRIORITY.get(new_st, 3) >= STATUS_PRIORITY.get(atual["status"], 0):
                            atual["status"] = new_st
                        if payload.get("text"):
                            atual["ultima_resposta"] = str(payload["text"])
                        if payload.get("profile_name"):
                            atual["profile_name"] = str(payload["profile_name"])
                        if payload.get("account_id"):
                            atual["conta_id"] = str(payload["account_id"])
                        if ts > atual["timestamp"]:
                            atual["timestamp"] = ts
                    elif ev_type == "manual_status_change":
                        new_st = str(payload.get("new_status", "")).strip().lower()
                        if new_st:
                            atual["status"] = new_st
                        if ts > atual["timestamp"]:
                            atual["timestamp"] = ts
                    elif ev_type == "lead_forwarded":
                        cons_nome = str(payload.get("consultant_name") or payload.get("consultant_phone") or "").strip()
                        if cons_nome:
                            atual["encaminhado_para"] = cons_nome
                            atual["encaminhado_em"] = ts
                    eventos_por_chave[k] = atual
        except Exception:
            return 0

        if not eventos_por_chave:
            return 0

        leads = []
        restaurados = 0
        chaves_enviadas: set = set()
        with open(LEADS_FILE, mode="r", encoding="utf-8", errors="replace") as fc:
            reader = csv.DictReader(fc)
            fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
            for row in reader:
                row.pop(None, None)
                tel = normalize_phone(row.get("telefone", ""))
                row["telefone"] = tel
                k = phone_dedup_key(tel)
                ev_info = eventos_por_chave.get(k)
                if ev_info:
                    st_csv = str(row.get("status", "novo")).strip().lower()
                    st_ev = ev_info["status"]
                    mudou = False
                    if STATUS_PRIORITY.get(st_ev, 0) > STATUS_PRIORITY.get(st_csv, 0):
                        row["status"] = st_ev
                        if st_ev == "opt_out":
                            row["opt_out"] = "true"
                        mudou = True
                    if ev_info["ultima_resposta"] and row.get("ultima_resposta") != ev_info["ultima_resposta"]:
                        row["ultima_resposta"] = ev_info["ultima_resposta"]
                        mudou = True
                    if ev_info["profile_name"] and (not row.get("nome") or str(row.get("nome", "")).startswith("Lead ")):
                        row["nome"] = ev_info["profile_name"]
                        mudou = True
                    if ev_info["conta_id"] and (not row.get("conta_id") or st_ev in ("interessado", "respondeu", "opt_out")):
                        row["conta_id"] = ev_info["conta_id"]
                        mudou = True
                    if ev_info.get("encaminhado_para") and not row.get("encaminhado_para"):
                        row["encaminhado_para"] = ev_info["encaminhado_para"]
                        row["encaminhado_em"] = ev_info.get("encaminhado_em", "")
                        mudou = True
                    if ev_info["timestamp"] and ev_info["timestamp"] > str(row.get("ultima_interacao_em", "")):
                        row["ultima_interacao_em"] = ev_info["timestamp"]
                    if mudou:
                        restaurados += 1
                st_final = str(row.get("status", "novo")).strip().lower()
                if tel and st_final in ("enviado", "interessado", "respondeu", "opt_out"):
                    chaves_enviadas.add(phone_dedup_key(tel))
                leads.append(row)

        if restaurados > 0:
            salvar_leads_csv(leads, fieldnames, allow_downgrade=True)

        # Garante que o Cofre Permanente Anti-Reenvio contenha 100% dos números já abordados
        try:
            existentes = carregar_historico_permanente()
            for k_ev, ev_inf in eventos_por_chave.items():
                if ev_inf.get("status") in ("enviado", "interessado", "respondeu", "opt_out"):
                    chaves_enviadas.add(k_ev)
            faltantes = chaves_enviadas - existentes
            if faltantes:
                os.makedirs(os.path.dirname(HISTORICO_ENVIADOS_FILE) or ".", exist_ok=True)
                with open(HISTORICO_ENVIADOS_FILE, "a", encoding="utf-8") as fh:
                    for fk in sorted(faltantes):
                        if fk:
                            fh.write(f"{fk}\n")
        except Exception:
            pass

        return restaurados


def registrar_encaminhamento_permanente(registro: Dict) -> None:
    """Grava cada encaminhamento (automático ou manual) no histórico permanente JSONL."""
    try:
        os.makedirs(os.path.dirname(ENCAMINHAMENTOS_FILE) or ".", exist_ok=True)
        item = dict(registro)
        item.setdefault("timestamp", datetime.now().isoformat())
        with CSV_LOCK:
            with open(ENCAMINHAMENTOS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
    except Exception:
        pass


def ler_encaminhamentos_permanente(limit: int = 100) -> List[Dict]:
    """Lê os últimos encaminhamentos salvos em data/encaminhamentos_permanente.jsonl."""
    if not os.path.exists(ENCAMINHAMENTOS_FILE):
        return []
    saida: List[Dict] = []
    try:
        with open(ENCAMINHAMENTOS_FILE, "r", encoding="utf-8", errors="replace") as f:
            linhas = f.readlines()[-limit:]
            for ln in reversed(linhas):
                ln = ln.strip()
                if ln:
                    try:
                        saida.append(json.loads(ln))
                    except Exception:
                        pass
    except Exception:
        pass
    return saida


def update_lead_status(
    lead_id: str,
    status: str,
    message_id: Optional[str] = None,
    last_interaction: Optional[str] = None,
    conta_id: Optional[str] = None,
) -> bool:
    """Atualiza o status de um lead específico diretamente no arquivo CSV."""
    if not os.path.exists(LEADS_FILE):
        return False

    leads = []
    updated = False
    with open(LEADS_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
        for row in reader:
            row.pop(None, None)
            if str(row.get("id")) == str(lead_id):
                row["status"] = status
                row["telefone"] = normalize_phone(row.get("telefone", ""))
                if message_id:
                    row["message_id"] = message_id
                if last_interaction:
                    row["ultima_interacao_em"] = last_interaction
                if conta_id:
                    row["conta_id"] = conta_id
                updated = True
            leads.append(row)

    if updated:
        salvar_leads_csv(leads, fieldnames)
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Disparador Multi-Instância Evolution API (Até 50 chips simultâneos)")
    parser.add_argument("--dry-run", action="store_true", help="Executa em modo simulação sem chamar a Evolution API real")
    parser.add_argument("--limit", type=int, default=5, help="Limite total de envios nesta execução")
    parser.add_argument("--force-hours", action="store_true", help="Ignora trava de horário comercial")
    parser.add_argument(
        "--account",
        type=str,
        default="todas_ativas",
        help="ID da conta ('conta_1'..'conta_50') ou 'todas_ativas' para Round-Robin",
    )
    args = parser.parse_args()

    modo_simulacao = DRY_RUN or args.dry_run
    limite_execucao = args.limit

    contas_disponiveis = get_active_accounts(args.account, dry_run=modo_simulacao)
    if not contas_disponiveis:
        print("Error: Nenhuma instância de prospecção ativa/conectada disponível.")
        return

    if not os.path.exists(LEADS_FILE):
        print(f"Error: Leads file not found at {LEADS_FILE}")
        return

    leads: List[Dict] = []
    with open(LEADS_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
        for row in reader:
            row.pop(None, None)
            row["telefone"] = normalize_phone(row.get("telefone", ""))
            leads.append(row)

    sent_count = 0
    attempts = 0
    por_conta: Dict[str, int] = {c["id"]: 0 for c in contas_disponiveis}
    rr_idx = 0

    for lead in leads:
        if sent_count >= limite_execucao or attempts >= limite_execucao or not contas_disponiveis:
            break

        opt_out_val = str(lead.get("opt_out", "false")).strip().lower()
        status_atual = str(lead.get("status", "novo")).strip().lower()
        if opt_out_val == "true" or status_atual != "novo":
            continue

        telefone_destino = lead.get("telefone", "")
        if not telefone_destino:
            continue

        conta_escolhida = None
        for _ in range(len(contas_disponiveis)):
            candidata = contas_disponiveis[rr_idx % len(contas_disponiveis)]
            rr_idx += 1
            if int(candidata.get("enviados_hoje", 0)) < int(candidata.get("daily_limit", DAILY_MESSAGE_LIMIT)):
                conta_escolhida = candidata
                break

        if not conta_escolhida:
            break

        response = send_whatsapp_message(
            telefone_destino,
            MENSAGEM_PADRAO_PROSPECCAO,
            dry_run=modo_simulacao,
            force_outside_hours=args.force_hours,
            account=conta_escolhida,
            lead_name=lead.get("nome", ""),
        )
        if response and response.get("error") in ("ja_enviado_historico", "ja_conversado_no_chip"):
            lead["status"] = "enviado"
            lead["conta_id"] = conta_escolhida["id"]
            salvar_leads_csv(leads, fieldnames)
            continue

        attempts += 1
        if response and "messages" in response:
            cid = conta_escolhida["id"]
            lead["status"] = "enviado"
            lead["conta_id"] = cid
            lead["message_id"] = response["messages"][0].get("id", "")
            lead["ultima_interacao_em"] = datetime.now().isoformat()
            sent_count += 1
            por_conta[cid] = por_conta.get(cid, 0) + 1
            conta_escolhida["enviados_hoje"] = int(conta_escolhida.get("enviados_hoje", 0)) + 1
            registrar_disparo_conta(cid)
            salvar_leads_csv(leads, fieldnames)
        elif response and response.get("fatal_connection"):
            contas_disponiveis = [c for c in contas_disponiveis if c["id"] != conta_escolhida["id"]]
            if not contas_disponiveis:
                break

        if sent_count < limite_execucao and attempts < limite_execucao and contas_disponiveis:
            time.sleep(0.05 if modo_simulacao else random.uniform(MIN_DELAY, max(MIN_DELAY, MAX_DELAY)))

    salvar_leads_csv(leads, fieldnames)
    print(f"Finished: {sent_count} sent | Per account: {por_conta}")


if __name__ == "__main__":
    main()
