#!/usr/bin/env python3
"""
Disparador de Campanhas WhatsApp via Evolution API (Multi-Instância: até 5 chips simultâneos)
Com normalização DDI +55, simulação de digitação ("Digitando..."), delay aleatório anti-ban (Jitter),
balanceamento Round-Robin entre as 5 instâncias, persistência atômica no CSV (.tmp) e proteção anti-reenvio.
"""

import argparse
import csv
import json
import os
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

MAX_WHATSAPP_ACCOUNTS = 5

# Configuração Global da Evolution API
EVOLUTION_API_URL = os.getenv("EVOLUTION_API_URL", "http://localhost:8081").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "lead_manager_evo_key_2026")
WEBHOOK_INTERNAL_URL = os.getenv("WEBHOOK_INTERNAL_URL", "http://host.docker.internal:5000/webhook")

# Campaign & Anti-Ban Settings
DEFAULT_DELAY = float(os.getenv("DEFAULT_DELAY", 8.0))
MIN_DELAY = float(os.getenv("MIN_DELAY", 6.0))
MAX_DELAY = float(os.getenv("MAX_DELAY", 15.0))
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
]


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


def carregar_historico_permanente() -> set:
    """
    Carrega o cofre permanente de números que já receberam mensagens (ou já interagiram),
    cruzando data/historico_enviados_permanente.txt, data/events.log e o CSV atual.
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

    # Cruza também com events.log para garantir que nenhum envio anterior seja esquecido
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


def evolution_ja_tem_conversa_no_chip(account: Dict, formatted_number: str) -> bool:
    """
    Consulta diretamente a Evolution API (/chat/findMessages/{instance}) para verificar
    se este chip JÁ conversou ou JÁ enviou mensagem para este número anteriormente no aparelho.
    Checa tanto a versão com 9º dígito quanto sem 9º dígito!
    """
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
    base = _evo_base_url()
    headers = _evo_headers(account)

    variantes = {formatted_number}
    if len(formatted_number) == 13 and formatted_number.startswith("55") and formatted_number[4] == "9":
        # Versão sem o 9º dígito (12 dígitos)
        variantes.add(formatted_number[:4] + formatted_number[5:])
    elif len(formatted_number) == 12 and formatted_number.startswith("55"):
        # Versão com o 9º dígito (13 dígitos)
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
                # Evolution v2 retorna {"messages": {"records": [...], "total": N}} ou lista
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


def is_business_hours() -> bool:
    """Verifica se o horário atual está dentro da janela comercial configurada."""
    now = datetime.now()
    if BUSINESS_DAYS_ONLY and now.weekday() > 4:  # 0 = Segunda, 6 = Domingo
        return False
    return BUSINESS_START_HOUR <= now.hour < BUSINESS_END_HOUR


SAUDACOES_VARIADAS = [
    "Olá, tudo bem?",
    "Oi, tudo bem?",
    "Olá! Tudo bem por aí?",
    "Oi! Tudo certo?",
    "Olá, como vai?",
]


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

    # Processa Spintax do tipo {Olá|Oi|Bom dia}
    def _resolver_spintax(match: re.Match) -> str:
        opcoes = [op.strip() for op in match.group(1).split("|") if op.strip()]
        return random.choice(opcoes) if opcoes else ""

    texto = re.sub(r"\{([^{}]*\|[^{}]*)\}", _resolver_spintax, texto)

    # Se o texto começar com "Olá, tudo bem?", alterna automaticamente entre saudações humanas
    if usar_variacao and texto.startswith("Olá, tudo bem?"):
        saudacao = random.choice(SAUDACOES_VARIADAS)
        texto = saudacao + texto[len("Olá, tudo bem?"):]

    return texto


def _default_accounts() -> List[Dict]:
    """Gera a estrutura padrão das 5 instâncias simultâneas da Evolution API."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    contas: List[Dict] = []
    for idx in range(1, MAX_WHATSAPP_ACCOUNTS + 1):
        prefix = f"WHATSAPP_ACCOUNT_{idx}_"
        instance_name = os.getenv(f"{prefix}INSTANCE", f"chip_{idx}")
        consultant = os.getenv(f"{prefix}CONSULTANT_PHONE") or os.getenv("CONSULTANT_PHONE_NUMBER", "")
        display_phone = os.getenv(f"{prefix}DISPLAY_PHONE", "")
        nome = os.getenv(f"{prefix}NAME", f"Chip WhatsApp #{idx}" + (" (Principal)" if idx == 1 else ""))
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
            "nome": nome,
            "instance_name": instance_name,
            "ativo": ativo,
            "connection_status": "close",
            "telefone_exibicao": normalize_phone(display_phone) if display_phone else "",
            "consultant_phone": normalize_phone(consultant) if consultant else "",
            "apikey": instance_token,
            "daily_limit": limit,
            "enviados_hoje": 0,
            "data_contagem": hoje,
            "ultimo_disparo_em": "",
        })
    return contas


def load_whatsapp_accounts() -> List[Dict]:
    """Carrega as 5 instâncias do WhatsApp a partir de data/whatsapp_accounts.json."""
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

        mapa_salvas = {str(c.get("id") or f"conta_{c.get('slot', i+1)}"): c for i, c in enumerate(salvas)}
        resultado: List[Dict] = []
        for item_padrao in padrao:
            cid = item_padrao["id"]
            slot = item_padrao["slot"]
            if cid in mapa_salvas:
                merged = dict(item_padrao)
                merged.update(mapa_salvas[cid])
                merged["id"] = cid
                merged["slot"] = slot
                if not merged.get("instance_name"):
                    merged["instance_name"] = f"chip_{slot}"
                if str(merged.get("nome", "")).startswith("Conta WhatsApp #"):
                    merged["nome"] = merged["nome"].replace("Conta WhatsApp #", "Chip WhatsApp #")
                if merged.get("data_contagem") != hoje:
                    merged["enviados_hoje"] = 0
                    merged["data_contagem"] = hoje
                resultado.append(merged)
            else:
                resultado.append(item_padrao)
        return resultado[:MAX_WHATSAPP_ACCOUNTS]
    except Exception:
        return padrao


def save_whatsapp_accounts(accounts: List[Dict]) -> None:
    """Salva as até 5 instâncias simultâneas de forma atômica em data/whatsapp_accounts.json."""
    os.makedirs(os.path.dirname(ACCOUNTS_FILE) or ".", exist_ok=True)
    normalizadas = accounts[:MAX_WHATSAPP_ACCOUNTS]
    temp_file = f"{ACCOUNTS_FILE}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(normalizadas, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, ACCOUNTS_FILE)


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


def evolution_check_connection(account: Dict) -> Dict:
    """Consulta o status real da instância na Evolution API (/instance/connectionState/{instance})."""
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
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

    # Aguarda o Baileys gerar o QR Code (até 5 tentativas de 1s)
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
    - Se não existir ou se o QR Code anterior tiver expirado ('close' / sem base64), recria a instância
      automaticamente para sempre devolver um QR Code Base64 fresquinho na tela.
    """
    inst = account.get("instance_name") or f"chip_{account.get('slot', 1)}"
    base = _evo_base_url()
    headers = _evo_headers(account)

    try:
        # 1. Consulta o estado real (/instance/connectionState/{inst})
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

        # 2. Se a instância existe mas não está 'open', tenta obter o QR Code atual
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

        # 3. Se não veio QR Code (ex: QR Code anterior expirou após 30s ou sofreu logout), recria a instância!
        return _create_fresh_evolution_instance(base, inst, headers, account)
    except Exception as e:
        return {
            "ok": False,
            "online": False,
            "instance": inst,
            "error": (
                f"Evolution API não encontrada em {base}. "
                "Suba o container com: docker compose -f docker-compose.evolution.yml up -d"
            ),
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
    Retorna a lista de instâncias da Evolution API aptas para envio (entre as 5 contas suportadas).
    - Se selected_account_id for 'conta_1'..'conta_5' (ou 'chip_1'..'chip_5'), retorna aquela instância.
    - Se for 'todas_ativas' (ou 'all'), retorna todas as instâncias marcadas como ativo=True.
    """
    todas = load_whatsapp_accounts()
    if selected_account_id and selected_account_id not in ("todas_ativas", "all", "todas"):
        filtradas = [
            c for c in todas
            if c.get("id") == selected_account_id
            or str(c.get("slot")) == str(selected_account_id)
            or c.get("instance_name") == selected_account_id
        ]
    else:
        filtradas = [c for c in todas if c.get("ativo", False)]

    if not filtradas and todas:
        filtradas = [todas[0]]

    if not dry_run:
        # CRÍTICO ANTI-BAN: Em modo LIVE, utiliza APENAS instâncias que estejam realmente conectadas ('open') na Evolution API!
        conectadas = []
        for c in filtradas:
            if not str(c.get("instance_name", "")).strip():
                continue
            chk = evolution_check_connection(c)
            if str(chk.get("state", "")).lower() == "open":
                conectadas.append(c)
        filtradas = conectadas
    return filtradas


def registrar_disparo_conta(account_id: str) -> None:
    """Incrementa o contador diário de mensagens enviadas pela instância especificada."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    contas = load_whatsapp_accounts()
    for c in contas:
        if c.get("id") == account_id or c.get("instance_name") == account_id:
            if c.get("data_contagem") != hoje:
                c["enviados_hoje"] = 0
                c["data_contagem"] = hoje
            c["enviados_hoje"] = int(c.get("enviados_hoje", 0)) + 1
            c["ultimo_disparo_em"] = datetime.now().isoformat()
            break
    save_whatsapp_accounts(contas)


def resolver_conta_whatsapp(account_id_or_instance: Optional[str] = None) -> Dict:
    """
    Localiza uma das 5 contas pelo id ('conta_1'..'conta_5'), pelo nome da instância na Evolution API
    ('chip_1'..'chip_5'), ou pelo telefone_exibicao. Se não encontrar, retorna a primeira ativa.
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
    ativas = [c for c in contas if c.get("ativo")]
    return ativas[0] if ativas else contas[0]


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
    Envia mensagem de texto via Evolution API (/message/sendText/{instance})
    com simulação humana de digitação ('composing') ou simula em modo DRY-RUN.
    """
    formatted_number = normalize_phone(to_number)
    if not formatted_number:
        print(f"Skipping invalid phone number: '{to_number}'")
        return None

    if not dry_run and not force_outside_hours and not is_business_hours():
        print(f"Skipping message to {formatted_number}: outside business hours.")
        return None

    conta_ativa = account or resolver_conta_whatsapp()
    conta_id = conta_ativa.get("id", "conta_1")
    conta_nome = conta_ativa.get("nome", conta_id)
    instance_name = conta_ativa.get("instance_name") or f"chip_{conta_ativa.get('slot', 1)}"

    # TRAVA 1: Verifica no Cofre Permanente Anti-Reenvio (DDD + 8 dígitos)
    if phone_dedup_key(formatted_number) in carregar_historico_permanente():
        print(f"[ANTI-REENVIO] Número {formatted_number} já consta no histórico permanente de enviados. Pulando!")
        return {
            "error": "ja_enviado_historico",
            "fatal_connection": False,
            "account_id": conta_id,
            "instance_name": instance_name,
        }

    # TRAVA 2: Em modo Real (LIVE), pergunta ao próprio chip na Evolution API se já existe conversa com esse número!
    if not dry_run and evolution_ja_tem_conversa_no_chip(conta_ativa, formatted_number):
        print(f"[ANTI-REENVIO][Evolution:{instance_name}] Número {formatted_number} JÁ possui conversa no aparelho! Pulando para não repetir.")
        registrar_historico_permanente(formatted_number)
        return {
            "error": "ja_conversado_no_chip",
            "fatal_connection": False,
            "account_id": conta_id,
            "instance_name": instance_name,
        }

    texto_final = formatar_mensagem_lead(message_body or MENSAGEM_PADRAO_PROSPECCAO, lead_name)

    if dry_run:
        resumo = texto_final.splitlines()[0] if texto_final else ""
        print(f"[DRY-RUN][Evolution:{instance_name} ({conta_id})] Sending to {formatted_number}: {resumo}")
        return {
            "messages": [{"id": f"evo_dry_{instance_name}_{int(time.time() * 1000)}"}],
            "account_id": conta_id,
            "account_name": conta_nome,
            "instance_name": instance_name,
        }

    base = _evo_base_url()
    url = f"{base}/message/sendText/{instance_name}"
    headers = _evo_headers(conta_ativa)

    # Payload nativo Evolution API v2
    payload = {
        "number": formatted_number,
        "text": texto_final,
        "delay": TYPING_MS,
        "linkPreview": False,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        if response.status_code == 400 and ("exists" in response.text.lower() or "not found" in response.text.lower()):
            print(f"[Evolution:{instance_name}] Número sem WhatsApp ativo ({formatted_number}). Marcando para pular.")
            return {
                "error": "sem_whatsapp",
                "fatal_connection": False,
                "account_id": conta_id,
                "instance_name": instance_name,
            }
        if response.status_code >= 400:
            detalhe = response.text[:200] if response.text else f"HTTP {response.status_code}"
            print(f"[CIRCUIT-BREAKER][Evolution:{instance_name}] Falha HTTP {response.status_code} ao enviar para {formatted_number}: {detalhe}")
            return {
                "error": f"http_{response.status_code}",
                "detail": detalhe,
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
        print(f"[CIRCUIT-BREAKER] Erro de conexão com Evolution API [{instance_name}] para {formatted_number}: {e}")
        return {
            "error": "connection_exception",
            "detail": str(e),
            "fatal_connection": True,
            "account_id": conta_id,
            "instance_name": instance_name,
        }


def salvar_leads_csv(leads: List[Dict], fieldnames: List[str]) -> None:
    """Salva a lista completa de leads de forma segura garantindo todas as colunas necessárias."""
    colunas_finais = list(fieldnames)
    for col in COLUNAS_PADRAO:
        if col not in colunas_finais:
            colunas_finais.append(col)

    arquivo_temp = f"{LEADS_FILE}.tmp"
    with open(arquivo_temp, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas_finais, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    os.replace(arquivo_temp, LEADS_FILE)


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
    parser = argparse.ArgumentParser(description="Disparador Multi-Instância Evolution API (Até 5 chips simultâneos)")
    parser.add_argument("--dry-run", action="store_true", help="Executa em modo simulação sem chamar a Evolution API real")
    parser.add_argument("--limit", type=int, default=DAILY_MESSAGE_LIMIT, help="Limite total de envios nesta execução")
    parser.add_argument("--force-hours", action="store_true", help="Ignora trava de horário comercial")
    parser.add_argument(
        "--account",
        type=str,
        default="todas_ativas",
        help="ID da conta ('conta_1'..'conta_5' / 'chip_1'..'chip_5') ou 'todas_ativas' para Round-Robin",
    )
    args = parser.parse_args()

    modo_simulacao = DRY_RUN or args.dry_run
    limite_execucao = args.limit

    contas_disponiveis = get_active_accounts(args.account, dry_run=modo_simulacao)
    if not contas_disponiveis:
        print("Error: Nenhuma instância Evolution API ativa. Ative ao menos 1 instância no painel web.")
        return

    if not os.path.exists(LEADS_FILE):
        print(f"Error: Leads file not found at {LEADS_FILE}")
        return

    leads: List[Dict] = []
    with open(LEADS_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
        for row in reader:
            row["telefone"] = normalize_phone(row.get("telefone", ""))
            leads.append(row)

    sent_count = 0
    skipped_already_sent = 0
    skipped_opt_out = 0
    por_conta: Dict[str, int] = {c["id"]: 0 for c in contas_disponiveis}
    rr_idx = 0

    for lead in leads:
        if sent_count >= limite_execucao:
            print(f"Message limit of {limite_execucao} reached. Stopping.")
            break

        opt_out_val = str(lead.get("opt_out", "false")).strip().lower()
        bloqueado_val = str(lead.get("bloqueado_para_envio", "false")).strip().lower()
        status_atual = str(lead.get("status", "novo")).strip().lower()

        if opt_out_val == "true" or bloqueado_val == "true" or status_atual == "opt_out":
            skipped_opt_out += 1
            continue

        if status_atual not in ("novo", "", "pendente"):
            skipped_already_sent += 1
            continue

        telefone_destino = lead.get("telefone", "")
        if not telefone_destino:
            continue

        conta_escolhida = None
        for _ in range(len(contas_disponiveis)):
            candidata = contas_disponiveis[rr_idx % len(contas_disponiveis)]
            rr_idx += 1
            limite_conta = int(candidata.get("daily_limit", DAILY_MESSAGE_LIMIT))
            enviados_conta = int(candidata.get("enviados_hoje", 0))
            if enviados_conta < limite_conta:
                conta_escolhida = candidata
                break

        if not conta_escolhida:
            print("Todas as 5 instâncias selecionadas atingiram seus limites diários de envio (Anti-Ban).")
            break

        response = send_whatsapp_message(
            telefone_destino,
            MENSAGEM_PADRAO_PROSPECCAO,
            dry_run=modo_simulacao,
            force_outside_hours=args.force_hours,
            account=conta_escolhida,
            lead_name=lead.get("nome", ""),
        )

        if response and "messages" in response:
            message_id = response["messages"][0].get("id", "")
            agora_iso = datetime.now().isoformat()
            cid = conta_escolhida["id"]
            lead["status"] = "enviado"
            lead["conta_id"] = cid
            lead["message_id"] = message_id
            lead["ultima_interacao_em"] = agora_iso
            sent_count += 1
            por_conta[cid] = por_conta.get(cid, 0) + 1
            conta_escolhida["enviados_hoje"] = int(conta_escolhida.get("enviados_hoje", 0)) + 1
            registrar_disparo_conta(cid)

            if not modo_simulacao:
                salvar_leads_csv(leads, fieldnames)

            # Delay aleatório anti-ban (Jitter) entre MIN_DELAY e MAX_DELAY
            espera = 0.05 if modo_simulacao else random.uniform(MIN_DELAY, max(MIN_DELAY, MAX_DELAY))
            time.sleep(espera)

    salvar_leads_csv(leads, fieldnames)

    print(
        f"\nCampaign finished via Evolution API ({'DRY-RUN' if modo_simulacao else 'LIVE'}).\n"
        f"  - Sent in this run: {sent_count}\n"
        f"  - Per instance breakdown: {por_conta}\n"
        f"  - Skipped (already processed): {skipped_already_sent}\n"
        f"  - Skipped (opt-out/blocked): {skipped_opt_out}"
    )


if __name__ == "__main__":
    main()
