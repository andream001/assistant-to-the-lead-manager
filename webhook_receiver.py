#!/usr/bin/env python3
"""
Webhook Receiver (Evolution API Multi-Instância: até 5 chips simultâneos do WhatsApp)
Recebe eventos MESSAGES_UPSERT e CONNECTION_UPDATE da Evolution API, identifica qual das 5 instâncias
recebeu a mensagem, responde pelo mesmo chip e notifica o consultor responsável daquela instância.
"""

import csv
import io
import json
import logging
import math
import os
import random
import re
import threading
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from flask import Flask, jsonify, request, send_file
from dotenv import load_dotenv

from send_campaigns import (
    COLUNAS_PADRAO,
    EVOLUTION_API_KEY,
    EVOLUTION_API_URL,
    MAX_WHATSAPP_ACCOUNTS,
    MENSAGEM_PADRAO_PROSPECCAO,
    evolution_check_connection,
    evolution_create_or_connect_instance,
    evolution_logout_instance,
    get_active_accounts,
    is_business_hours,
    load_whatsapp_accounts,
    registrar_disparo_conta,
    resolver_conta_whatsapp,
    salvar_leads_csv,
    save_whatsapp_accounts,
    send_whatsapp_message,
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

app = Flask(__name__)
CAMPANHA_LOCK = threading.Lock()
STOP_CAMPANHA_FLAG = False

BASE_DIR = Path(__file__).resolve().parent
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "seu_token_secreto")
CONSULTANT_PHONE_NUMBER = os.getenv("CONSULTANT_PHONE_NUMBER", "")
_default_leads_rel = "data/leads.local.csv" if (BASE_DIR / "data/leads.local.csv").exists() else "data/leads.csv"
LEADS_CSV = str(BASE_DIR / os.getenv("LEADS_FILE", _default_leads_rel))
EVENTS_LOG = str(BASE_DIR / os.getenv("EVENTS_LOG", "data/events.log"))

RESPOSTA_INTERESSADO = (
    "Perfeito! Vou registrar seu interesse e repassar agora mesmo ao consultor responsável para continuidade do atendimento."
)
RESPOSTA_OPT_OUT = (
    "Entendido. Você não receberá mais mensagens deste canal."
)
RESPOSTA_LIVRE = (
    "Obrigado pelo retorno! Vou repassar sua mensagem ao consultor responsável. Este número é um canal exclusivo de triagem inicial."
)


def normalize_phone(phone: str) -> str:
    """Remove sufixos JID (@s.whatsapp.net) e garante o DDI 55 (Brasil) para números de 10 ou 11 dígitos."""
    raw = str(phone or "").split("@")[0]
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if len(digits) in (10, 11) and not digits.startswith("55"):
        return f"55{digits}"
    return digits


def phones_match(phone_a: str, phone_b: str) -> bool:
    """Compara dois números ignorando formatação, DDI 55 e o nono dígito opcional."""
    na = normalize_phone(phone_a)
    nb = normalize_phone(phone_b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Compara últimos 8 dígitos + DDD (caso o WhatsApp remova ou adicione o 9º dígito)
    if len(na) >= 12 and len(nb) >= 12 and na[:4] == nb[:4] and na[-8:] == nb[-8:]:
        return True
    return False


def descobrir_conta_do_lead(phone_number: str, incoming_instance_or_id: Optional[str] = None) -> Dict:
    """
    Descobre qual das 5 instâncias Evolution API está associada a este lead:
    1. Se o webhook da Evolution informou `instance` (ex: 'chip_2') ou `conta_id`, resolve direto.
    2. Caso contrário, verifica na planilha `data/leads.csv` qual `conta_id` enviou para este telefone.
    3. Fallback: primeira instância ativa.
    """
    if incoming_instance_or_id:
        return resolver_conta_whatsapp(incoming_instance_or_id)

    if os.path.exists(LEADS_CSV):
        try:
            with open(LEADS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if phones_match(row.get("telefone", ""), phone_number):
                        cid = (row.get("conta_id") or "").strip()
                        if cid:
                            return resolver_conta_whatsapp(cid)
        except Exception:
            pass

    return resolver_conta_whatsapp()


def send_whatsapp_text(
    to_number: str,
    message_body: str,
    account: Optional[Dict] = None,
) -> Optional[Dict]:
    """Envia uma mensagem de texto via Evolution API (/message/sendText/{instance})."""
    dest = normalize_phone(to_number)
    conta = account or resolver_conta_whatsapp()
    conta_id = conta.get("id", "conta_1")
    instance_name = conta.get("instance_name") or f"chip_{conta.get('slot', 1)}"

    if not dest:
        logger.warning(f"[{conta_id}:{instance_name}] Número inválido ({to_number}).")
        return None

    base_url = os.getenv("EVOLUTION_API_URL", EVOLUTION_API_URL).rstrip("/")
    api_key = conta.get("apikey") or os.getenv("EVOLUTION_API_KEY", EVOLUTION_API_KEY)
    url = f"{base_url}/message/sendText/{instance_name}"

    headers = {
        "apikey": api_key,
        "Content-Type": "application/json",
    }

    payload = {
        "number": dest,
        "text": message_body,
        "options": {
            "delay": 900,
            "presence": "composing",
            "linkPreview": False,
        },
        "textMessage": {
            "text": message_body,
        },
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        logger.info(f"[Evolution:{instance_name}] Mensagem enviada com sucesso para {dest}")
        return response.json() if response.content else {"status": "sent"}
    except Exception as e:
        logger.warning(f"[Evolution:{instance_name}] Evolution API offline ou erro ao enviar para {dest}: {e}")
        return None


def update_lead_status(
    phone_number: str,
    status: str,
    response_text: Optional[str] = None,
    profile_name: Optional[str] = None,
    conta_id: Optional[str] = None,
) -> bool:
    """Atualiza status, conta_id, resposta e timestamp do lead no CSV de forma atômica."""
    try:
        leads: List[Dict] = []
        updated = False
        fieldnames: List[str] = list(COLUNAS_PADRAO)

        if os.path.exists(LEADS_CSV):
            with open(LEADS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    fieldnames = list(reader.fieldnames)
                for col in COLUNAS_PADRAO:
                    if col not in fieldnames:
                        fieldnames.append(col)

                for row in reader:
                    row["telefone"] = normalize_phone(row.get("telefone", ""))
                    if phones_match(row.get("telefone", ""), phone_number):
                        row["status"] = status
                        if response_text is not None:
                            row["ultima_resposta"] = response_text
                        if profile_name and (not row.get("nome") or row.get("nome", "").startswith("Lead ")):
                            row["nome"] = profile_name
                        if conta_id:
                            row["conta_id"] = conta_id
                        row["ultima_interacao_em"] = datetime.now().isoformat()

                        if status == "opt_out":
                            row["opt_out"] = "true"
                        updated = True

                    leads.append(row)

        if not updated and os.path.exists(LEADS_CSV):
            novo_id = str(len(leads) + 1)
            leads.append({
                "id": novo_id,
                "nome": profile_name or f"Contato {novo_id}",
                "telefone": normalize_phone(phone_number),
                "status": status,
                "opt_out": "true" if status == "opt_out" else "false",
                "conta_id": conta_id or "conta_1",
                "message_id": "",
                "ultima_interacao_em": datetime.now().isoformat(),
                "ultima_resposta": response_text or "",
            })
            updated = True

        if updated:
            os.makedirs(os.path.dirname(LEADS_CSV) or ".", exist_ok=True)
            arquivo_temp = f"{LEADS_CSV}.tmp"
            with open(arquivo_temp, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(leads)
            os.replace(arquivo_temp, LEADS_CSV)
            logger.info(f"Lead {normalize_phone(phone_number)} atualizado para status: {status} ({conta_id or 'conta_1'})")

        return updated

    except Exception as e:
        logger.error(f"Erro ao atualizar lead {phone_number}: {e}")
        return False


def log_event(event_type: str, phone_number: str, payload: Dict) -> None:
    """Registra evento no arquivo JSONL de auditoria."""
    try:
        os.makedirs(os.path.dirname(EVENTS_LOG) or ".", exist_ok=True)
        event = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "phone": normalize_phone(phone_number),
            "payload": payload,
        }

        with open(EVENTS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

        logger.info(f"Evento registrado: {event_type} - {phone_number}")

    except Exception as e:
        logger.error(f"Erro ao registrar evento: {e}")


def _buscar_lead_na_planilha(phone_number: str) -> Optional[Dict]:
    """Procura o telefone na planilha de leads (com casamento de 9º dígito). Retorna None se não for um lead da campanha."""
    if not os.path.exists(LEADS_CSV):
        return None
    try:
        with open(LEADS_CSV, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row.pop(None, None)
                if phones_match(row.get("telefone", ""), phone_number):
                    return row
    except Exception:
        pass
    return None


def process_message(
    phone_number: str,
    message_text: str,
    profile_name: Optional[str] = None,
    incoming_account_ref: Optional[str] = None,
    from_simulator: bool = False,
) -> str:
    """
    Processa a resposta do lead (1, 2 ou livre):
    - Ignora contatos pessoais que NÃO estão na planilha da campanha (quando from_simulator=False).
    - Só envia auto-resposta do robô na 1ª resposta após o disparo (evita responder repetidamente na mesma conversa).
    """
    texto_limpo = (message_text or "").strip()
    phone_norm = normalize_phone(phone_number)
    lead_existente = _buscar_lead_na_planilha(phone_norm)

    if not from_simulator and not lead_existente:
        logger.info(f"[FILTRO PESSOAL] Contato {phone_norm} ({profile_name}) não faz parte da planilha da campanha. Ignorando auto-resposta.")
        return "ignorado_contato_pessoal"

    status_anterior = str((lead_existente or {}).get("status", "novo")).strip().lower()
    ja_interagiu_antes = (not from_simulator) and (status_anterior in ("interessado", "respondeu", "opt_out"))

    nome_exibicao = profile_name or (lead_existente or {}).get("nome") or "Cliente"

    conta = descobrir_conta_do_lead(phone_norm, incoming_account_ref)
    conta_id = conta.get("id", "conta_1")
    conta_nome = conta.get("nome", conta_id)
    instance_name = conta.get("instance_name") or f"chip_{conta.get('slot', 1)}"
    consultor_destino = conta.get("consultant_phone") or CONSULTANT_PHONE_NUMBER

    if texto_limpo in ("1", "1️⃣"):
        status = "interessado"
        logger.info(f"[{instance_name}] Lead {phone_norm} ({nome_exibicao}) marcado como INTERESSADO")

        if not ja_interagiu_antes:
            send_whatsapp_text(phone_norm, RESPOSTA_INTERESSADO, account=conta)
            if consultor_destino:
                wa_link = f"https://wa.me/{phone_norm}"
                notification_msg = (
                    f"🔔 *Novo Lead Interessado (Plano Empresarial)!*\n"
                    f"📲 *Instância Evolution:* {conta_nome} ({instance_name})\n\n"
                    f"👤 *Nome:* {nome_exibicao}\n"
                    f"📱 *Telefone:* {phone_norm}\n\n"
                    f"🔗 *Conversar agora:* {wa_link}"
                )
                send_whatsapp_text(consultor_destino, notification_msg, account=conta)

    elif texto_limpo in ("2", "2️⃣", "sair", "parar", "nao", "não"):
        status = "opt_out"
        logger.info(f"[{instance_name}] Lead {phone_norm} solicitou OPT-OUT")
        if status_anterior != "opt_out" or from_simulator:
            send_whatsapp_text(phone_norm, RESPOSTA_OPT_OUT, account=conta)

    else:
        status = "interessado" if status_anterior == "interessado" else "respondeu"
        logger.info(f"[{instance_name}] Lead {phone_norm} enviou mensagem livre: {texto_limpo}")

        # Só envia a mensagem automática "Obrigado pelo retorno..." na PRIMEIRA vez que o lead responde!
        if not ja_interagiu_antes:
            send_whatsapp_text(phone_norm, RESPOSTA_LIVRE, account=conta)
            if consultor_destino:
                wa_link = f"https://wa.me/{phone_norm}"
                notification_msg = (
                    f"💬 *Nova Resposta Livre de Lead!*\n"
                    f"📲 *Instância Evolution:* {conta_nome} ({instance_name})\n\n"
                    f"👤 *Nome:* {nome_exibicao}\n"
                    f"📱 *Telefone:* {phone_norm}\n"
                    f"📝 *Mensagem:* \"{texto_limpo}\"\n\n"
                    f"🔗 *Atender agora:* {wa_link}"
                )
                send_whatsapp_text(consultor_destino, notification_msg, account=conta)

    update_lead_status(phone_norm, status, texto_limpo, profile_name, conta_id=conta_id)

    log_event(
        "message_received",
        phone_norm,
        {
            "account_id": conta_id,
            "instance_name": instance_name,
            "account_name": conta_nome,
            "text": texto_limpo,
            "profile_name": profile_name,
            "new_status": status,
        },
    )

    # Notifica automaticamente o painel Central Perfectenschlag (se ativo)
    perfectenschlag_url = os.getenv("PERFECTENSCHLAG_WEBHOOK_URL", "http://127.0.0.1:8080/webhook/whatsapp-lead")
    if perfectenschlag_url:
        try:
            requests.post(
                perfectenschlag_url,
                json={
                    "account_id": conta_id,
                    "instance_name": instance_name,
                    "account_name": conta_nome,
                    "phone": phone_norm,
                    "name": nome_exibicao,
                    "text": texto_limpo,
                    "status": status,
                },
                timeout=2,
            )
        except Exception:
            pass

    return status


def _extract_evolution_text(msg_obj: Dict) -> str:
    """Extrai o texto de qualquer tipo de mensagem da Evolution API (conversation, extendedTextMessage, botões, listas)."""
    if not isinstance(msg_obj, dict):
        return ""
    if msg_obj.get("conversation"):
        return str(msg_obj["conversation"])
    if isinstance(msg_obj.get("extendedTextMessage"), dict):
        return str(msg_obj["extendedTextMessage"].get("text", ""))
    if isinstance(msg_obj.get("buttonsResponseMessage"), dict):
        btn = msg_obj["buttonsResponseMessage"]
        return str(btn.get("selectedDisplayText") or btn.get("selectedButtonId") or "")
    if isinstance(msg_obj.get("listResponseMessage"), dict):
        lst = msg_obj["listResponseMessage"]
        return str(lst.get("title") or lst.get("singleSelectReply", {}).get("selectedRowId") or "")
    if isinstance(msg_obj.get("templateButtonReplyMessage"), dict):
        tbtn = msg_obj["templateButtonReplyMessage"]
        return str(tbtn.get("selectedDisplayText") or tbtn.get("selectedId") or "")
    return ""


@app.route("/webhook", methods=["GET"])
def verify_webhook():
    """Health/Verify endpoint para o Webhook."""
    verify_token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if verify_token and verify_token == WEBHOOK_VERIFY_TOKEN:
        return challenge, 200
    return jsonify({"status": "evolution_webhook_ready", "max_instances": MAX_WHATSAPP_ACCOUNTS}), 200


@app.route("/webhook", methods=["POST"])
@app.route("/webhook/<path:subpath>", methods=["POST"])
def handle_webhook(subpath: Optional[str] = None):
    """
    Processa eventos nativos da Evolution API (messages.upsert, connection.update)
    para qualquer uma das 5 instâncias simultâneas (chip_1 .. chip_5).
    """
    try:
        data = request.get_json(silent=True) or {}
        event_name = str(data.get("event") or subpath or "").strip().lower().replace("_", ".")
        instance_name = str(data.get("instance") or "").strip()

        # 1. Formato Nativo Evolution API (v1.8.x e v2.x)
        if event_name in ("messages.upsert", "message.upsert") or ("instance" in data and "data" in data):
            payload_data = data.get("data") or {}

            # Atualização de status de conexão do chip (open / close / connecting)
            if "connection" in event_name or "state" in payload_data:
                new_state = payload_data.get("state") or payload_data.get("status") or "unknown"
                contas = load_whatsapp_accounts()
                for c in contas:
                    if c.get("instance_name") == instance_name or c.get("id") == instance_name:
                        c["connection_status"] = new_state
                        break
                save_whatsapp_accounts(contas)
                log_event("evolution_connection", instance_name, {"instance": instance_name, "state": new_state})
                return jsonify({"status": "connection_updated", "state": new_state}), 200

            # Lista de mensagens em data (v1 ou v2)
            records = payload_data.get("messages") if isinstance(payload_data, dict) and "messages" in payload_data else [payload_data]
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                key = rec.get("key") or {}
                if key.get("fromMe"):
                    # Ignora mensagens enviadas pelo próprio chip da campanha
                    continue
                remote_jid = str(key.get("remoteJid") or "")
                if not remote_jid or "@g.us" in remote_jid or "status@broadcast" in remote_jid:
                    # Ignora grupos e status do WhatsApp
                    continue

                phone_number = normalize_phone(remote_jid)
                push_name = rec.get("pushName") or None
                msg_content = rec.get("message") or {}
                text = _extract_evolution_text(msg_content)

                if phone_number and text:
                    process_message(
                        phone_number=phone_number,
                        message_text=text,
                        profile_name=push_name,
                        incoming_account_ref=instance_name,
                    )

            return jsonify({"status": "ok", "engine": "evolution-api"}), 200

        # 2. Fallback caso também chegue formato Meta Cloud API
        if data.get("object") == "whatsapp_business_account":
            for entry in data.get("entry", []):
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    metadata = value.get("metadata", {}) or {}
                    incoming_phone_id = metadata.get("phone_number_id") or metadata.get("display_phone_number")
                    contacts = {
                        c.get("wa_id"): c.get("profile", {}).get("name")
                        for c in value.get("contacts", [])
                    }
                    for message in value.get("messages", []):
                        phone_number = message.get("from", "")
                        message_type = message.get("type", "")
                        profile_name = contacts.get(phone_number)
                        if message_type == "text":
                            text = message.get("text", {}).get("body", "")
                            process_message(phone_number, text, profile_name, incoming_phone_id)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logger.error(f"Erro ao processar webhook: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def index_dashboard():
    """Interface Web principal do Assistant to the Lead Manager."""
    frontend_path = Path(__file__).resolve().parent / "frontend" / "index.html"
    if frontend_path.exists():
        return send_file(str(frontend_path))
    return jsonify({"status": "online", "message": "Frontend index.html nao encontrado."}), 200


def _sanitize_accounts_for_ui(contas: List[Dict], stats_por_conta: Dict[str, Dict], check_live: bool = False) -> List[Dict]:
    """Prepara os dados das 5 instâncias da Evolution API para o frontend."""
    saida: List[Dict] = []
    for c in contas[:MAX_WHATSAPP_ACCOUNTS]:
        cid = c.get("id", "conta_1")
        slot = c.get("slot", 1)
        inst = c.get("instance_name") or f"chip_{slot}"
        apikey_raw = str(c.get("apikey", "") or "").strip()
        metrica = stats_por_conta.get(cid, {"enviados": 0, "interessados": 0, "respondeu": 0, "opt_out": 0})

        conn_state = c.get("connection_status", "close")
        if check_live:
            chk = evolution_check_connection(c)
            conn_state = chk.get("state", conn_state)

        saida.append({
            "id": cid,
            "slot": slot,
            "nome": c.get("nome", f"Chip WhatsApp #{slot}"),
            "instance_name": inst,
            "ativo": bool(c.get("ativo", False)),
            "connection_status": conn_state,
            "telefone_exibicao": c.get("telefone_exibicao", ""),
            "has_custom_apikey": bool(apikey_raw),
            "consultant_phone": c.get("consultant_phone", ""),
            "daily_limit": int(c.get("daily_limit", 60)),
            "enviados_hoje": int(c.get("enviados_hoje", 0)),
            "ultimo_disparo_em": c.get("ultimo_disparo_em", ""),
            "stats": metrica,
        })
    return saida


@app.route("/api/dashboard", methods=["GET"])
def api_dashboard():
    """Retorna KPIs, status das 5 instâncias Evolution API, lista paginada de leads, logs e configuração."""
    try:
        page = max(1, int(request.args.get("page", 1)))
        per_page = max(1, min(200, int(request.args.get("per_page", 50))))
        status_filter = (request.args.get("status", "todos") or "todos").strip().lower()
        account_filter = (request.args.get("account", "todas") or "todas").strip().lower()
        query = (request.args.get("q", "") or "").strip().lower()

        stats = {
            "total": 0,
            "novo": 0,
            "enviado": 0,
            "interessado": 0,
            "respondeu": 0,
            "opt_out": 0,
            "erro": 0,
        }
        stats_por_conta: Dict[str, Dict] = {
            f"conta_{i}": {"enviados": 0, "interessados": 0, "respondeu": 0, "opt_out": 0}
            for i in range(1, MAX_WHATSAPP_ACCOUNTS + 1)
        }
        filtered: List[Dict] = []

        if os.path.exists(LEADS_CSV):
            with open(LEADS_CSV, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row.pop(None, None)
                    stats["total"] += 1
                    st = (row.get("status", "novo") or "novo").strip().lower()
                    cid = (row.get("conta_id", "") or "").strip().lower()
                    if st in stats:
                        stats[st] += 1
                    else:
                        stats[st] = stats.get(st, 0) + 1

                    if cid in stats_por_conta:
                        if st in ("enviado", "interessado", "respondeu", "opt_out"):
                            stats_por_conta[cid]["enviados"] += 1
                        if st == "interessado":
                            stats_por_conta[cid]["interessados"] += 1
                        elif st == "respondeu":
                            stats_por_conta[cid]["respondeu"] += 1
                        elif st == "opt_out":
                            stats_por_conta[cid]["opt_out"] += 1

                    row["telefone"] = normalize_phone(row.get("telefone", ""))
                    if status_filter != "todos" and st != status_filter:
                        continue
                    if account_filter != "todas" and cid != account_filter:
                        continue
                    if query:
                        haystack = f"{row.get('id', '')} {row.get('nome', '')} {row.get('telefone', '')} {cid}".lower()
                        if query not in haystack:
                            continue
                    filtered.append(row)

        total_filtered = len(filtered)
        total_pages = max(1, math.ceil(total_filtered / per_page))
        start = (page - 1) * per_page
        paginated = filtered[start : start + per_page]

        recent_events: List[Dict] = []
        if os.path.exists(EVENTS_LOG):
            with open(EVENTS_LOG, "r", encoding="utf-8", errors="replace") as ef:
                lines = ef.readlines()[-25:]
                for ln in reversed(lines):
                    try:
                        recent_events.append(json.loads(ln.strip()))
                    except Exception:
                        pass

        contas_raw = load_whatsapp_accounts()
        contas_ui = _sanitize_accounts_for_ui(contas_raw, stats_por_conta)
        ativas_count = sum(1 for c in contas_ui if c["ativo"])
        capacidade_diaria_total = sum(c["daily_limit"] for c in contas_ui if c["ativo"])

        config_info = {
            "evolution_api_url": os.getenv("EVOLUTION_API_URL", EVOLUTION_API_URL),
            "evolution_api_key": os.getenv("EVOLUTION_API_KEY", EVOLUTION_API_KEY),
            "consultant_phone": os.getenv("CONSULTANT_PHONE_NUMBER", ""),
            "verify_token": os.getenv("WEBHOOK_VERIFY_TOKEN", ""),
            "daily_limit": int(os.getenv("DAILY_MESSAGE_LIMIT", 60)),
            "min_delay": float(os.getenv("MIN_DELAY", 6.0)),
            "max_delay": float(os.getenv("MAX_DELAY", 15.0)),
            "dry_run": os.getenv("DRY_RUN", "True").lower() == "true",
            "business_hours_now": is_business_hours(),
            "mensagem_padrao": MENSAGEM_PADRAO_PROSPECCAO,
            "max_accounts": MAX_WHATSAPP_ACCOUNTS,
            "active_accounts_count": ativas_count,
            "total_daily_capacity": capacidade_diaria_total,
        }

        return jsonify({
            "ok": True,
            "stats": stats,
            "accounts": contas_ui,
            "leads": paginated,
            "filtered_total": total_filtered,
            "page": page,
            "total_pages": total_pages,
            "events": recent_events,
            "config": config_info,
        }), 200
    except Exception as e:
        logger.error(f"Erro em /api/dashboard: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/evolution/qrcode", methods=["POST"])
def api_evolution_qrcode():
    """Cria/conecta uma das 5 instâncias na Evolution API e retorna o QR Code Base64 para parear o WhatsApp."""
    body = request.get_json(silent=True) or {}
    account_id = str(body.get("id", "conta_1")).strip()
    conta = resolver_conta_whatsapp(account_id)
    resultado = evolution_create_or_connect_instance(conta)

    # Atualiza status salvo da instância
    if resultado.get("ok"):
        contas = load_whatsapp_accounts()
        for c in contas:
            if c.get("id") == conta.get("id"):
                c["connection_status"] = resultado.get("state", "connecting")
                break
        save_whatsapp_accounts(contas)

    return jsonify(resultado), 200


@app.route("/api/evolution/status", methods=["POST"])
def api_evolution_status():
    """Consulta em tempo real na Evolution API o estado das 5 instâncias (open, connecting, close)."""
    contas = load_whatsapp_accounts()
    estados = {}
    online_any = False
    for c in contas:
        res = evolution_check_connection(c)
        if res.get("online"):
            online_any = True
        c["connection_status"] = res.get("state", "close")
        estados[c["id"]] = res
    save_whatsapp_accounts(contas)
    return jsonify({"ok": True, "evolution_online": online_any, "statuses": estados}), 200


@app.route("/api/evolution/logout", methods=["POST"])
def api_evolution_logout():
    """Desconecta o aparelho WhatsApp de uma das 5 instâncias na Evolution API."""
    body = request.get_json(silent=True) or {}
    account_id = str(body.get("id", "conta_1")).strip()
    conta = resolver_conta_whatsapp(account_id)
    res = evolution_logout_instance(conta)
    contas = load_whatsapp_accounts()
    for c in contas:
        if c.get("id") == conta.get("id"):
            c["connection_status"] = "close"
            break
    save_whatsapp_accounts(contas)
    return jsonify(res), 200


@app.route("/api/accounts", methods=["GET", "POST"])
def api_accounts():
    """Gerencia as até 5 instâncias simultâneas da Evolution API."""
    if request.method == "GET":
        contas = load_whatsapp_accounts()
        return jsonify({"ok": True, "accounts": _sanitize_accounts_for_ui(contas, {})}), 200

    try:
        body = request.get_json(silent=True) or {}
        contas = load_whatsapp_accounts()

        if body.get("action") == "reset_counters":
            hoje = datetime.now().strftime("%Y-%m-%d")
            for c in contas:
                c["enviados_hoje"] = 0
                c["data_contagem"] = hoje
            save_whatsapp_accounts(contas)
            return jsonify({"ok": True, "message": "Contadores diários das 5 instâncias zerados."}), 200

        if body.get("action") == "toggle":
            target_id = str(body.get("id", "")).strip()
            for c in contas:
                if c.get("id") == target_id:
                    c["ativo"] = not bool(c.get("ativo", False))
                    break
            save_whatsapp_accounts(contas)
            return jsonify({"ok": True}), 200

        incoming_list = body.get("accounts")
        if isinstance(incoming_list, list):
            mapa_atual = {c["id"]: c for c in contas}
            for item in incoming_list[:MAX_WHATSAPP_ACCOUNTS]:
                cid = str(item.get("id", "")).strip()
                if cid not in mapa_atual:
                    continue
                atual = mapa_atual[cid]
                if "nome" in item:
                    atual["nome"] = str(item["nome"] or atual["nome"]).strip()
                if "instance_name" in item and str(item["instance_name"]).strip():
                    atual["instance_name"] = re.sub(r"[^a-zA-Z0-9_-]", "_", str(item["instance_name"]).strip())
                if "ativo" in item:
                    atual["ativo"] = bool(item["ativo"])
                if "telefone_exibicao" in item:
                    atual["telefone_exibicao"] = normalize_phone(str(item["telefone_exibicao"] or ""))
                if "consultant_phone" in item:
                    atual["consultant_phone"] = normalize_phone(str(item["consultant_phone"] or ""))
                if "daily_limit" in item:
                    try:
                        atual["daily_limit"] = max(1, int(item["daily_limit"]))
                    except Exception:
                        pass
                nova_key = str(item.get("apikey", "") or "").strip()
                if nova_key:
                    atual["apikey"] = nova_key

            save_whatsapp_accounts(contas)
            return jsonify({"ok": True}), 200

        return jsonify({"ok": False, "error": "Payload inválido para /api/accounts"}), 400
    except Exception as e:
        logger.error(f"Erro em /api/accounts: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campanha/parar", methods=["POST"])
def api_parar_campanha():
    """Aciona parada imediata de qualquer lote de campanha em andamento."""
    global STOP_CAMPANHA_FLAG
    STOP_CAMPANHA_FLAG = True
    return jsonify({"ok": True, "message": "Sinal de parada imediata enviado."}), 200


@app.route("/api/campanha/disparar", methods=["POST"])
def api_disparar_campanha():
    """Executa um lote controlado da campanha via Evolution API com trava de exclusividade (Lock) e Anti-Reenvio."""
    global STOP_CAMPANHA_FLAG
    if not CAMPANHA_LOCK.acquire(blocking=False):
        return jsonify({
            "ok": False,
            "error": "Já existe um disparo em andamento em segundo plano! Aguarde concluir ou clique em '🛑 Parar Disparo'.",
        }), 409

    STOP_CAMPANHA_FLAG = False
    try:
        body = request.get_json(silent=True) or {}
        dry_run = bool(body.get("dry_run", True))
        limit = max(1, int(body.get("limit", 5)))
        min_delay = max(0.0, float(body.get("min_delay", body.get("delay", 15.0))))
        max_delay = max(min_delay, float(body.get("max_delay", min_delay + 15.0)))
        force_hours = bool(body.get("force_hours", True))
        account_mode = str(body.get("account_mode", "todas_ativas") or "todas_ativas").strip()
        custom_msg = (body.get("message") or MENSAGEM_PADRAO_PROSPECCAO).strip()

        contas_aptas = get_active_accounts(account_mode, dry_run=dry_run)
        if not contas_aptas:
            return jsonify({
                "ok": False,
                "error": (
                    "Nenhum chip ativo está CONECTADO (🟢) na Evolution API no momento. "
                    "Clique em '📱 QR Code' para conectar seu aparelho primeiro (ou use o modo Simulação DRY-RUN)."
                ),
            }), 400

        if not os.path.exists(LEADS_CSV):
            return jsonify({"ok": False, "error": "Arquivo data/leads.csv não encontrado."}), 404

        leads: List[Dict] = []
        with open(LEADS_CSV, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
            for row in reader:
                row.pop(None, None)
                row["telefone"] = normalize_phone(row.get("telefone", ""))
                leads.append(row)

        sent = 0
        attempts = 0
        skipped_sent = 0
        skipped_opt_out = 0
        ja_conversado_count = 0
        sem_whatsapp_count = 0
        circuit_breaker_triggered = False
        circuit_breaker_reason = ""
        por_conta: Dict[str, int] = {c["id"]: 0 for c in contas_aptas}
        rr_idx = 0

        for lead in leads:
            if STOP_CAMPANHA_FLAG:
                circuit_breaker_triggered = True
                circuit_breaker_reason = "Disparo interrompido manualmente pelo usuário."
                break
            if sent >= limit or attempts >= limit or not contas_aptas:
                break
            opt_out_val = str(lead.get("opt_out", "false")).strip().lower()
            status_atual = str(lead.get("status", "novo")).strip().lower()
            if opt_out_val == "true" or status_atual == "opt_out":
                skipped_opt_out += 1
                continue
            if status_atual not in ("novo", "", "pendente"):
                skipped_sent += 1
                continue
            tel = lead.get("telefone", "")
            if not tel:
                continue

            conta_escolhida = None
            for _ in range(len(contas_aptas)):
                candidata = contas_aptas[rr_idx % len(contas_aptas)]
                rr_idx += 1
                if int(candidata.get("enviados_hoje", 0)) < int(candidata.get("daily_limit", 60)):
                    conta_escolhida = candidata
                    break

            if not conta_escolhida:
                break

            resp = send_whatsapp_message(
                tel,
                custom_msg,
                dry_run=dry_run,
                force_outside_hours=force_hours,
                account=conta_escolhida,
                lead_name=lead.get("nome", ""),
            )
            # Se o número já tinha histórico no aparelho ou no cofre permanente, marca como enviado sem gastar tentativa/espera!
            if resp and resp.get("error") in ("ja_enviado_historico", "ja_conversado_no_chip"):
                lead["status"] = "enviado"
                lead["conta_id"] = conta_escolhida["id"]
                lead["ultima_interacao_em"] = lead.get("ultima_interacao_em") or datetime.now().isoformat()
                ja_conversado_count += 1
                skipped_sent += 1
                salvar_leads_csv(leads, fieldnames)
                continue

            attempts += 1
            if resp and "messages" in resp:
                cid = conta_escolhida["id"]
                inst_name = conta_escolhida.get("instance_name", cid)
                lead["status"] = "enviado"
                lead["conta_id"] = cid
                lead["message_id"] = resp["messages"][0].get("id", "")
                lead["ultima_interacao_em"] = datetime.now().isoformat()
                sent += 1
                por_conta[cid] = por_conta.get(cid, 0) + 1
                conta_escolhida["enviados_hoje"] = int(conta_escolhida.get("enviados_hoje", 0)) + 1
                registrar_disparo_conta(cid)
                salvar_leads_csv(leads, fieldnames)
                log_event(
                    "campaign_sent",
                    tel,
                    {
                        "account_id": cid,
                        "instance_name": inst_name,
                        "account_name": conta_escolhida.get("nome", cid),
                        "dry_run": dry_run,
                        "message_id": lead["message_id"],
                    },
                )
            elif resp and resp.get("error") == "sem_whatsapp":
                lead["status"] = "sem_whatsapp"
                lead["ultima_interacao_em"] = datetime.now().isoformat()
                sem_whatsapp_count += 1
                salvar_leads_csv(leads, fieldnames)
            elif resp and resp.get("fatal_connection"):
                bad_id = conta_escolhida["id"]
                inst_name = conta_escolhida.get("instance_name", bad_id)
                circuit_breaker_triggered = True
                circuit_breaker_reason = f"Instância {inst_name} desconectou ou recusou envio ({resp.get('error')}). Lote interrompido por segurança Anti-Ban."
                logger.warning(f"[CIRCUIT-BREAKER] {circuit_breaker_reason}")
                contas_aptas = [c for c in contas_aptas if c["id"] != bad_id]
                if not contas_aptas:
                    break

            # Respeita SEMPRE o intervalo completo de Jitter Anti-Ban
            if sent < limit and attempts < limit and contas_aptas and not STOP_CAMPANHA_FLAG:
                espera = 0.04 if dry_run else random.uniform(min_delay, max_delay)
                time.sleep(espera)

        salvar_leads_csv(leads, fieldnames)
        return jsonify({
            "ok": True,
            "mode": "DRY-RUN" if dry_run else "EVOLUTION-LIVE",
            "sent": sent,
            "attempts": attempts,
            "ja_conversado": ja_conversado_count,
            "sem_whatsapp": sem_whatsapp_count,
            "circuit_breaker": circuit_breaker_triggered,
            "circuit_breaker_reason": circuit_breaker_reason,
            "per_account": por_conta,
            "skipped_sent": skipped_sent,
            "skipped_opt_out": skipped_opt_out,
        }), 200
    except Exception as e:
        logger.error(f"Erro em /api/campanha/disparar: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        CAMPANHA_LOCK.release()


@app.route("/api/webhook/simular", methods=["POST"])
def api_simular_webhook():
    """Permite simular eventos MESSAGES_UPSERT da Evolution API em qualquer uma das 5 instâncias."""
    body = request.get_json(silent=True) or {}
    phone = str(body.get("phone", "")).strip()
    text = str(body.get("text", "")).strip()
    name = str(body.get("name", "")).strip() or None
    account_id = str(body.get("account_id", "")).strip() or None
    if not phone or not text:
        return jsonify({"ok": False, "error": "Informe telefone e texto."}), 400
    new_status = process_message(phone, text, name, incoming_account_ref=account_id, from_simulator=True)
    return jsonify({"ok": True, "new_status": new_status, "phone": normalize_phone(phone)}), 200


@app.route("/api/leads/atualizar", methods=["POST"])
def api_atualizar_lead():
    """Atualiza manualmente o status de um lead pelo ID."""
    body = request.get_json(silent=True) or {}
    lead_id = str(body.get("id", "")).strip()
    new_status = str(body.get("status", "novo")).strip().lower()
    if not os.path.exists(LEADS_CSV):
        return jsonify({"ok": False}), 404

    leads: List[Dict] = []
    with open(LEADS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or COLUNAS_PADRAO)
        for row in reader:
            if str(row.get("id")) == lead_id:
                row["status"] = new_status
                row["opt_out"] = "true" if new_status == "opt_out" else "false"
                row["ultima_interacao_em"] = datetime.now().isoformat()
            leads.append(row)

    salvar_leads_csv(leads, fieldnames)
    return jsonify({"ok": True}), 200


def _phone_dedup_key(phone: str) -> str:
    """Gera chave única de deduplicação considerando DDD + 8 dígitos finais (ignora diferença do 9º dígito)."""
    norm = normalize_phone(phone)
    if len(norm) >= 12 and norm.startswith("55"):
        return f"{norm[:4]}_{norm[-8:]}"
    return norm


def _is_valid_br_phone(phone: str, only_mobile: bool = False) -> bool:
    """Valida se o telefone normalizado possui tamanho válido no Brasil (12 ou 13 dígitos com DDI 55)."""
    norm = normalize_phone(phone)
    if len(norm) not in (12, 13) or not norm.startswith("55"):
        return False
    if only_mobile:
        # Celulares brasileiros têm 13 dígitos (55+DDD+9+8dig) ou 8 dígitos finais iniciando em 6,7,8,9
        if len(norm) == 12 and norm[4] not in ("6", "7", "8", "9"):
            return False
    return True


def _inserir_pares_leads(
    pares: List[Tuple[str, str]],
    conta_id_padrao: str = "",
    prefixo_tag: str = "",
    only_mobile: bool = False,
) -> Dict:
    """Insere lista de (nome, telefone) em data/leads.csv com deduplicação 9º dígito e estatísticas detalhadas."""
    leads: List[Dict] = []
    fieldnames = list(COLUNAS_PADRAO)
    chaves_existentes = set()

    if os.path.exists(LEADS_CSV):
        with open(LEADS_CSV, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                fieldnames = list(reader.fieldnames)
            for row in reader:
                t = normalize_phone(row.get("telefone", ""))
                row["telefone"] = t
                if t:
                    chaves_existentes.add(_phone_dedup_key(t))
                leads.append(row)

    adicionados = 0
    duplicados = 0
    invalidos = 0
    preview: List[Dict] = []

    tag_str = f"[{prefixo_tag.strip()}] " if prefixo_tag and prefixo_tag.strip() else ""

    for nm_raw, tl_raw in pares:
        tn = normalize_phone(tl_raw)
        if not _is_valid_br_phone(tn, only_mobile=only_mobile):
            invalidos += 1
            continue
        chave = _phone_dedup_key(tn)
        if chave in chaves_existentes:
            duplicados += 1
            continue

        chaves_existentes.add(chave)
        novo_id = str(len(leads) + 1)
        nome_limpo = (nm_raw or "").strip() or f"Lead {novo_id}"
        if tag_str and not nome_limpo.startswith("["):
            nome_limpo = f"{tag_str}{nome_limpo}"

        novo_lead = {
            "id": novo_id,
            "nome": nome_limpo,
            "telefone": tn,
            "status": "novo",
            "opt_out": "false",
            "conta_id": conta_id_padrao if conta_id_padrao not in ("todas", "todas_ativas") else "",
            "message_id": "",
            "ultima_interacao_em": "",
            "ultima_resposta": "",
        }
        leads.insert(0, novo_lead)
        adicionados += 1
        if len(preview) < 8:
            preview.append({"id": novo_id, "nome": nome_limpo, "telefone": tn})

    if adicionados > 0:
        salvar_leads_csv(leads, fieldnames)

    return {
        "ok": True,
        "added": adicionados,
        "duplicates": duplicados,
        "invalid": invalidos,
        "preview": preview,
    }


def _extrair_pares_de_linhas_estruturadas(texto: str) -> List[Tuple[str, str]]:
    """Analisa linhas CSV, TSV (coladas do Excel/Google Sheets) ou lista de telefones."""
    pares: List[Tuple[str, str]] = []
    linhas = [ln.strip() for ln in (texto or "").splitlines() if ln.strip()]
    if not linhas:
        return pares

    # Detecta separador predominante (\t do Excel, ; do CSV BR, , do CSV padrão, ou |)
    for idx, ln in enumerate(linhas):
        sep = "\t" if "\t" in ln else (";" if ";" in ln else ("," if "," in ln else ("|" if "|" in ln else None)))
        if sep:
            cols = [c.strip().strip('"').strip("'") for c in ln.split(sep) if c.strip()]
            # Ignora linha de cabeçalho
            if idx == 0 and any(h in ln.lower() for h in ("telefone", "celular", "whatsapp", "phone", "nome", "empresa")):
                continue
            tel_encontrado = ""
            nome_partes = []
            for c in cols:
                digs = re.sub(r"\D", "", c)
                if not tel_encontrado and len(digs) in (10, 11, 12, 13):
                    tel_encontrado = digs
                else:
                    nome_partes.append(c)
            if tel_encontrado:
                nome_final = " - ".join(nome_partes[:2]) if nome_partes else ""
                pares.append((nome_final, tel_encontrado))
        else:
            digs = re.sub(r"\D", "", ln)
            if len(digs) in (10, 11, 12, 13):
                texto_sem_num = re.sub(r"[\d\+\(\)\-\s]{8,}", " ", ln).strip(" -:;.,|")
                pares.append((texto_sem_num, digs))
    return pares


def _extrair_pares_texto_bruto_regex(texto: str) -> List[Tuple[str, str]]:
    """
    Caçador Inteligente de Telefones (Regex Scraper):
    Varre qualquer texto bagunçado (Google Maps, PDF, e-mail, listas de WhatsApp, sites)
    e extrai todos os telefones brasileiros junto com o nome da linha atual ou anterior.
    """
    pares: List[Tuple[str, str]] = []
    padrao_tel = re.compile(r"(?:\+?55[\s\-\.]*)?(?:\(?\d{2}\)?[\s\-\.]*)?9?\d{4}[\s\-\.]?\d{4}")
    linhas = [ln.strip() for ln in (texto or "").splitlines() if ln.strip()]
    ultima_linha_texto = ""

    for ln in linhas:
        matches = list(padrao_tel.finditer(ln))
        if not matches:
            if len(ln) > 2 and not ln.isdigit():
                ultima_linha_texto = ln[:80]
            continue

        # Texto que sobra na mesma linha removendo os números de telefone
        sobra = padrao_tel.sub(" ", ln)
        sobra = re.sub(r"\s+", " ", sobra).strip(" -:;.,|/•*#")
        nome_associado = sobra if len(sobra) >= 2 else ultima_linha_texto

        for m in matches:
            bruto = m.group(0)
            digs = re.sub(r"\D", "", bruto)
            if len(digs) in (10, 11, 12, 13):
                pares.append((nome_associado, digs))

    return pares


def _parse_vcf_contacts(vcf_text: str) -> List[Tuple[str, str]]:
    """Extrai contatos (FN e TEL) de arquivos .vcf (vCard de celular Android / iPhone / Google Contacts)."""
    pares: List[Tuple[str, str]] = []
    blocos = re.split(r"END:VCARD", vcf_text or "", flags=re.IGNORECASE)
    for bloco in blocos:
        if "BEGIN:VCARD" not in bloco.upper():
            continue
        nome = ""
        tels: List[str] = []
        for ln in bloco.splitlines():
            ln_s = ln.strip()
            if ln_s.upper().startswith("FN:") or ln_s.upper().startswith("FN;"):
                nome = ln_s.split(":", 1)[-1].strip()
            elif not nome and (ln_s.upper().startswith("N:") or ln_s.upper().startswith("N;")):
                partes_n = [p.strip() for p in ln_s.split(":", 1)[-1].split(";") if p.strip()]
                nome = " ".join(reversed(partes_n[:2]))
            elif ln_s.upper().startswith("TEL"):
                t_val = ln_s.split(":", 1)[-1].strip()
                tels.append(t_val)
        for t in tels:
            pares.append((nome, t))
    return pares


def _parse_xlsx_bytes(raw_bytes: bytes) -> List[Tuple[str, str]]:
    """Lê arquivos Excel (.xlsx) nativamente usando zipfile + xml.etree sem depender de bibliotecas externas."""
    pares: List[Tuple[str, str]] = []
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root_ss = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root_ss.iter():
                if si.tag.endswith("}si") or si.tag == "si":
                    partes = [t.text or "" for t in si.iter() if (t.tag.endswith("}t") or t.tag == "t")]
                    shared_strings.append("".join(partes))

        sheet_files = sorted(n for n in zf.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml"))
        for sf in sheet_files:
            root_sh = ET.fromstring(zf.read(sf))
            linhas_txt: List[str] = []
            for row_el in root_sh.iter():
                if row_el.tag.endswith("}row") or row_el.tag == "row":
                    vals: List[str] = []
                    for c_el in row_el:
                        if not (c_el.tag.endswith("}c") or c_el.tag == "c"):
                            continue
                        c_type = c_el.attrib.get("t", "")
                        v_val = ""
                        for child in c_el:
                            if child.tag.endswith("}v") or child.tag == "v":
                                v_val = child.text or ""
                            elif child.tag.endswith("}is") or child.tag == "is":
                                v_val = "".join(t.text or "" for t in child.iter() if (t.tag.endswith("}t") or t.tag == "t"))
                        if c_type == "s" and v_val.isdigit():
                            idx = int(v_val)
                            if 0 <= idx < len(shared_strings):
                                v_val = shared_strings[idx]
                        if v_val:
                            vals.append(str(v_val).strip())
                    if vals:
                        linhas_txt.append("\t".join(vals))
            if linhas_txt:
                pares.extend(_extrair_pares_de_linhas_estruturadas("\n".join(linhas_txt)))
    return pares


@app.route("/api/leads/importar", methods=["POST"])
def api_importar_leads():
    """
    Importa novos leads para data/leads.csv suportando múltiplos modos:
    - 'manual' / 'lote': Linha única ou lote CSV / TSV (Excel Copy & Paste)
    - 'texto_bruto': Extrator Regex inteligente para textos livres (Google Maps, PDF, WhatsApp, Sites)
    """
    body = request.get_json(silent=True) or {}
    modo = str(body.get("modo", "lote")).strip().lower()
    nome_unico = str(body.get("nome", "")).strip()
    tel_unico = str(body.get("telefone", "")).strip()
    lote = str(body.get("lote", "")).strip()
    conta_id = str(body.get("conta_id", "")).strip()
    tag = str(body.get("tag", "")).strip()
    only_mobile = bool(body.get("only_mobile", False))

    pares: List[Tuple[str, str]] = []
    if tel_unico:
        pares.append((nome_unico, tel_unico))

    if lote:
        if modo == "texto_bruto":
            pares.extend(_extrair_pares_texto_bruto_regex(lote))
        else:
            pares.extend(_extrair_pares_de_linhas_estruturadas(lote))

    resultado = _inserir_pares_leads(pares, conta_id_padrao=conta_id, prefixo_tag=tag, only_mobile=only_mobile)
    return jsonify(resultado), 200


@app.route("/api/leads/importar_arquivo", methods=["POST"])
def api_importar_arquivo():
    """
    Importa arquivos enviados via Upload (.CSV, .XLSX Excel, .VCF vCard de Contatos, .JSON, .TXT).
    """
    try:
        conta_id = str(request.form.get("conta_id", "")).strip()
        tag = str(request.form.get("tag", "")).strip()
        only_mobile = str(request.form.get("only_mobile", "false")).lower() == "true"
        arquivos = request.files.getlist("files")
        if not arquivos and "file" in request.files:
            arquivos = [request.files["file"]]

        if not arquivos:
            return jsonify({"ok": False, "error": "Nenhum arquivo selecionado."}), 400

        todos_pares: List[Tuple[str, str]] = []
        nomes_arquivos: List[str] = []

        for arq in arquivos:
            fname = (arq.filename or "arquivo").lower()
            nomes_arquivos.append(arq.filename or "arquivo")
            raw = arq.read()

            if fname.endswith(".xlsx"):
                todos_pares.extend(_parse_xlsx_bytes(raw))
                continue

            # Decodifica texto tentando UTF-8 e fallback Latin-1 (comum no Excel BR)
            try:
                texto = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                texto = raw.decode("latin-1", errors="replace")

            if fname.endswith(".vcf"):
                todos_pares.extend(_parse_vcf_contacts(texto))
            elif fname.endswith(".json"):
                try:
                    jdata = json.loads(texto)
                    lista_j = jdata if isinstance(jdata, list) else jdata.get("leads") or jdata.get("contacts") or jdata.get("data") or []
                    for item in lista_j:
                        if isinstance(item, dict):
                            nm = str(item.get("nome") or item.get("name") or item.get("razao_social") or item.get("empresa") or "")
                            tl = str(item.get("telefone") or item.get("phone") or item.get("celular") or item.get("whatsapp") or "")
                            if tl:
                                todos_pares.append((nm, tl))
                except Exception:
                    todos_pares.extend(_extrair_pares_texto_bruto_regex(texto))
            else:
                extraidos = _extrair_pares_de_linhas_estruturadas(texto)
                if not extraidos:
                    extraidos = _extrair_pares_texto_bruto_regex(texto)
                todos_pares.extend(extraidos)

        resultado = _inserir_pares_leads(todos_pares, conta_id_padrao=conta_id, prefixo_tag=tag, only_mobile=only_mobile)
        resultado["files"] = nomes_arquivos
        return jsonify(resultado), 200
    except Exception as e:
        logger.error(f"Erro em /api/leads/importar_arquivo: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/leads/importar_cnpj", methods=["POST"])
def api_importar_cnpj():
    """
    Recebe uma lista de CNPJs, consulta gratuitamente na BrasilAPI (Receita Federal)
    a Razão Social / Nome Fantasia e os telefones cadastrados do CNPJ, e importa para data/leads.csv.
    """
    try:
        body = request.get_json(silent=True) or {}
        texto_cnpjs = str(body.get("cnpjs", "")).strip()
        conta_id = str(body.get("conta_id", "")).strip()
        tag = str(body.get("tag", "CNPJ")).strip()

        # Extrai sequências de 14 dígitos (CNPJs formatados ou puros)
        brutos = re.findall(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}", texto_cnpjs)
        cnpjs_limpos = []
        vistos = set()
        for b in brutos:
            d = re.sub(r"\D", "", b)
            if len(d) == 14 and d not in vistos:
                vistos.add(d)
                cnpjs_limpos.append(d)

        if not cnpjs_limpos:
            return jsonify({"ok": False, "error": "Nenhum CNPJ válido de 14 dígitos foi encontrado no texto."}), 400

        pares: List[Tuple[str, str]] = []
        detalhes_cnpj: List[Dict] = []

        for cnpj in cnpjs_limpos[:25]:
            try:
                r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=6)
                if r.status_code == 200:
                    d = r.json() or {}
                    empresa = (d.get("nome_fantasia") or d.get("razao_social") or f"CNPJ {cnpj}").strip()
                    tel1 = re.sub(r"\D", "", str(d.get("ddd_telefone_1") or ""))
                    tel2 = re.sub(r"\D", "", str(d.get("ddd_telefone_2") or ""))
                    achou_tel = False
                    for t in (tel1, tel2):
                        if len(t) >= 10:
                            pares.append((empresa, t))
                            achou_tel = True
                    detalhes_cnpj.append({
                        "cnpj": cnpj,
                        "empresa": empresa,
                        "telefone": tel1 or tel2 or "Sem telefone na Receita",
                        "situacao": d.get("descricao_situacao_cadastral", ""),
                        "ok": achou_tel,
                    })
                else:
                    detalhes_cnpj.append({"cnpj": cnpj, "empresa": "Não encontrado", "ok": False})
            except Exception as exc:
                detalhes_cnpj.append({"cnpj": cnpj, "empresa": f"Erro: {exc}", "ok": False})

        resultado = _inserir_pares_leads(pares, conta_id_padrao=conta_id, prefixo_tag=tag)
        resultado["cnpj_results"] = detalhes_cnpj
        return jsonify(resultado), 200
    except Exception as e:
        logger.error(f"Erro em /api/leads/importar_cnpj: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/leads/importar_url", methods=["POST"])
def api_importar_url():
    """
    Importa leads diretamente de uma URL pública (planilha do Google Sheets compartilhada ou link CSV/JSON).
    Converte automaticamente links de edição do Google Sheets em links de exportação CSV.
    """
    try:
        body = request.get_json(silent=True) or {}
        url = str(body.get("url", "")).strip()
        conta_id = str(body.get("conta_id", "")).strip()
        tag = str(body.get("tag", "Planilha Web")).strip()
        only_mobile = bool(body.get("only_mobile", False))

        if not url.startswith(("http://", "https://")):
            return jsonify({"ok": False, "error": "Informe uma URL iniciando com http:// ou https://"}), 400

        # Converte automaticamente link normal do Google Sheets em link de download CSV
        m_gs = re.search(r"docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        if m_gs and "/export" not in url and "output=csv" not in url:
            sheet_id = m_gs.group(1)
            gid_match = re.search(r"[#&?]gid=(\d+)", url)
            gid_param = f"&gid={gid_match.group(1)}" if gid_match else ""
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv{gid_param}"

        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        texto = resp.content.decode("utf-8-sig", errors="replace")

        pares = _extrair_pares_de_linhas_estruturadas(texto)
        if not pares:
            pares = _extrair_pares_texto_bruto_regex(texto)

        resultado = _inserir_pares_leads(pares, conta_id_padrao=conta_id, prefixo_tag=tag, only_mobile=only_mobile)
        resultado["source_url"] = url
        return jsonify(resultado), 200
    except Exception as e:
        logger.error(f"Erro em /api/leads/importar_url: {e}")
        return jsonify({"ok": False, "error": f"Falha ao baixar planilha da URL: {e}"}), 500


@app.route("/api/leads/importar_evolution", methods=["POST"])
def api_importar_evolution():
    """
    Sincroniza e importa contatos/conversas diretamente da agenda de um dos 5 chips conectados na Evolution API.
    """
    try:
        body = request.get_json(silent=True) or {}
        account_id = str(body.get("conta_id", "conta_1")).strip() or "conta_1"
        atribuir_ao_chip = bool(body.get("assign_to_chip", True))
        conta = resolver_conta_whatsapp(account_id)
        inst = conta.get("instance_name") or f"chip_{conta.get('slot', 1)}"

        base_url = os.getenv("EVOLUTION_API_URL", EVOLUTION_API_URL).rstrip("/")
        api_key = conta.get("apikey") or os.getenv("EVOLUTION_API_KEY", EVOLUTION_API_KEY)
        headers = {"apikey": api_key, "Content-Type": "application/json"}

        pares: List[Tuple[str, str]] = []
        # 1. Busca contatos sincronizados na instância Evolution API
        resp = requests.post(f"{base_url}/chat/findContacts/{inst}", headers=headers, json={"where": {}}, timeout=12)
        if resp.ok and resp.content:
            contatos = resp.json()
            if isinstance(contatos, list):
                for c in contatos:
                    if not isinstance(c, dict):
                        continue
                    jid = str(c.get("remoteJid") or c.get("id") or "")
                    if "@g.us" in jid or "broadcast" in jid:
                        continue
                    nome = str(c.get("pushName") or c.get("name") or "").strip()
                    tel = normalize_phone(jid)
                    if tel:
                        pares.append((nome, tel))

        # 2. Busca também conversas recentes (/chat/findChats/{inst}) caso findContacts venha vazio
        if not pares:
            resp_chats = requests.post(f"{base_url}/chat/findChats/{inst}", headers=headers, json={}, timeout=12)
            if resp_chats.ok and resp_chats.content:
                chats = resp_chats.json()
                if isinstance(chats, list):
                    for ch in chats:
                        if not isinstance(ch, dict):
                            continue
                        jid = str(ch.get("remoteJid") or ch.get("id") or "")
                        if "@g.us" in jid or "broadcast" in jid:
                            continue
                        nome = str(ch.get("name") or ch.get("pushName") or "").strip()
                        tel = normalize_phone(jid)
                        if tel:
                            pares.append((nome, tel))

        resultado = _inserir_pares_leads(
            pares,
            conta_id_padrao=conta["id"] if atribuir_ao_chip else "",
            prefixo_tag=f"Agenda {inst}",
        )
        resultado["instance"] = inst
        return jsonify(resultado), 200
    except Exception as e:
        logger.error(f"Erro em /api/leads/importar_evolution: {e}")
        return jsonify({"ok": False, "error": f"Não foi possível buscar contatos da instância Evolution: {e}"}), 500


@app.route("/api/leads/exportar", methods=["GET"])
def api_exportar_csv():
    """Exporta o arquivo data/leads.csv atualizado."""
    caminho = Path(LEADS_CSV).resolve()
    if caminho.exists():
        return send_file(str(caminho), as_attachment=True, download_name="leads_atualizados.csv")
    return jsonify({"error": "Arquivo CSV nao encontrado"}), 404


@app.route("/api/config", methods=["POST"])
def api_salvar_config():
    """Salva configurações da Evolution API e parâmetros globais no arquivo .env."""
    body = request.get_json(silent=True) or {}
    env_path = Path(__file__).resolve().parent / ".env"
    atuais: Dict[str, str] = {}
    if env_path.exists():
        for ln in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in ln and not ln.strip().startswith("#"):
                k, v = ln.split("=", 1)
                atuais[k.strip()] = v.strip()

    for k, v in body.items():
        val = str(v or "").strip()
        if val:
            atuais[k] = val
            os.environ[k] = val

    linhas = [f"{k}={v}" for k, v in atuais.items()]
    env_path.write_text("\n".join(linhas) + "\n", encoding="utf-8")
    return jsonify({"ok": True}), 200


@app.route("/health", methods=["GET"])
def health():
    """Health check."""
    return jsonify({"status": "healthy", "engine": "evolution-api"}), 200


@app.route("/stats", methods=["GET"])
def get_stats():
    """Obter estatísticas da campanha."""
    try:
        stats = {
            "total": 0,
            "novo": 0,
            "enviado": 0,
            "interessado": 0,
            "respondeu": 0,
            "opt_out": 0,
            "erro": 0,
        }

        if os.path.exists(LEADS_CSV):
            with open(LEADS_CSV, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row.pop(None, None)
                    stats["total"] += 1
                    status = row.get("status", "novo") or "novo"
                    if status in stats:
                        stats[status] += 1
                    else:
                        stats[status] = 1

        return jsonify(stats), 200

    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        return jsonify({"error": str(e)}), 500


def main() -> None:
    """Função principal."""
    import webbrowser
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("DEBUG", "False").lower() == "true"

    logger.info(f"Iniciando Assistant to the Lead Manager (Evolution API 5x Instâncias) na porta {port}")
    logger.info(f"Painel Web: http://localhost:{port}/")
    logger.info(f"Webhook Evolution API: http://localhost:{port}/webhook")

    try:
        c0 = load_whatsapp_accounts()[0]
        teste_qr = evolution_create_or_connect_instance(c0)
        logger.info(
            f"[Auto-Check Evolution API] Instancia={teste_qr.get('instance')} | "
            f"ok={teste_qr.get('ok')} | state={teste_qr.get('state')} | "
            f"qr_len={len(teste_qr.get('qrcode_base64') or '')} | err={teste_qr.get('error', '')}"
        )
    except Exception as exc:
        logger.warning(f"[Auto-Check Evolution API] Aviso: {exc}")

    try:
        webbrowser.open(f"http://localhost:{port}/")
    except Exception:
        pass

    app.run(host="0.0.0.0", port=port, debug=debug)


if __name__ == "__main__":
    main()
