# Assistant to the Lead Manager · Evolution API (5 Chips Simultâneos)

Sistema completo de **prospecção ativa, triagem automática via Webhook e repasse inteligente de leads B2B no WhatsApp**, equipado com interface web nativa, balanceamento **Round-Robin para até 5 chips simultâneos na Evolution API**, proteção anti-ban (*Jitter* + presença *"Digitando..."*) e **6 formas de importação de leads**.

---

## Principais Funcionalidades

1. **Pool Multi-Instância (Até 5 Chips Simultâneos via QR Code)**:
   - Conecte até 5 aparelhos (`chip_1` a `chip_5`) lendo o QR Code diretamente no painel web (`http://localhost:5000`).
   - Alternância **Round-Robin** automática entre os chips ativos e controle de cota diária individual (*Warm-Up*) por aparelho.
2. **Proteção Anti-Ban Nativa**:
   - **Jitter Aleatório**: Sorteia o intervalo entre cada envio (ex: entre `6s` e `15s`) para simular comportamento humano.
   - **Presença `"Digitando..."` (`composing`)**: Simula digitação antes do envio de cada mensagem.
   - **Trava de Janela Comercial**: Restringe disparos ao horário comercial (Seg–Sex, 8h às 18h), com opção `--force-hours` para testes.
3. **Central de Importação Inteligente (6 Formas de Entrada)**:
   - **📂 Upload de Arquivos**: `.XLSX` (Excel nativo), `.CSV`, `.VCF` (vCard exportado do celular Android/iPhone), `.JSON` e `.TXT`.
   - **📋 Copiar e Colar do Excel / Google Sheets**: Suporte direto a tabulação (`Ctrl+C` $\rightarrow$ `Ctrl+V`).
   - **🔍 Caçador Inteligente de Telefones (Regex Scraper)**: Extrai telefones brasileiros de textos brutos (Google Maps, PDFs, listas, sites).
   - **🏢 Enriquecimento por Lista de CNPJs (Receita Federal / BrasilAPI)**: Consulta automaticamente Razão Social / Nome Fantasia e telefones registrados do CNPJ.
   - **🌐 Link do Google Sheets / URL CSV**: Converte links compartilhados do Google Planilhas e importa automaticamente.
   - **📲 Sincronização da Agenda do Chip Conectado**: Importa contatos e conversas individuais direto da instância conectada na Evolution API.
4. **Receptor Webhook Automático (`MESSAGES_UPSERT`)**:
   - **Casamento Inteligente de DDI `+55` e 9º Dígito**: Identifica o lead na planilha mesmo quando o WhatsApp adiciona ou omite o nono dígito.
   - **Opção `1` (Interessado)**: Responde ao lead e envia alerta imediato para o WhatsApp do consultor responsável daquele chip com link clicável `https://wa.me/...`.
   - **Opção `2` (Opt-Out)**: Bloqueia permanentemente o contato contra novos disparos.
   - **Resposta Livre**: Encaminha a dúvida integral do cliente para o consultor assumir o atendimento.
5. **Explicação Interativa (`?`) & Modo Gravação (LGPD / DLP)**:
   - Botões de ajuda (`?` / `📖 Guia & Dicionário de Termos`) em todos os cartões e funções.
   - **Modo Gravação (`🔒 Ocultar Tel`)**: Mascara telefones na tela (`+5511•••••001`) para gravações, reuniões e demonstrações seguras.
   - **Proteção de Base Real Local**: Se existir um arquivo `data/leads.local.csv` (ignorado pelo Git), o sistema o utiliza automaticamente em vez do `data/leads.csv` de demonstração.

---

## Como Executar

### 1. Subir a Evolution API (Docker)

```bash
docker compose -f docker-compose.evolution.yml up -d
```

Isso iniciará a **Evolution API** na porta `8081` (com PostgreSQL e Redis integrados e Webhook Global pré-configurado para `http://host.docker.internal:5000/webhook`).

### 2. Iniciar o Painel Web e Webhook Receiver

```bash
pip install -r requirements.txt
python webhook_receiver.py
```

Acesse no navegador: **http://localhost:5000**

### 3. Disparo via Linha de Comando (Opcional)

```bash
# Modo simulacao segura (DRY-RUN) distribuindo entre os chips ativos
python send_campaigns.py --dry-run --limit 20

# Disparo real em uma instancia especifica
python send_campaigns.py --account conta_1 --limit 30
```

---

## Segurança e Privacidade (LGPD / DLP)

- O arquivo `data/leads.csv` versionado neste repositório contém **exclusivamente 10 registros fictícios de demonstração** (`5511900000001` a `5511900000010`).
- Bases reais (`data/leads.local.csv`), chaves (`.env`), sessões (`data/whatsapp_accounts.json`) e logs (`data/events.log`) são protegidos via `.gitignore`.
