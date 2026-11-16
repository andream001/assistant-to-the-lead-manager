@echo off
chcp 65001 >nul
title Assistant to the Lead Manager - Evolution API (5 Chips)
cd /d "%~dp0"

echo ============================================================================
echo   LIGANDO EVOLUTION API (DOCKER PORTA 8081) E PAINEL WEB (PORTA 5000)
echo ============================================================================
echo.

echo [1/2] Subindo containers da Evolution API (PostgreSQL + Redis + Evolution)...
docker compose -f docker-compose.evolution.yml up -d

echo.
echo [2/2] Iniciando Painel Web e Webhook Receiver em http://localhost:5000 ...
echo (Mantenha esta janela aberta enquanto estiver usando o sistema)
echo.
python webhook_receiver.py
pause
