@echo off
chcp 65001 >nul
title Desligando Assistant to the Lead Manager e Evolution API
cd /d "%~dp0"

echo ============================================================================
echo   DESLIGANDO INSTANCIAS EVOLUTION API E SERVIDOR WEB (PORTA 5000)
echo ============================================================================
echo.

echo [1/2] Encerrando servidor Python na porta 5000...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5000" ^| findstr "LISTENING"') do (
    echo Finalizando PID %%a ...
    taskkill /F /PID %%a >nul 2>&1
)

echo [2/2] Parando containers Docker da Evolution API (sessoes ficam salvas)...
docker compose -f docker-compose.evolution.yml stop

echo.
echo [OK] Todas as instancias e servidores foram desligados com seguranca!
timeout /t 4
