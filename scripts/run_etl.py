"""
run_etl.py
----------
Orquestrador principal da ETL de Pessoas — VOBYS / Estado de Goiás.

Fluxo:
  1. Gera dados fictícios (se CSV não existir ou --gerar-dados for passado)
  2. Extrai do CSV com suporte a checkpoint (retomada após falha)
  3. Transforma: normaliza, valida, deduplica
  4. Carrega no SQLite (schema normalizado, UPSERT)
  5. Gera relatório de execução

Uso:
  python run_etl.py                          # execução normal (modo demo se CSV não existir)
  python run_etl.py --reset                  # apaga checkpoint e reexecuta do zero
  python run_etl.py --gerar-dados            # regenera CSV no modo demo (500 registros)
  python run_etl.py --gerar-dados --modo completo  # regenera CSV com 166.261 registros (Anexo I)

No Notebook (para evitar conflito com argumentos do Jupyter):
  import sys
  sys.argv = ['run_etl.py']                          # modo demo
  sys.argv = ['run_etl.py', '--modo', 'completo']    # modo completo
"""

import argparse
import logging
import os
import sys

# Garante que o diretório scripts está no path
sys.path.insert(0, os.path.dirname(__file__))

from generate_data import gerar_dataset_demo, gerar_dataset_completo, salvar_csv
from extract       import extract, reset_checkpoint
from transform     import transform
from load          import load
from report        import gerar_relatorio

# ── Configuração de logging ──────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOG_PATH = os.path.join(BASE_DIR, 'logs', 'etl.log')
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ── Caminhos ────────────────────────────────────────────────────
CSV_PATH = os.path.join(BASE_DIR, 'data', 'servidores_raw.csv')


def main():
    parser = argparse.ArgumentParser(description='ETL de Pessoas — VOBYS/Goiás')
    parser.add_argument('--reset',       action='store_true', help='Remove checkpoint e reinicia do zero')
    parser.add_argument('--gerar-dados', action='store_true', help='Regenera o CSV de dados fictícios')
    parser.add_argument(
        '--modo',
        choices=['demo', 'completo'],
        default='demo',
        help=(
            "'demo' = 500 registros com órgãos aleatórios (padrão, para apresentação) | "
            "'completo' = 166.261 registros exatos por órgão conforme Anexo I"
        )
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  INICIANDO ETL DE PESSOAS — VOBYS / ESTADO DE GOIÁS")
    logger.info(f"  MODO: {args.modo.upper()}")
    logger.info("=" * 60)

    # ── Passo 0: Reset (opcional) ────────────────────────────────
    if args.reset:
        reset_checkpoint()
        logger.info("[main] Modo --reset: checkpoint apagado.")

    # ── Passo 1: Geração de dados fictícios ──────────────────────
    if args.gerar_dados or not os.path.exists(CSV_PATH):
        logger.info(f"[main] Gerando dados fictícios — modo '{args.modo}'...")
        if args.modo == 'completo':
            dados = gerar_dataset_completo()
        else:
            dados = gerar_dataset_demo(n=500)
        salvar_csv(dados, CSV_PATH)
    else:
        logger.info(f"[main] CSV já existe: {CSV_PATH} — pulando geração.")

    # ── Passo 2: Extract ─────────────────────────────────────────
    chunk = 5000 if args.modo == 'completo' else 100
    logger.info("[main] ── ETAPA 1/3: EXTRACT ──")
    try:
        registros_raw, metricas_extract = extract(CSV_PATH, chunk_size=chunk)
    except Exception as e:
        logger.critical(f"[main] Falha na extração: {e}")
        sys.exit(1)

    # ── Passo 3: Transform ───────────────────────────────────────
    logger.info("[main] ── ETAPA 2/3: TRANSFORM ──")
    try:
        registros_validos, quarentena, metricas_transform = transform(registros_raw)
    except Exception as e:
        logger.critical(f"[main] Falha na transformação: {e}")
        sys.exit(1)

    # ── Passo 4: Load ────────────────────────────────────────────
    logger.info("[main] ── ETAPA 3/3: LOAD ──")
    metricas_anteriores = {
        'extract':   metricas_extract,
        'transform': metricas_transform,
    }
    try:
        metricas_load = load(registros_validos, metricas_anteriores)
    except Exception as e:
        logger.critical(f"[main] Falha na carga: {e}")
        sys.exit(1)

    # ── Passo 5: Relatório ───────────────────────────────────────
    logger.info("[main] ── GERANDO RELATÓRIO ──")
    metricas_completas = {
        'extract':   metricas_extract,
        'transform': metricas_transform,
        'load':      metricas_load,
    }
    gerar_relatorio(metricas_completas)

    logger.info("[main] ETL concluída com sucesso.")
    return metricas_completas


if __name__ == '__main__':
    main()
