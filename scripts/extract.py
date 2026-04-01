"""
extract.py
----------
Etapa de EXTRAÇÃO da ETL.

Responsabilidades:
  - Ler o CSV fonte em chunks (para suportar grandes volumes)
  - Registrar checkpoint de progresso (para retomar após falha)
  - Logar métricas de leitura
"""

import csv
import json
import logging
import os
import time
from datetime import datetime

# ── Configuração de logging ──────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Caminhos padrão ──────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(__file__))
CHECKPOINT_PATH = os.path.join(BASE_DIR, 'output', 'checkpoint.json')
LOGS_DIR        = os.path.join(BASE_DIR, 'logs')


# ── Checkpoint ───────────────────────────────────────────────────

def _load_checkpoint() -> dict:
    """Carrega o checkpoint salvo (se existir)."""
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, 'r', encoding='utf-8') as f:
            cp = json.load(f)
        logger.info(f"[extract] Checkpoint encontrado: {cp}")
        return cp
    return {}


def _save_checkpoint(data: dict):
    """Persiste o estado atual do checkpoint."""
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    with open(CHECKPOINT_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def reset_checkpoint():
    """Remove o checkpoint para forçar execução do zero."""
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)
        logger.info("[extract] Checkpoint removido — execução iniciará do zero.")


# ── Extração principal ───────────────────────────────────────────

def extract(source_path: str, chunk_size: int = 100) -> list[dict]:
    """
    Lê o CSV fonte em chunks e retorna todos os registros como lista de dicts.

    Parâmetros
    ----------
    source_path : str
        Caminho para o arquivo CSV de origem.
    chunk_size : int
        Quantidade de linhas processadas por chunk (útil para simular
        leitura incremental de grandes volumes).

    Retorna
    -------
    list[dict]  — todos os registros extraídos.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"[extract] Arquivo não encontrado: {source_path}")

    checkpoint = _load_checkpoint()
    start_row  = checkpoint.get('last_row_extracted', 0)

    if start_row > 0:
        logger.warning(
            f"[extract] Retomando extração a partir da linha {start_row} "
            f"(checkpoint de {checkpoint.get('timestamp','?')})."
        )

    start_time = time.time()
    registros  = []

    try:
        with open(source_path, 'r', encoding='utf-8') as f:
            reader   = csv.DictReader(f)
            todas    = list(reader)

        total = len(todas)
        logger.info(f"[extract] Total de linhas no arquivo: {total}")

        # Retoma de onde parou
        pendentes = todas[start_row:]

        for i in range(0, len(pendentes), chunk_size):
            chunk = pendentes[i: i + chunk_size]
            registros.extend(chunk)

            rows_lidas = start_row + i + len(chunk)

            # Simula checkpoint incremental
            _save_checkpoint({
                'last_row_extracted': rows_lidas,
                'timestamp': datetime.now().isoformat(),
                'source': source_path,
                'status': 'in_progress',
            })

            logger.debug(f"[extract] Chunk {i // chunk_size + 1}: "
                         f"{rows_lidas}/{total} linhas extraídas.")

    except Exception as exc:
        logger.error(f"[extract] Erro durante leitura: {exc}")
        _save_checkpoint({
            **checkpoint,
            'status': 'error',
            'error': str(exc),
            'timestamp': datetime.now().isoformat(),
        })
        raise

    elapsed = time.time() - start_time

    # Checkpoint final — marca extração completa
    _save_checkpoint({
        'last_row_extracted': len(todas),
        'timestamp': datetime.now().isoformat(),
        'source': source_path,
        'status': 'extraction_complete',
    })

    metrics = {
        'etapa': 'extract',
        'total_lido': len(registros),
        'tempo_segundos': round(elapsed, 4),
        'linhas_ignoradas_por_checkpoint': start_row,
    }
    logger.info(f"[extract] Métricas: {metrics}")

    return registros, metrics
