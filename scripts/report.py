"""
report.py
---------
Gera o relatório consolidado de execução ETL em CSV.

Inclui:
  - Métricas por etapa (Extract, Transform, Load)
  - Resumo de qualidade de dados
  - Timestamp da execução
"""

import csv
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_DIR       = os.path.dirname(os.path.dirname(__file__))
RELATORIO_PATH = os.path.join(BASE_DIR, 'output', 'relatorio_execucao.csv')


def gerar_relatorio(metricas: dict):
    """
    Recebe o dicionário consolidado de métricas e salva CSV de relatório.

    Parâmetro
    ---------
    metricas : {
        'extract':   {...},
        'transform': {...},
        'load':      {...},
    }
    """
    os.makedirs(os.path.dirname(RELATORIO_PATH), exist_ok=True)

    m_ext = metricas.get('extract',   {})
    m_trn = metricas.get('transform', {})
    m_ld  = metricas.get('load',      {})

    total_tempo = round(
        m_ext.get('tempo_segundos', 0) +
        m_trn.get('tempo_segundos', 0) +
        m_ld.get('tempo_segundos',  0), 4
    )

    linhas = [
        # ── Identificação da execução ──────────────────────
        {'metrica': 'execucao_timestamp',      'valor': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'etapa': 'geral'},
        {'metrica': 'tempo_total_segundos',    'valor': total_tempo,                                  'etapa': 'geral'},

        # ── Extract ────────────────────────────────────────
        {'metrica': 'registros_extraidos',     'valor': m_ext.get('total_lido', 0),                  'etapa': 'extract'},
        {'metrica': 'linhas_ignoradas_checkpoint','valor': m_ext.get('linhas_ignoradas_por_checkpoint', 0), 'etapa': 'extract'},
        {'metrica': 'tempo_extract_segundos',  'valor': m_ext.get('tempo_segundos', 0),               'etapa': 'extract'},

        # ── Transform ──────────────────────────────────────
        {'metrica': 'registros_validos',       'valor': m_trn.get('total_validos', 0),                'etapa': 'transform'},
        {'metrica': 'registros_quarentena',    'valor': m_trn.get('total_quarentena', 0),             'etapa': 'transform'},
        {'metrica': 'taxa_rejeicao_pct',       'valor': m_trn.get('taxa_rejeicao_pct', 0),            'etapa': 'transform'},
        {'metrica': 'tempo_transform_segundos','valor': m_trn.get('tempo_segundos', 0),               'etapa': 'transform'},

        # ── Load ───────────────────────────────────────────
        {'metrica': 'registros_inseridos',     'valor': m_ld.get('inseridos', 0),                     'etapa': 'load'},
        {'metrica': 'registros_atualizados',   'valor': m_ld.get('atualizados', 0),                   'etapa': 'load'},
        {'metrica': 'erros_carga',             'valor': m_ld.get('erros_carga', 0),                   'etapa': 'load'},
        {'metrica': 'tempo_load_segundos',     'valor': m_ld.get('tempo_segundos', 0),                'etapa': 'load'},
        {'metrica': 'banco_destino',           'valor': m_ld.get('banco', ''),                        'etapa': 'load'},
    ]

    with open(RELATORIO_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['etapa', 'metrica', 'valor'])
        writer.writeheader()
        writer.writerows(linhas)

    logger.info(f"[report] Relatório salvo em: {RELATORIO_PATH}")

    # Imprime resumo no console
    print("\n" + "═" * 55)
    print("  RELATÓRIO DE EXECUÇÃO ETL — VOBYS / Estado de Goiás")
    print("═" * 55)
    print(f"  {'Registros extraídos':<30} {m_ext.get('total_lido', 0):>8}")
    print(f"  {'Registros válidos':<30} {m_trn.get('total_validos', 0):>8}")
    print(f"  {'Registros em quarentena':<30} {m_trn.get('total_quarentena', 0):>8}")
    print(f"  {'Taxa de rejeição':<30} {m_trn.get('taxa_rejeicao_pct', 0):>7.1f}%")
    print(f"  {'Inseridos no banco':<30} {m_ld.get('inseridos', 0):>8}")
    print(f"  {'Atualizados no banco':<30} {m_ld.get('atualizados', 0):>8}")
    print(f"  {'Tempo total (s)':<30} {total_tempo:>8.2f}")
    print("═" * 55 + "\n")

    return RELATORIO_PATH
