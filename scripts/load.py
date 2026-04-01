"""
load.py
-------
Etapa de CARGA da ETL.

Responsabilidades:
  - Criar schema normalizado no SQLite (simula Oracle)
  - Inserir registros válidos com controle de duplicatas (UPSERT)
  - Registrar métricas de carga
  - Garantir integridade referencial via FK

Schema (3FN):
  orgaos (id, sigla)
  vinculos (id, descricao)
  cargos (id, descricao)
  escolaridades (id, descricao)
  servidores (id, matricula, nome, cpf, ..., id_orgao FK, id_vinculo FK, ...)
  etl_execucoes (log de cada execução)
"""

import logging
import os
import sqlite3
import time
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH  = os.path.join(BASE_DIR, 'output', 'gestao_pessoas.db')


# ════════════════════════════════════════════════════════════════
#  DDL — Schema Normalizado (3FN)
# ════════════════════════════════════════════════════════════════

DDL = """
PRAGMA foreign_keys = ON;

-- Tabela de referência: Órgãos
CREATE TABLE IF NOT EXISTS orgaos (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    sigla  TEXT    NOT NULL UNIQUE
);

-- Tabela de referência: Vínculos empregatícios
CREATE TABLE IF NOT EXISTS vinculos (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    descricao  TEXT    NOT NULL UNIQUE
);

-- Tabela de referência: Cargos
CREATE TABLE IF NOT EXISTS cargos (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    descricao  TEXT    NOT NULL UNIQUE
);

-- Tabela de referência: Escolaridades
CREATE TABLE IF NOT EXISTS escolaridades (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    descricao  TEXT    NOT NULL UNIQUE
);

-- Tabela principal: Servidores
CREATE TABLE IF NOT EXISTS servidores (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    matricula         TEXT    NOT NULL UNIQUE,
    nome              TEXT    NOT NULL,
    cpf               TEXT    NOT NULL UNIQUE,
    data_nascimento   TEXT,
    sexo              TEXT    CHECK(sexo IN ('M','F')),
    id_escolaridade   INTEGER REFERENCES escolaridades(id),
    id_cargo          INTEGER REFERENCES cargos(id),
    id_orgao          INTEGER NOT NULL REFERENCES orgaos(id),
    id_vinculo        INTEGER REFERENCES vinculos(id),
    data_admissao     TEXT,
    salario_bruto     REAL,
    email             TEXT,
    telefone          TEXT,
    ativo             INTEGER DEFAULT 1 CHECK(ativo IN (0,1)),
    etl_carregado_em  TEXT,
    etl_linha_origem  INTEGER
);

-- Log de execuções ETL
CREATE TABLE IF NOT EXISTS etl_execucoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    iniciado_em     TEXT,
    finalizado_em   TEXT,
    total_extraido  INTEGER,
    total_valido    INTEGER,
    total_carregado INTEGER,
    total_erros     INTEGER,
    status          TEXT
);
"""


# ════════════════════════════════════════════════════════════════
#  FUNÇÕES AUXILIARES
# ════════════════════════════════════════════════════════════════

def _get_or_create(conn: sqlite3.Connection, tabela: str, campo: str, valor: str) -> int | None:
    """
    Retorna o id de um registro em tabela de referência,
    criando-o se não existir (INSERT OR IGNORE).
    """
    if not valor:
        return None
    conn.execute(
        f"INSERT OR IGNORE INTO {tabela} ({campo}) VALUES (?)", (valor,)
    )
    cur = conn.execute(f"SELECT id FROM {tabela} WHERE {campo} = ?", (valor,))
    row = cur.fetchone()
    return row[0] if row else None


# ════════════════════════════════════════════════════════════════
#  CARGA PRINCIPAL
# ════════════════════════════════════════════════════════════════

def load(registros: list[dict], metricas_anteriores: dict) -> dict:
    """
    Carrega os registros transformados no banco SQLite.

    Parâmetros
    ----------
    registros            : lista de dicts normalizados (saída de transform)
    metricas_anteriores  : dict com métricas das etapas Extract e Transform

    Retorna
    -------
    dict com métricas completas da execução ETL
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    start_time  = time.time()
    inseridos   = 0
    atualizados = 0
    erros_carga = 0
    iniciado_em = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Aplica DDL
        conn.executescript(DDL)
        conn.commit()
        logger.info(f"[load] Banco inicializado: {DB_PATH}")

        for reg in registros:
            try:
                # Resolve FKs nas tabelas de referência
                id_orgao       = _get_or_create(conn, 'orgaos',       'sigla',     reg.get('orgao'))
                id_vinculo     = _get_or_create(conn, 'vinculos',     'descricao', reg.get('vinculo'))
                id_cargo       = _get_or_create(conn, 'cargos',       'descricao', reg.get('cargo'))
                id_escolar     = _get_or_create(conn, 'escolaridades','descricao', reg.get('escolaridade'))

                if not id_orgao:
                    logger.warning(f"[load] Registro {reg.get('matricula')} sem órgão válido — pulado.")
                    erros_carga += 1
                    continue

                # UPSERT: insere ou atualiza pelo CPF
                cur = conn.execute(
                    "SELECT id FROM servidores WHERE cpf = ? OR matricula = ?",
                    (reg.get('cpf'), reg.get('matricula'))
                )
                existente = cur.fetchone()

                if existente:
                    conn.execute("""
                        UPDATE servidores SET
                            matricula        = ?,
                            nome             = ?,
                            data_nascimento  = ?,
                            sexo             = ?,
                            id_escolaridade  = ?,
                            id_cargo         = ?,
                            id_orgao         = ?,
                            id_vinculo       = ?,
                            data_admissao    = ?,
                            salario_bruto    = ?,
                            email            = ?,
                            telefone         = ?,
                            ativo            = ?,
                            etl_carregado_em = ?,
                            etl_linha_origem = ?
                        WHERE id = ?
                    """, (
                        reg.get('matricula'),
                        reg.get('nome'),
                        reg.get('data_nascimento'),
                        reg.get('sexo'),
                        id_escolar,
                        id_cargo,
                        id_orgao,
                        id_vinculo,
                        reg.get('data_admissao'),
                        reg.get('salario_bruto'),
                        reg.get('email'),
                        reg.get('telefone'),
                        reg.get('ativo'),
                        reg.get('etl_carregado_em'),
                        reg.get('etl_linha_origem'),
                        existente['id'],
                    ))
                    atualizados += 1
                else:
                    conn.execute("""
                        INSERT INTO servidores (
                            matricula, nome, cpf, data_nascimento, sexo,
                            id_escolaridade, id_cargo, id_orgao, id_vinculo,
                            data_admissao, salario_bruto, email, telefone,
                            ativo, etl_carregado_em, etl_linha_origem
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        reg.get('matricula'),
                        reg.get('nome'),
                        reg.get('cpf'),
                        reg.get('data_nascimento'),
                        reg.get('sexo'),
                        id_escolar,
                        id_cargo,
                        id_orgao,
                        id_vinculo,
                        reg.get('data_admissao'),
                        reg.get('salario_bruto'),
                        reg.get('email'),
                        reg.get('telefone'),
                        reg.get('ativo'),
                        reg.get('etl_carregado_em'),
                        reg.get('etl_linha_origem'),
                    ))
                    inseridos += 1

            except sqlite3.Error as e:
                erros_carga += 1
                logger.error(f"[load] Erro ao inserir linha {reg.get('etl_linha_origem')}: {e}")

        conn.commit()

    except Exception as exc:
        logger.critical(f"[load] Falha crítica: {exc}")
        raise
    finally:
        conn.close()

    elapsed      = time.time() - start_time
    finalizado_em = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    metricas_carga = {
        'etapa': 'load',
        'inseridos': inseridos,
        'atualizados': atualizados,
        'erros_carga': erros_carga,
        'tempo_segundos': round(elapsed, 4),
        'banco': DB_PATH,
    }
    logger.info(f"[load] Métricas: {metricas_carga}")

    # Registra log de execução no próprio banco
    _registrar_execucao(
        iniciado_em   = iniciado_em,
        finalizado_em = finalizado_em,
        total_extraido= metricas_anteriores.get('extract', {}).get('total_lido', 0),
        total_valido  = metricas_anteriores.get('transform', {}).get('total_validos', 0),
        total_carregado= inseridos + atualizados,
        total_erros   = metricas_anteriores.get('transform', {}).get('total_quarentena', 0) + erros_carga,
        status        = 'sucesso',
    )

    return metricas_carga


def _registrar_execucao(**kwargs):
    """Persiste o log desta execução na tabela etl_execucoes."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO etl_execucoes
                (iniciado_em, finalizado_em, total_extraido, total_valido,
                 total_carregado, total_erros, status)
            VALUES (?,?,?,?,?,?,?)
        """, (
            kwargs['iniciado_em'], kwargs['finalizado_em'],
            kwargs['total_extraido'], kwargs['total_valido'],
            kwargs['total_carregado'], kwargs['total_erros'],
            kwargs['status'],
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[load] Não foi possível registrar execução: {e}")
