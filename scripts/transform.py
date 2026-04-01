"""
transform.py
------------
Etapa de TRANSFORMAÇÃO da ETL.

Responsabilidades:
  - Normalização de dados (nomes, órgãos, datas, salários)
  - Padronização de formatos (CPF, telefone, e-mail, datas → ISO 8601)
  - Integridade de dados (FK de órgãos, unicidade de matrícula/CPF)
  - Detecção e quarentena de registros com erro
  - Métricas de qualidade
"""

import csv
import logging
import os
import re
import time
import unicodedata
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_DIR        = os.path.dirname(os.path.dirname(__file__))
QUARENTENA_PATH = os.path.join(BASE_DIR, 'output', 'quarentena.csv')
RELATORIO_PATH  = os.path.join(BASE_DIR, 'output', 'relatorio_erros.csv')


# ── Tabela de referência de órgãos (integridade referencial) ─────
ORGAOS_VALIDOS = {
    "CGE","PGE","SEAD","ECONOMIA","DETRAN","SSP","CBM","DGPP","DGPC",
    "SES","PM","SEDF","FAPEG","SECOM","VICEGOV","GOIAS TURISMO","SERINT",
    "SEINFRA","CASA CIVIL","SEAPA","SECTI","JUCEG","AGR","SIC","RETOMADA",
    "SECULT","SEEL","ABC","SECAMI","SEMAD","SGG","GOINFRA","EMATER",
    "DPE-GO","SEDS","AGRODEFESA","UEG","SEDUC","GOIASPREV",
}


# ════════════════════════════════════════════════════════════════
#  FUNÇÕES DE NORMALIZAÇÃO
# ════════════════════════════════════════════════════════════════

def _strip_accents(text: str) -> str:
    """Remove acentos para comparação case-insensitive."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


def normalizar_nome(raw) -> tuple[str | None, list[str]]:
    """
    Normaliza nome: strip, title case, colapsa espaços múltiplos.
    Retorna (valor_normalizado, lista_de_alertas).
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['nome_nulo']
    nome = str(raw).strip()
    nome = re.sub(r'\s+', ' ', nome)        # colapsa espaços múltiplos
    nome = nome.title()                      # Title Case
    if len(nome) < 5:
        erros.append('nome_muito_curto')
    return nome, erros


def normalizar_orgao(raw) -> tuple[str | None, list[str]]:
    """
    Normaliza sigla do órgão: strip, uppercase.
    Valida contra tabela de referência (integridade referencial).
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['orgao_nulo']
    orgao = str(raw).strip().upper()
    if orgao not in ORGAOS_VALIDOS:
        erros.append(f'orgao_invalido:{orgao}')
        return orgao, erros
    return orgao, erros


def normalizar_data(raw, campo='data') -> tuple[str | None, list[str]]:
    """
    Tenta parsear a data em múltiplos formatos e retorna ISO 8601 (YYYY-MM-DD).
    Formatos suportados: dd/mm/yyyy, yyyy-mm-dd, dd-mm-yyyy.
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, [f'{campo}_nulo']
    raw = str(raw).strip()
    formatos = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%y', '%Y/%m/%d']
    for fmt in formatos:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime('%Y-%m-%d'), erros
        except ValueError:
            continue
    erros.append(f'{campo}_formato_invalido:{raw}')
    return None, erros


def normalizar_salario(raw) -> tuple[float | None, list[str]]:
    """
    Normaliza salário removendo 'R$', espaços, e convertendo vírgula → ponto.
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['salario_nulo']
    s = str(raw).strip()
    s = re.sub(r'[Rr]\$\s*', '', s)   # remove R$
    s = s.replace(' ', '')
    # Se tem vírgula como separador decimal (ex: 1.234,56 ou 1234,56)
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        valor = round(float(s), 2)
        if valor < 0:
            erros.append('salario_negativo')
        return valor, erros
    except ValueError:
        erros.append(f'salario_formato_invalido:{raw}')
        return None, erros


def normalizar_cpf(raw) -> tuple[str | None, list[str]]:
    """
    Normaliza CPF para formato 000.000.000-00.
    Valida comprimento (11 dígitos).
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['cpf_nulo']
    digitos = re.sub(r'\D', '', str(raw))
    if len(digitos) != 11:
        erros.append(f'cpf_tamanho_invalido:{raw}')
        return None, erros
    cpf_fmt = f"{digitos[:3]}.{digitos[3:6]}.{digitos[6:9]}-{digitos[9:]}"
    return cpf_fmt, erros


def normalizar_email(raw) -> tuple[str | None, list[str]]:
    """Valida formato básico de e-mail via regex."""
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['email_nulo']
    email = str(raw).strip().lower()
    padrao = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    if not re.match(padrao, email):
        erros.append(f'email_invalido:{email}')
        return None, erros
    return email, erros


def normalizar_telefone(raw) -> tuple[str | None, list[str]]:
    """
    Normaliza telefone para formato (XX) 9XXXX-XXXX.
    """
    erros = []
    if not raw or str(raw).strip() == '':
        return None, []   # telefone é opcional — não gera erro crítico
    digitos = re.sub(r'\D', '', str(raw))
    if len(digitos) == 11:
        return f"({digitos[:2]}) {digitos[2:7]}-{digitos[7:]}", erros
    elif len(digitos) == 10:
        return f"({digitos[:2]}) {digitos[2:6]}-{digitos[6:]}", erros
    else:
        erros.append(f'telefone_formato_invalido:{raw}')
        return None, erros


def normalizar_sexo(raw) -> tuple[str | None, list[str]]:
    erros = []
    if not raw or str(raw).strip() == '':
        return None, ['sexo_nulo']
    s = str(raw).strip().upper()
    if s not in ('M', 'F'):
        erros.append(f'sexo_invalido:{raw}')
        return None, erros
    return s, erros


# ════════════════════════════════════════════════════════════════
#  TRANSFORMAÇÃO PRINCIPAL
# ════════════════════════════════════════════════════════════════

# Campos que, se nulos após normalização, tornam o registro inválido (crítico)
CAMPOS_CRITICOS = {'matricula', 'nome', 'cpf', 'orgao'}


def transformar_registro(raw: dict, idx: int) -> tuple[dict | None, dict | None]:
    """
    Transforma um registro bruto.

    Retorna
    -------
    (registro_limpo, erro_registro)
      - Se válido: (dict_normalizado, None)
      - Se inválido: (None, dict_de_erro)
    """
    erros_acumulados = {}
    r = {}

    # ── Matrícula ────────────────────────────────────────────
    matricula = str(raw.get('matricula', '')).strip().upper()
    if not matricula:
        erros_acumulados['matricula'] = 'matricula_nula'
    else:
        r['matricula'] = matricula

    # ── Nome ────────────────────────────────────────────────
    nome, err = normalizar_nome(raw.get('nome'))
    r['nome'] = nome
    if err:
        erros_acumulados['nome'] = '|'.join(err)

    # ── CPF ─────────────────────────────────────────────────
    cpf, err = normalizar_cpf(raw.get('cpf'))
    r['cpf'] = cpf
    if err:
        erros_acumulados['cpf'] = '|'.join(err)

    # ── Data de Nascimento ───────────────────────────────────
    dn, err = normalizar_data(raw.get('data_nascimento'), 'data_nascimento')
    r['data_nascimento'] = dn
    if err:
        erros_acumulados['data_nascimento'] = '|'.join(err)

    # ── Sexo ─────────────────────────────────────────────────
    sexo, err = normalizar_sexo(raw.get('sexo'))
    r['sexo'] = sexo
    if err:
        erros_acumulados['sexo'] = '|'.join(err)

    # ── Escolaridade ─────────────────────────────────────────
    esc = str(raw.get('escolaridade', '')).strip().title()
    r['escolaridade'] = esc if esc else None

    # ── Cargo ────────────────────────────────────────────────
    cargo = str(raw.get('cargo', '')).strip().title()
    r['cargo'] = cargo if cargo else None

    # ── Órgão ────────────────────────────────────────────────
    orgao, err = normalizar_orgao(raw.get('orgao'))
    r['orgao'] = orgao
    if err:
        erros_acumulados['orgao'] = '|'.join(err)

    # ── Vínculo ──────────────────────────────────────────────
    vinculo = str(raw.get('vinculo', '')).strip().title()
    r['vinculo'] = vinculo if vinculo else None

    # ── Data de Admissão ─────────────────────────────────────
    da, err = normalizar_data(raw.get('data_admissao'), 'data_admissao')
    r['data_admissao'] = da
    if err:
        erros_acumulados['data_admissao'] = '|'.join(err)

    # ── Salário ──────────────────────────────────────────────
    sal, err = normalizar_salario(raw.get('salario_bruto'))
    r['salario_bruto'] = sal
    if err:
        erros_acumulados['salario_bruto'] = '|'.join(err)

    # ── E-mail ───────────────────────────────────────────────
    email, err = normalizar_email(raw.get('email'))
    r['email'] = email
    if err:
        erros_acumulados['email'] = '|'.join(err)

    # ── Telefone ─────────────────────────────────────────────
    tel, err = normalizar_telefone(raw.get('telefone'))
    r['telefone'] = tel
    if err:
        erros_acumulados['telefone'] = '|'.join(err)

    # ── Ativo ────────────────────────────────────────────────
    ativo_raw = str(raw.get('ativo', '1')).strip()
    r['ativo'] = 1 if ativo_raw in ('1', 'true', 'True', 'sim', 'Sim') else 0

    # ── Metadado de auditoria ────────────────────────────────
    r['etl_carregado_em'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    r['etl_linha_origem'] = idx

    # ── Decide se o registro vai para quarentena ──────────────
    campos_com_erro = set(erros_acumulados.keys())
    if campos_com_erro & CAMPOS_CRITICOS:
        erro_registro = {
            'linha_origem': idx,
            'matricula_raw': raw.get('matricula'),
            'nome_raw': raw.get('nome'),
            'cpf_raw': raw.get('cpf'),
            'orgao_raw': raw.get('orgao'),
            'erros': str(erros_acumulados),
        }
        return None, erro_registro

    # Registros com erros não-críticos: carregam com campos nulos, mas são logados
    if erros_acumulados:
        logger.warning(f"[transform] Linha {idx} — erros não-críticos: {erros_acumulados}")

    return r, None


def transform(registros: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """
    Aplica transformações em todos os registros.

    Retorna
    -------
    (registros_validos, registros_quarentena, metricas)
    """
    start_time = time.time()

    validos      = []
    quarentena   = []
    cpfs_vistos  = {}     # cpf → primeira linha (deduplicação)
    mats_vistas  = {}     # matricula → primeira linha

    for idx, raw in enumerate(registros, start=1):
        limpo, erro = transformar_registro(raw, idx)

        if erro:
            quarentena.append(erro)
            continue

        # ── Deduplicação de CPF ──────────────────────────────
        cpf = limpo.get('cpf')
        if cpf:
            if cpf in cpfs_vistos:
                quarentena.append({
                    'linha_origem': idx,
                    'matricula_raw': limpo.get('matricula'),
                    'nome_raw': limpo.get('nome'),
                    'cpf_raw': cpf,
                    'orgao_raw': limpo.get('orgao'),
                    'erros': f"cpf_duplicado:primeira_ocorrencia_linha_{cpfs_vistos[cpf]}",
                })
                continue
            cpfs_vistos[cpf] = idx

        # ── Deduplicação de Matrícula ────────────────────────
        mat = limpo.get('matricula')
        if mat:
            if mat in mats_vistas:
                quarentena.append({
                    'linha_origem': idx,
                    'matricula_raw': mat,
                    'nome_raw': limpo.get('nome'),
                    'cpf_raw': cpf,
                    'orgao_raw': limpo.get('orgao'),
                    'erros': f"matricula_duplicada:primeira_ocorrencia_linha_{mats_vistas[mat]}",
                })
                continue
            mats_vistas[mat] = idx

        validos.append(limpo)

    elapsed = time.time() - start_time

    # ── Salva relatório de quarentena ────────────────────────
    _salvar_quarentena(quarentena)

    metricas = {
        'etapa': 'transform',
        'total_entrada': len(registros),
        'total_validos': len(validos),
        'total_quarentena': len(quarentena),
        'taxa_rejeicao_pct': round(len(quarentena) / max(len(registros), 1) * 100, 2),
        'tempo_segundos': round(elapsed, 4),
    }
    logger.info(f"[transform] Métricas: {metricas}")
    return validos, quarentena, metricas


def _salvar_quarentena(quarentena: list[dict]):
    """Persiste registros inválidos em CSV para análise posterior."""
    if not quarentena:
        logger.info("[transform] Nenhum registro em quarentena.")
        return
    os.makedirs(os.path.dirname(QUARENTENA_PATH), exist_ok=True)
    campos = ['linha_origem', 'matricula_raw', 'nome_raw', 'cpf_raw', 'orgao_raw', 'erros']
    with open(QUARENTENA_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(quarentena)
    logger.info(f"[transform] {len(quarentena)} registros salvos em quarentena: {QUARENTENA_PATH}")
