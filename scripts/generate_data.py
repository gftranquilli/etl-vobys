"""
generate_data.py
----------------
Gera um CSV fictício de servidores públicos do estado de Goiás
com imperfeições intencionais para demonstrar as etapas de ETL.

Modos de geração:
  - DEMO    : 500 registros com órgãos sorteados aleatoriamente.
              Ideal para apresentação e testes rápidos.
  - COMPLETO: 166.261 registros respeitando exatamente a quantidade
              de servidores por órgão conforme o Anexo I do edital.

Imperfeições incluídas (propositalmente):
  - CPFs duplicados
  - Datas em formatos mistos (dd/mm/yyyy, yyyy-mm-dd, dd-mm-yyyy)
  - Campos nome em caixa mista / com espaços extras
  - Salários como string com vírgula ou ponto como separador decimal
  - E-mails inválidos
  - Campos nulos aleatórios
  - Órgãos com siglas inconsistentes (ex: 'seduc', 'SEDUC ', 'Seduc')
  - Matrículas duplicadas
"""

import csv
import random
import os
from datetime import date, timedelta

# ── Configurações ────────────────────────────────────────────────
random.seed(42)
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'servidores_raw.csv')

# Tabela exata do Anexo I — usada no modo COMPLETO
ORGAOS_SERVIDORES = {
    "CGE": 199, "PGE": 367, "SEAD": 2692, "ECONOMIA": 3108,
    "DETRAN": 1274, "SSP": 1586, "CBM": 2675, "DGPP": 3316,
    "DGPC": 4738, "SES": 7589, "PM": 11647, "SEDF": 38,
    "FAPEG": 78, "SECOM": 110, "VICEGOV": 111, "GOIAS TURISMO": 111,
    "SERINT": 129, "SEINFRA": 130, "CASA CIVIL": 138, "SEAPA": 152,
    "SECTI": 178, "JUCEG": 188, "AGR": 192, "SIC": 206,
    "RETOMADA": 253, "SECULT": 259, "SEEL": 385, "ABC": 390,
    "SECAMI": 460, "SEMAD": 522, "SGG": 577, "GOINFRA": 737,
    "EMATER": 828, "DPE-GO": 845, "SEDS": 966, "AGRODEFESA": 1004,
    "UEG": 2001, "SEDUC": 44981, "GOIASPREV": 71101,
}

# Lista simples de siglas — usada no modo DEMO
ORGAOS = list(ORGAOS_SERVIDORES.keys())

# Variações sujas de órgãos para simular inconsistência
def dirty_orgao(orgao):
    variants = [
        orgao,
        orgao.lower(),
        orgao.capitalize(),
        orgao + " ",
        " " + orgao,
    ]
    return random.choice(variants)

NOMES_PRIMEIRO = [
    "Ana","Carlos","Maria","João","Fernanda","Lucas","Patrícia","Rafael",
    "Juliana","Marcos","Beatriz","Pedro","Camila","Ricardo","Larissa",
    "Eduardo","Vanessa","Felipe","Aline","Bruno","Daniela","Thiago",
    "Renata","Gustavo","Priscila","Alexandre","Tatiane","Rodrigo","Simone",
    "Leandro","Cristina","Diego","Mônica","Henrique","Sandra","Vinicius",
]

NOMES_SOBRENOME = [
    "Silva","Santos","Oliveira","Souza","Rodrigues","Ferreira","Almeida",
    "Costa","Gomes","Martins","Araújo","Carvalho","Melo","Ribeiro","Nascimento",
    "Lima","Moura","Pereira","Castro","Campos","Barbosa","Cardoso","Rocha",
    "Moreira","Cavalcanti","Dias","Nunes","Monteiro","Teixeira","Borges",
]

CARGOS = [
    "Analista","Técnico","Auditor","Inspetor","Agente","Assistente",
    "Coordenador","Gestor","Especialista","Fiscal","Perito","Delegado",
    "Escrivão","Agente Penitenciário","Médico","Enfermeiro","Professor",
    "Engenheiro","Advogado","Contador",
]

VINCULOS = ["Estatutário","Celetista","Comissionado","Temporário","Estagiário"]

SEXOS = ["M","F","M","M","F","F"]  # distribuição não uniforme

ESCOLARIDADES = [
    "Ensino Fundamental","Ensino Médio","Ensino Médio",
    "Superior Incompleto","Superior Completo","Superior Completo",
    "Especialização","Mestrado","Doutorado",
]

def gerar_cpf(valido=True):
    """Gera um CPF formatado (não necessariamente válido matematicamente)."""
    nums = [random.randint(0, 9) for _ in range(11)]
    if not valido:
        nums = [random.randint(0, 9) for _ in range(11)]
    return f"{nums[0]}{nums[1]}{nums[2]}.{nums[3]}{nums[4]}{nums[5]}.{nums[6]}{nums[7]}{nums[8]}-{nums[9]}{nums[10]}"

def gerar_data_nascimento():
    """Gera uma data entre 1960 e 2000 em formato aleatoriamente sujo."""
    start = date(1960, 1, 1)
    end = date(2000, 12, 31)
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, delta))
    fmt = random.choice(['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'])
    return d.strftime(fmt)

def gerar_data_admissao():
    """Gera uma data de admissão entre 1990 e 2023."""
    start = date(1990, 1, 1)
    end = date(2023, 12, 31)
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, delta))
    fmt = random.choice(['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'])
    return d.strftime(fmt)

def gerar_salario():
    """Gera salário como string com formato inconsistente."""
    valor = round(random.uniform(1320.00, 25000.00), 2)
    estilo = random.choice(['ponto', 'virgula', 'sem_cents', 'str_r$'])
    if estilo == 'ponto':
        return f"{valor:.2f}"
    elif estilo == 'virgula':
        return f"{valor:.2f}".replace('.', ',')
    elif estilo == 'sem_cents':
        return str(int(valor))
    else:
        return f"R$ {valor:.2f}".replace('.', ',')

def gerar_email(nome, sobrenome, orgao, invalido=False):
    """Gera e-mail, às vezes inválido."""
    base = f"{nome.lower().replace(' ','')}.{sobrenome.lower().replace(' ','')}"
    dominio = f"{orgao.lower().replace(' ','').replace('-','')}.go.gov.br"
    if invalido:
        return random.choice([
            f"{base}@",
            f"{base}",
            f"@{dominio}",
            f"{base}@@{dominio}",
            "",
        ])
    return f"{base}@{dominio}"

def gerar_telefone():
    """Gera telefone com ou sem formatação."""
    ddd = random.choice(['62','61','64','63'])
    numero = random.randint(90000000, 99999999)
    estilo = random.choice(['formatado','so_numeros','nenhum'])
    if estilo == 'formatado':
        return f"({ddd}) 9{str(numero)[1:]}"
    elif estilo == 'so_numeros':
        return f"{ddd}9{str(numero)[1:]}"
    else:
        return ""

def gerar_nome(sujo=False):
    primeiro = random.choice(NOMES_PRIMEIRO)
    sobrenome = random.choice(NOMES_SOBRENOME)
    nome_completo = f"{primeiro} {sobrenome}"
    if sujo:
        variante = random.choice([
            nome_completo.upper(),
            nome_completo.lower(),
            "  " + nome_completo + "  ",
            nome_completo.replace(" ", "  "),
        ])
        return variante, primeiro, sobrenome
    return nome_completo, primeiro, sobrenome

def gerar_matricula(base):
    return f"GO{base:07d}"


def _gerar_registro(i: int, orgao_limpo: str, cpfs_usados: list, matriculas_usadas: list) -> dict:
    """
    Núcleo de geração de um único registro.
    Recebe o órgão já definido externamente (demo ou completo).
    """
    # Nome com ~20% de sujeira
    nome_sujo = random.random() < 0.20
    nome_completo, primeiro, sobrenome = gerar_nome(sujo=nome_sujo)

    # CPF com ~5% duplicados e ~3% nulos
    if cpfs_usados and random.random() < 0.05:
        cpf = random.choice(cpfs_usados)
    elif random.random() < 0.03:
        cpf = None
    else:
        cpf = gerar_cpf()
        cpfs_usados.append(cpf)

    # Matrícula com ~3% duplicada
    if matriculas_usadas and random.random() < 0.03:
        matricula = random.choice(matriculas_usadas)
    else:
        matricula = gerar_matricula(i + 1000)
        matriculas_usadas.append(matricula)

    orgao = dirty_orgao(orgao_limpo)

    email_invalido = random.random() < 0.08
    email = gerar_email(primeiro, sobrenome, orgao_limpo, invalido=email_invalido)

    def maybe_none(val, prob=0.05):
        return None if random.random() < prob else val

    return {
        "matricula":       matricula,
        "nome":            nome_completo,
        "cpf":             cpf,
        "data_nascimento": maybe_none(gerar_data_nascimento()),
        "sexo":            maybe_none(random.choice(SEXOS)),
        "escolaridade":    maybe_none(random.choice(ESCOLARIDADES)),
        "cargo":           maybe_none(random.choice(CARGOS)),
        "orgao":           orgao,
        "vinculo":         maybe_none(random.choice(VINCULOS)),
        "data_admissao":   maybe_none(gerar_data_admissao()),
        "salario_bruto":   gerar_salario(),
        "email":           email,
        "telefone":        gerar_telefone(),
        "ativo":           random.choice([1, 1, 1, 0]),
    }


def gerar_dataset_demo(n: int = 500) -> list[dict]:
    """
    Modo DEMO: gera `n` registros com órgãos sorteados aleatoriamente.
    Rápido, ideal para apresentação e testes.
    """
    print(f"[generate_data] Modo DEMO — gerando {n} registros com órgãos aleatórios...")
    registros        = []
    cpfs_usados      = []
    matriculas_usadas = []

    for i in range(n):
        orgao_limpo = random.choice(ORGAOS)
        registros.append(_gerar_registro(i, orgao_limpo, cpfs_usados, matriculas_usadas))

    return registros


def gerar_dataset_completo() -> list[dict]:
    """
    Modo COMPLETO: gera exatamente 166.261 registros respeitando
    a quantidade de servidores por órgão do Anexo I.
    """
    total = sum(ORGAOS_SERVIDORES.values())
    print(f"[generate_data] Modo COMPLETO — gerando {total:,} registros (Anexo I)...")

    registros         = []
    cpfs_usados       = []
    matriculas_usadas = []
    i                 = 0  # contador global de matrícula

    for orgao_limpo, quantidade in ORGAOS_SERVIDORES.items():
        for _ in range(quantidade):
            registros.append(_gerar_registro(i, orgao_limpo, cpfs_usados, matriculas_usadas))
            i += 1

        print(f"  ✓ {orgao_limpo:<15} {quantidade:>6} registros gerados")

    return registros


def salvar_csv(registros, caminho):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    campos = list(registros[0].keys())
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(registros)
    print(f"[generate_data] {len(registros)} registros salvos em '{caminho}'")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Gerador de dados fictícios — VOBYS/Goiás')
    parser.add_argument(
        '--modo',
        choices=['demo', 'completo'],
        default='demo',
        help="'demo' = 500 registros aleatórios | 'completo' = 166.261 por órgão (Anexo I)"
    )
    args = parser.parse_args()

    if args.modo == 'completo':
        dados = gerar_dataset_completo()
    else:
        dados = gerar_dataset_demo(n=500)

    salvar_csv(dados, OUTPUT_PATH)
