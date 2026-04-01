# ETL de Pessoas — VOBYS / Estado de Goiás

> Teste técnico para a vaga de **Analista de Dados** na VOBYS  
> Migração de dados de servidores públicos para implantação do Sistema de Gestão de Pessoas

---

## Contexto

A VOBYS foi contratada pelo Estado de Goiás para implantar um **Sistema de Gestão de Pessoas** cobrindo:

- **40 órgãos** do executivo estadual
- ~**166.261 servidores**
- **3 projetos paralelos**: RH/Folha de Pagamento · eSocial · Gestão Estratégica de Pessoas
- **Destino**: banco de dados Oracle único e orquestrado
- **Prazo de implantação**: 18 meses

Esta ETL cobre o **Projeto Migração de Pessoas**, com dados fictícios representativos.

---

## Tecnologias Utilizadas

| Tecnologia | Papel |
|---|---|
| **Python 3.11** | Linguagem principal |
| **Pandas** | Manipulação e análise de dados |
| **SQLite** | Banco relacional de destino (simula Oracle — mesmo DDL compatível) |
| **csv / json / logging** | Leitura, checkpoint e logs (stdlib) |
| **Jupyter Notebook** | Apresentação e análise exploratória |

> **Por que SQLite e não Oracle?**  
> SQLite permite execução local sem infraestrutura, com DDL 100% compatível com Oracle.  
> Em produção, basta trocar a string de conexão e os tipos de dados AUTOINCREMENT → SEQUENCE.

---

## Estrutura do Repositório

```
etl_vobys/
├── data/
│   └── servidores_raw.csv          # CSV gerado com 500 registros fictícios
├── scripts/
│   ├── generate_data.py            # Gerador de dados fictícios com imperfeições
│   ├── extract.py                  # Etapa E: leitura em chunks + checkpoint
│   ├── transform.py                # Etapa T: normalização, validação, deduplicação
│   ├── load.py                     # Etapa L: carga no SQLite (schema 3FN, UPSERT)
│   ├── report.py                   # Geração de relatório de execução
│   └── run_etl.py                  # Orquestrador principal (ponto de entrada)
├── notebook/
│   └── ETL_Pessoas_VOBYS.ipynb    # Notebook de apresentação e análise
├── output/                         # Gerado automaticamente na execução
│   ├── gestao_pessoas.db           # Banco SQLite com schema normalizado
│   ├── quarentena.csv              # Registros inválidos para revisão
│   ├── relatorio_execucao.csv      # Métricas consolidadas
│   └── checkpoint.json             # Estado para retomada após falha
├── logs/
│   └── etl.log                     # Log detalhado de execução
└── requirements.txt
```

---

## Como Executar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Executar a ETL completa

```bash
# Execução normal (gera dados se CSV não existir)
python scripts/run_etl.py

# Forçar regeneração do CSV de dados, escolhendo entre o modo com 500 registros ou com todos os 166k registros
python scripts/run_etl.py --gerar-dados                        # demo (padrão)
python scripts/run_etl.py --gerar-dados --modo completo        # 166k exatos

# Resetar checkpoint (reexecuta do zero)
python scripts/run_etl.py --reset
```

### 3. Abrir o Notebook de apresentação

```bash
jupyter notebook notebook/ETL_Pessoas_VOBYS.ipynb
```

---

## Funcionalidades Demonstradas

### Normalização de dados
- Nomes: strip, Title Case, colapso de espaços múltiplos
- Órgãos: uppercase, strip (integridade referencial)
- Datas: múltiplos formatos aceitos → ISO 8601 (`YYYY-MM-DD`)
- Salários: remoção de `R$`, vírgula → ponto, string → float
- Telefone: normalização para `(XX) 9XXXX-XXXX`
- CPF: normalização para `000.000.000-00`

### Integridade de dados
- Schema em **3ª Forma Normal** com tabelas de referência: `orgaos`, `vinculos`, `cargos`, `escolaridades`
- **Foreign keys** habilitadas no SQLite (`PRAGMA foreign_keys = ON`)
- **UPSERT** por CPF/matrícula: evita duplicatas na carga

### Detecção de erro com relatório
- Campos **críticos** (CPF, matrícula, nome, órgão nulos) → quarentena
- Campos **não críticos** (email inválido, telefone) → alerta no log, registro carregado com campo nulo
- Arquivo `output/quarentena.csv` com linha de origem + descrição do erro
- Arquivo `output/relatorio_execucao.csv` com métricas por etapa
- Tabela `etl_execucoes` no banco para histórico auditável

### Padronização de formatos
- Todas as datas no banco em ISO 8601
- Salários como `REAL` (2 casas decimais)
- CPF, matrícula, e-mail padronizados antes da carga

### Robustez contra erros na execução
- **Checkpoint** em `output/checkpoint.json` salvo a cada chunk de 100 linhas
- Retomada automática do ponto de interrupção ao reexecutar
- Try/catch com log de erro e preservação do estado para debugging

### Métricas de execução
- Tempo por etapa (extract, transform, load)
- Total extraído, válidos, quarentena, inseridos, atualizados
- Taxa de rejeição percentual
- Log estruturado em `logs/etl.log`

---

## Schema do Banco (3FN)

```sql
orgaos        (id, sigla UNIQUE)
vinculos      (id, descricao UNIQUE)
cargos        (id, descricao UNIQUE)
escolaridades (id, descricao UNIQUE)

servidores (
    id, matricula UNIQUE, nome, cpf UNIQUE,
    data_nascimento, sexo,
    id_escolaridade → escolaridades(id),
    id_cargo        → cargos(id),
    id_orgao        → orgaos(id),       ← NOT NULL
    id_vinculo      → vinculos(id),
    data_admissao, salario_bruto,
    email, telefone, ativo,
    etl_carregado_em, etl_linha_origem  ← auditoria
)

etl_execucoes (id, iniciado_em, finalizado_em,
               total_extraido, total_valido,
               total_carregado, total_erros, status)
```

---

## Resultado da Execução (exemplo)

```
═══════════════════════════════════════════════════════
  RELATÓRIO DE EXECUÇÃO ETL — VOBYS / Estado de Goiás
═══════════════════════════════════════════════════════
  Registros extraídos                 500
  Registros válidos                   445
  Registros em quarentena              55
  Taxa de rejeição                  11.0%
  Inseridos no banco                  445
  Atualizados no banco                  0
  Tempo total (s)                    0.18
═══════════════════════════════════════════════════════
```

---

## Considerações para Produção (166k servidores / Oracle)

| Aspecto | PoC (este repositório) | Produção |
|---|---|---|
| Orquestração | Script Python | Apache Airflow — DAG por projeto |
| Volume | 500 registros | 166.261 registros em chunks de 5k |
| Banco destino | SQLite | Oracle 19c (mesmo DDL) |
| Monitoramento | Logs + CSV | Grafana + tabela `etl_execucoes` |
| Paralelismo | Sequencial | 3 DAGs paralelas (RH, eSocial, GEP) |
| Segurança/LGPD | Plaintext | Oracle TDE · perfis de acesso · mascaramento em DEV |
| Qualidade | Quarentena manual | Great Expectations + alertas automáticos |

### Principais riscos e mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|---|---|---|---|
| Dados inconsistentes entre órgãos | Alta | Alto | Tabela de-para com validação prévia |
| CPF duplicado/inválido em massa | Alta | Alto | Quarentena + workflow com gestores |
| Sobreposição entre projetos | Média | Crítico | Chave única CPF + orquestração sequencial |
| Falha durante carga dos 166k | Média | Alto | Checkpoint + UPSERT idempotente |
| Exposição de dados (LGPD) | Baixa | Crítico | Mascaramento, acesso por perfil, auditoria |

---

## Autor

*Gabriel Farias Tranquilli*  
Analista de Dados  
*([(73) 99926-4697](https://wa.me/5573999264697) / [LinkedIn](https://www.linkedin.com/in/gabrieltranquilli/))*
