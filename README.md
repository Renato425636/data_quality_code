# Framework Genérico de Qualidade de Dados com PySpark

Este repositório contém um framework de qualidade de dados (DQ) robusto, genérico e configurável, construído em Python e PySpark. Ele foi projetado para ser uma ferramenta prática e poderosa para validar a integridade e a consistência de datasets em um Data Lake ou em qualquer etapa de um pipeline de ETL/ELT.

O grande diferencial deste projeto é que ele é **totalmente autocontido**: o único script Python é responsável por criar seus próprios dados de exemplo e arquivos de configuração, tornando a demonstração e o teste extremamente simples.

## 🏛️ Visão e Filosofia do Projeto

A confiança nos dados é o alicerce de qualquer iniciativa de Business Intelligence, Analytics ou Machine Learning. Este framework foi criado para ser um "portão de qualidade" automatizado, garantindo que apenas dados que atendam a critérios de negócio predefinidos prossigam no pipeline.

A filosofia central do projeto é a **separação entre a lógica de validação e as regras de negócio**:

  * **A Lógica (O "Como"):** A classe `AdvancedDataQualityFramework` contém o motor de validação genérico, escrito em PySpark.
  * **As Regras (O "O Quê"):** As validações são definidas externamente em um arquivo `rules.json`, permitindo que a equipe de dados defina e modifique regras sem tocar no código do framework.

## ✨ Principais Funcionalidades

O framework é equipado com funcionalidades avançadas para lidar com cenários complexos de qualidade de dados:

✔️ **Motor de Regras via JSON**
Toda a validação é controlada por um arquivo `rules.json`. Você pode especificar múltiplos datasets, e para cada um, uma lista de regras a serem aplicadas em colunas específicas.

🚦 **Níveis de Severidade e Controle de Pipeline**
Cada regra pode ter uma ação `on_fail` para controle granular do fluxo do pipeline:

  * `"WARN"` (Padrão): Registra a falha, mas permite que o pipeline continue. Ideal para problemas de baixa prioridade.
  * `"STOP"`: Interrompe a execução do pipeline **imediatamente** ao encontrar uma falha, lançando uma exceção. Essencial para erros críticos que invalidam o dataset.

🔬 **Quarentena de Dados (Isolamento de Erros)**
Quando uma regra falha, o framework pode **isolar os registros exatos** que violaram a regra e salvá-los em um diretório de "quarentena". Isso permite uma análise offline detalhada da causa raiz do problema. A funcionalidade é ativada com o parâmetro `"quarantine": true`.

📋 **Conjunto Abrangente de Regras de Validação**
O framework já vem com um conjunto de regras prontas para uso:

  * **Nível de Linha:** `is_unique`, `is_not_null`, `has_accepted_values`, `is_in_range`, `matches_regex`.
  * **Nível de Agregação (Estatístico):** `null_percentage_is_less_than`, `mean_is_between`.

📄 **Relatórios Detalhados em JSON**
Ao final de cada execução, um relatório completo em formato JSON é gerado. Ele contém um sumário da execução (total de regras, aprovadas, falhas) e os detalhes de cada teste, incluindo as métricas que levaram à falha.

## 🔧 Como Funciona: O Arquivo de Regras (`rules.json`)

O coração da customização do framework é o arquivo `rules.json`. Ele define quais validações executar.

**Estrutura do `rules.json`:**

```json
{
    "validation_sets": [
        {
            "dataset_name": "Clientes",
            "data_source_path": "data/customers_v2.csv",
            "data_source_format": "csv",
            "rules": [
                {
                    "rule_type": "is_unique",
                    "column": "customer_id",
                    "on_fail": "STOP",
                    "quarantine": true
                },
                {
                    "rule_type": "null_percentage_is_less_than",
                    "column": "state",
                    "params": {
                        "threshold": 15
                    },
                    "on_fail": "WARN"
                },
                {
                    "rule_type": "mean_is_between",
                    "column": "age",
                    "params": {
                        "min": 20,
                        "max": 40
                    }
                }
            ]
        }
    ]
}
```

**Anatomia de uma regra:**

  * `rule_type`: O tipo de validação a ser aplicada (ex: `is_unique`).
  * `column`: A coluna alvo da validação.
  * `params`: (Opcional) Um objeto com parâmetros específicos para a regra (ex: `threshold` para porcentagem de nulos, ou `min` e `max` para um intervalo).
  * `on_fail`: (Opcional, padrão "WARN"). Define a ação em caso de falha: `"WARN"` para continuar ou `"STOP"` para interromper o pipeline.
  * `quarantine`: (Opcional, padrão `false`). Se `true`, os registros que falharem nesta regra serão salvos para análise.

## 🚀 Como Executar o Projeto

Este projeto é **autocontido**. O único script Python irá gerar automaticamente todos os arquivos necessários para a demonstração.

### 1\. Pré-requisitos

  * Python 3.9 ou superior.
  * Java Development Kit (JDK) 8 ou 11 (necessário para o Spark).
  * Instale as dependências Python:
    ```bash
    pip install pyspark pyyaml
    ```

### 2\. Execução

Basta salvar o código fornecido como um arquivo (ex: `dq_framework_standalone.py`) e executá-lo a partir do seu terminal:

```bash
python dq_framework_standalone.py
```

**O que acontece durante a execução:**

1.  A função `setup_test_environment()` é chamada primeiro.
2.  Ela cria os diretórios `data/`, `reports/` e `quarantine/`.
3.  Ela cria os arquivos `config.yaml`, `rules.json` e `data/customers_v2.csv`.
4.  Só então o `AdvancedDataQualityFramework` é iniciado, lendo os arquivos que acabaram de ser criados.

### 3\. Entendendo a Saída

Após a execução, você verá:

  * **No Console:** Logs detalhados de cada regra sendo executada. A execução será **interrompida** com uma mensagem de erro crítico, pois a regra `is_unique` para `customer_id` irá falhar (conforme definido com `"on_fail": "STOP"`).
  * **Diretório `reports/`:** Conterá um arquivo `dq_report_...json` com o relatório detalhado das regras que foram executadas até a interrupção.
  * **Diretório `quarantine/`:** Conterá um subdiretório `Clientes/is_unique_customer_id/`, e dentro dele, um arquivo Parquet com os registros de `customer_id = 104` que causaram a falha, prontos para sua análise.
