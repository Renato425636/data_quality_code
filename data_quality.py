import logging
import sys
import os
import json
import yaml
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit, mean

class DataQualityCheckFailedError(Exception):

    pass

class AdvancedDataQualityFramework:

    def __init__(self, spark: SparkSession, config_path: str, rules_path: str):
        self.spark = spark
        self.config = self._load_config(config_path)
        self.rules = self._load_rules(rules_path)
        self._setup_logging()
        self.results = []

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        logging.basicConfig(level=self.config.get("log_level", "INFO"), format="%(asctime)s - [%(levelname)s] - %(message)s")
        self.logger = logging.getLogger(self.config.get("pipeline_name", "DQFramework"))
    
    def _load_rules(self, rules_path: str) -> dict:
        with open(rules_path, 'r') as f:
            return json.load(f)

    def run(self):

        self.logger.info("Iniciando a execução do Framework de Qualidade de Dados Avançado...")
        for validation_set in self.rules.get("validation_sets", []):
            dataset_name = validation_set["dataset_name"]
            self.logger.info(f"--- Processando dataset: {dataset_name} ---")
            
            try:
                df = self.spark.read.format(validation_set["data_source_format"]).option("header", "true").option("inferSchema", "true").load(validation_set["data_source_path"])
                for rule in validation_set.get("rules", []):
                    self._execute_rule(df, dataset_name, rule)
            except DataQualityCheckFailedError as e:
                self.logger.critical(f"Pipeline interrompido devido a uma falha crítica de DQ. Razão: {e}")
                self._generate_report()
                raise
            except Exception as e:
                self.logger.error(f"Falha ao processar o dataset {dataset_name}. Erro: {e}", exc_info=True)
        
        self._generate_report()
        self.logger.info("Framework de Qualidade de Dados concluiu a execução.")

    def _execute_rule(self, df: DataFrame, dataset_name: str, rule: dict):

        rule_type = rule["rule_type"]
        self.logger.info(f"Executando regra '{rule_type}' na coluna '{rule.get('column', 'N/A')}'...")

        validation_map = {
            "is_not_null": self._validate_not_null, "is_unique": self._validate_is_unique,
            "has_accepted_values": self._validate_accepted_values, "is_in_range": self._validate_in_range,
            "matches_regex": self._validate_regex, "null_percentage_is_less_than": self._validate_null_percentage,
            "mean_is_between": self._validate_mean_between
        }
        
        validation_func = validation_map.get(rule_type)
        if not validation_func:
            raise NotImplementedError(f"Tipo de regra desconhecido: {rule_type}")

        result = validation_func(df, rule)
        self.results.append({"dataset_name": dataset_name, "rule": rule, **result})
        
        if result["status"] == "FAIL":
            self.logger.warning(f"FALHA na regra '{rule_type}' para a coluna '{rule.get('column', 'N/A')}': {result['metrics']}")
            if rule.get("quarantine", False) and "failing_records_df" in result:
                self._quarantine_records(result["failing_records_df"], dataset_name, rule)
            
            if rule.get("on_fail", "WARN").upper() == "STOP":
                raise DataQualityCheckFailedError(f"Regra crítica '{rule_type}' falhou para a coluna '{rule.get('column', 'N/A')}' no dataset '{dataset_name}'.")

    def _quarantine_records(self, df: DataFrame, dataset_name: str, rule: dict):

        quarantine_path = self.config["paths"]["quarantine_path"]
        column = rule.get('column', 'no_column').replace(' ', '_')
        output_path = os.path.join(quarantine_path, dataset_name, f"{rule['rule_type']}_{column}")
        self.logger.info(f"Enviando {df.count()} registros falhos para quarentena em: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
    
    def _generate_report(self):
        """Gera e salva um relatório detalhado da execução das validações em JSON."""
        report_path_base = self.config["paths"]["report_path"]
        os.makedirs(report_path_base, exist_ok=True)
        report_file = os.path.join(report_path_base, f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        

        report_details = [{k: v for k, v in r.items() if k != 'failing_records_df'} for r in self.results]
        
        report = {
            "execution_timestamp": datetime.now().isoformat(),
            "summary": {"total_rules": len(self.results), "passed": sum(1 for r in self.results if r["status"] == "PASS"), "failed": sum(1 for r in self.results if r["status"] == "FAIL")},
            "details": report_details
        }
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=4)
        self.logger.info(f"Relatório salvo em: {report_file}")
        self.logger.info(f"SUMÁRIO: {report['summary']['passed']} Aprovados, {report['summary']['failed']} Falhas.")


    def _validate_not_null(self, df: DataFrame, rule: dict) -> dict:
        column = rule["column"]
        failing_records = df.filter(col(column).isNull())
        null_count = failing_records.count()
        return {"status": "PASS" if null_count == 0 else "FAIL", "metrics": {"null_count": null_count}, "failing_records_df": failing_records}

    def _validate_is_unique(self, df: DataFrame, rule: dict) -> dict:
        column = rule["column"]
        duplicates = df.groupBy(col(column)).agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        duplicate_count = duplicates.count()
        failing_records = df.join(duplicates.select(column), column, "inner")
        return {"status": "PASS" if duplicate_count == 0 else "FAIL", "metrics": {"duplicate_count": duplicate_count}, "failing_records_df": failing_records}

    def _validate_accepted_values(self, df: DataFrame, rule: dict) -> dict:
        column, accepted_list = rule["column"], rule["params"]["values"]
        failing_records = df.filter(~col(column).isin(accepted_list))
        invalid_count = failing_records.count()
        return {"status": "PASS" if invalid_count == 0 else "FAIL", "metrics": {"accepted_list": accepted_list, "invalid_count": invalid_count}, "failing_records_df": failing_records}

    def _validate_null_percentage(self, df: DataFrame, rule: dict) -> dict:
        column, threshold = rule["column"], rule["params"]["threshold"]
        total_count = df.count()
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        status = "PASS" if null_percentage < threshold else "FAIL"

        return {"status": status, "metrics": {"total_rows": total_count, "null_count": null_count, "null_percentage": f"{null_percentage:.2f}%", "threshold": f"< {threshold}%"}}

    def _validate_mean_between(self, df: DataFrame, rule: dict) -> dict:
        column, min_val, max_val = rule["column"], rule["params"]["min"], rule["params"]["max"]
        mean_val = df.select(mean(col(column))).first()[0]
        status = "PASS" if mean_val is not None and min_val <= mean_val <= max_val else "FAIL"

        return {"status": status, "metrics": {"mean_value": f"{mean_val:.2f}" if mean_val is not None else "N/A", "min_expected": min_val, "max_expected": max_val}}
    
    def _validate_in_range(self, df: DataFrame, rule: dict) -> dict:
        column, min_val, max_val = rule["column"], rule["params"]["min"], rule["params"]["max"]
        failing_records = df.filter((col(column) < min_val) | (col(column) > max_val))
        out_of_range_count = failing_records.count()
        status = "PASS" if out_of_range_count == 0 else "FAIL"
        return {"status": status, "metrics": {"min_range": min_val, "max_range": max_val, "out_of_range_count": out_of_range_count}, "failing_records_df": failing_records}

    def _validate_regex(self, df: DataFrame, rule: dict) -> dict:
        column, pattern = rule["column"], rule["params"]["pattern"]
        failing_records = df.filter(~col(column).rlike(pattern))
        mismatch_count = failing_records.count()
        status = "PASS" if mismatch_count == 0 else "FAIL"
        return {"status": status, "metrics": {"regex_pattern": pattern, "mismatch_count": mismatch_count}, "failing_records_df": failing_records}

def setup_test_environment(config_file="config.yaml", rules_file="rules.json"):

    print("Configurando ambiente de teste...")

    os.makedirs("data", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    os.makedirs("quarantine", exist_ok=True)

    config_data = {
        "pipeline_name": "AdvancedDataQualityFramework", "log_level": "INFO",
        "paths": {"report_path": "reports/", "quarantine_path": "quarantine/"}
    }
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    customers_data = "customer_id,name,state,age\n101,Alice,SP,30\n102,Bob,RJ,25\n103,Charlie,MG,35\n104,David,SP,40\n104,Eve,BA,22\n105,Frank,,55\n106,Grace,SP,\n107,Heidi,RJ,28"
    with open("data/customers_v2.csv", "w") as f: f.write(customers_data)
    
    rules_data = {
        "validation_sets": [
            {
                "dataset_name": "Clientes", "data_source_path": "data/customers_v2.csv", "data_source_format": "csv",
                "rules": [
                    {
                        "rule_type": "is_unique", "column": "customer_id", 
                        "on_fail": "STOP", "quarantine": True
                    },
                    {
                        "rule_type": "null_percentage_is_less_than", "column": "state",
                        "params": {"threshold": 15}, "on_fail": "WARN"
                    },
                    {
                        "rule_type": "mean_is_between", "column": "age",
                        "params": {"min": 20, "max": 40}, "on_fail": "WARN"
                    }
                ]
            }
        ]
    }
    with open(rules_file, "w") as f: json.dump(rules_data, f, indent=4)
    print("Ambiente de teste configurado com sucesso.")

if __name__ == "__main__":
    CONFIG_FILE = "config.yaml"
    RULES_FILE = "rules.json"

    setup_test_environment(CONFIG_FILE, RULES_FILE)
    
    spark = SparkSession.builder \
        .appName("DQFrameworkV2") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        dq_framework = AdvancedDataQualityFramework(spark, CONFIG_FILE, RULES_FILE)
        dq_framework.run()
    except DataQualityCheckFailedError as e:

        print(f"\nEXECUÇÃO INTERROMPIDA: {e}", file=sys.stderr)
        sys.exit(1)
    finally:

        print("Finalizando sessão Spark.")
        spark.stop()
