# Delta Lake Data Ingestion Framework

[![PyPI](https://img.shields.io/pypi/v/delta-spark)](https://pypi.org/project/delta-spark/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A production-ready data ingestion framework for loading data from multiple sources into Delta Lake tables. Built with Python, PySpark, and Delta Lake for scalable and reliable ETL workflows.

## Features ✨

- **Multi-Source Support**: Files (CSV, JSON, XML, Parquet, Excel), RDBMS, NoSQL, APIs, Kafka
- **Delta Lake Integration**: ACID transactions, schema evolution, time travel
- **Processing Modes**: Full load & incremental ingestion (merge/append)
- **Data Quality**: Schema validation, null checks, custom constraints
- **Resilience**: Checkpointing, retry mechanisms, error logging
- **Security**: Secrets management, encryption, SSL
- **Parallel Execution**: Multi-threaded config processing
- **YAML-Driven**: Declarative pipeline configurations

## Installation 🛠️

**Prerequisites:**
- Python 3.8+
- Apache Spark 3.3+
- Delta Lake 2.2+

```bash
pip install delta-spark pyspark pyyaml pandas cryptography
