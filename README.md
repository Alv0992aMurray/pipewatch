# pipewatch

A lightweight CLI for monitoring and alerting on ETL pipeline health metrics in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source postgres://user:pass@localhost/mydb --interval 30
```

Set up an alert rule to notify when a threshold is breached:

```bash
pipewatch alert --metric row_count --threshold "<1000" --notify slack
```

Run a one-time health check and print a summary report:

```bash
pipewatch check --pipeline daily_sales_etl
```

**Example output:**

```
[✓] daily_sales_etl   rows_processed: 45,210   latency: 1.2s   status: healthy
[✗] user_sync_etl     rows_processed: 0        latency: N/A    status: FAILED
```

---

## Configuration

pipewatch looks for a config file at `~/.pipewatch/config.yaml`. You can specify pipeline sources, alert channels, and thresholds there. Run `pipewatch init` to generate a starter config.

---

## Requirements

- Python 3.8+
- See `requirements.txt` for dependencies

---

## License

This project is licensed under the [MIT License](LICENSE).