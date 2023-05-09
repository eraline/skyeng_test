#!/bin/bash

python3 db/create_db.py
prefect orion database reset -y
prefect orion start --host 0.0.0.0 &
prefect deployment build /app/main.py:main -a --name="ETL To Load from DB to DWH" --cron "0 9 * * 1-7"
prefect deployment run "main/ETL To Load from DB to DWH" -p "date=2021-01-01"
prefect agent start --work-queue "default"

