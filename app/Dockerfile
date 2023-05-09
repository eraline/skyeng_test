FROM python:3.10-slim

WORKDIR /app

COPY ./ .

RUN pip3 install -r /app/requirements.txt
RUN apt update -y && apt install -y build-essential libpq-dev
RUN pip3 install psycopg2-binary --no-binary psycopg2-binary
ENV PREFECT_ORION_API_HOST="127.0.0.1"
# RUN prefect orion start &
# RUN prefect deployment build /app/main.py:main -a --name="ETL To Load from DB to DWH" --cron "0 9 * * 1-7"
# RUN prefect agent start  --work-queue "default" &
# RUN python3 db/create_db.py
# RUN prefect deployment run "main/ETL To Load from DB to DWH" -p "date=2021-01-01"
RUN chmod +x /app/deploy.sh

# CMD ["prefect", "agent", "start", "--work-queue", "'default'"]
CMD ["/app/deploy.sh"]