FROM python:3.8-slim

# Add Postgres repo for latest versions
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(grep VERSION_CODENAME /etc/os-release | cut -d= -f2)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-client-18 && \
    apt-get clean

COPY etl_script.py .

CMD ["python", "etl_script.py"]
