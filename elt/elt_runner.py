import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

DUMP_FILE = Path(os.getenv("ELT_DUMP_PATH", "data_dump.sql"))
MAX_RETRIES = int(os.getenv("ELT_MAX_RETRIES", "10"))
RETRY_DELAY_SECONDS = int(os.getenv("ELT_RETRY_DELAY_SECONDS", "5"))


def wait_for_postgres(config: Dict[str, str]) -> None:
    """Block until PostgreSQL is ready to accept connections."""
    env = os.environ.copy()
    env["PGPASSWORD"] = config["password"]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            subprocess.run(
                [
                    "pg_isready",
                    "-h",
                    config["host"],
                    "-p",
                    str(config["port"]),
                    "-U",
                    config["user"],
                ],
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            print(
                f"[ready] Postgres {config['host']}:{config['port']} is ready "
                f"(attempt {attempt})"
            )
            return
        except subprocess.CalledProcessError as exc:
            print(
                f"[wait] Waiting for Postgres {config['host']}:{config['port']} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )
            time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError(
        f"Postgres at {config['host']}:{config['port']} was not ready after "
        f"{MAX_RETRIES} attempts."
    )


def run_command(command: List[str], password: str) -> None:
    """Execute a postgres client command with password-based auth."""
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    print(f"[cmd] Running command: {' '.join(command)}")
    subprocess.run(command, env=env, check=True)


def dump_database(config: Dict[str, str]) -> None:
    run_command(
        [
            "pg_dump",
            "-h",
            config["host"],
            "-p",
            str(config["port"]),
            "-U",
            config["user"],
            "-d",
            config["dbname"],
            "-f",
            str(DUMP_FILE),
            "-w",
        ],
        password=config["password"],
    )


def load_database(config: Dict[str, str]) -> None:
    run_command(
        [
            "psql",
            "-h",
            config["host"],
            "-p",
            str(config["port"]),
            "-U",
            config["user"],
            "-d",
            config["dbname"],
            "-a",
            "-f",
            str(DUMP_FILE),
        ],
        password=config["password"],
    )


def database_config(prefix: str, default_host: str) -> Dict[str, str]:
    """Build configuration dictionary from environment variables."""
    default_db_name = "source_db" if prefix == "SOURCE" else "destination_db"

    return {
        "dbname": os.getenv(f"{prefix}_DB_NAME", default_db_name),
        "user": os.getenv(f"{prefix}_DB_USER", "root"),
        "password": os.getenv(f"{prefix}_DB_PASSWORD", "1724"),
        "host": os.getenv(f"{prefix}_DB_HOST", default_host),
        "port": int(os.getenv(f"{prefix}_DB_PORT", "5432")),
    }


def main() -> None:
    source_cfg = database_config("SOURCE", "source_postgres")
    destination_cfg = database_config("DESTINATION", "destination_postgres")

    print("[start] Starting ELT replication process")

    wait_for_postgres(source_cfg)
    wait_for_postgres(destination_cfg)

    try:
        dump_database(source_cfg)
        load_database(destination_cfg)
        print("[done] Data replicated to destination database")
    except subprocess.CalledProcessError as exc:
        print(f"[error] ELT replication failed: {exc}")
        sys.exit(1)
    finally:
        if DUMP_FILE.exists():
            DUMP_FILE.unlink()
            print(f"[cleanup] Removed temporary dump file {DUMP_FILE}")


if __name__ == "__main__":
    main()
