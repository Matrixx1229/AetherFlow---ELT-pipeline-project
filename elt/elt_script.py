import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retires = 0
    while retires < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print("Successfully connected to Postgres.")
                return True 
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to Postgres: {e}")
            retries += 1 
            print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Could not connect to Postgres. Exiting")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)
    
print("Starting ETL script...")

source_config = {
    'dbname': 'source_postgres_db',
    'user': 'postgres',
    'password': 'source-secret',
    "host": "source_postgres"
}

destination_config = {
    'dbname': 'destination_postgres_db',
    'user': 'postgres',
    'password': 'destination-secret',
    "host": "destination_postgres"
}

dump_to_source_db_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=source_config['password'])

subprocess.run(dump_to_source_db_command, env=subprocess_env, check=True)

load_into_destination_db_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a','-f', 'data_dump.sql',
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

subprocess.run(load_into_destination_db_command, env=subprocess_env, check=True)

print("ETL script completed successfully. ending elt script")