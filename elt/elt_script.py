import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
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

# Use the function before running the ELT process
if not wait_for_postgres(host="source_postgres"):
    exit(1)
    
print("Starting ETL script...")

# Configuration for the source PostgreSQL database
source_config = {
    'dbname': 'source_postgres_db',
    'user': 'postgres',
    'password': 'source-secret',
    # Use the service name from docker-compose as the hostname
    "host": "source_postgres"
}

# Configuration for the destination PostgreSQL database
destination_config = {
    'dbname': 'destination_postgres_db',
    'user': 'postgres',
    'password': 'destination-secret',
    # Use the service name from docker-compose as the hostname
    "host": "destination_postgres"
}

# Use pg_dump to dump the source database to a SQL file
dump_to_source_db_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'  # Do not prompt for password
]

# Set the PGPASSWORD environment variable to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config['password'])

# Execute the dump command
subprocess.run(dump_to_source_db_command, env=subprocess_env, check=True)

# Use psql to load the dumped SQL file into the destination database
load_into_destination_db_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a','-f', 'data_dump.sql',
]

# Set the PGPASSWORD environment variable for the destination database
subprocess_env = dict(PGPASSWORD=destination_config['password'])

# Execute the load command
subprocess.run(load_into_destination_db_command, env=subprocess_env, check=True)

print("ETL script completed successfully. ending elt script")