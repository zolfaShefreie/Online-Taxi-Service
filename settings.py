import environ
import os


# environment setup
env = environ.Env()
env_path = "./.env"
environ.Env.read_env(env_path)

# variables for kafka settings
BOOTSTRAP_SERVERS = env("BOOTSTRAP_SERVERS", default="localhost:9092")
RAW_DATA_PATH = env("RAW_DATA_PATH", default=None)
SORTED_DATA_PATH = env("SORTED_DATA_PATH", default="sorted_data.csv")

# variables for elasticsearch settings
ELASTIC_SERVER = env("ELASTIC_SERVER", default="localhost:9200")
ELASTIC_PASSWORD = env("ELASTIC_PASSWORD", default=None)

# validate RAW_DATA_PATH and SORTED_DATA_PATH
if (RAW_DATA_PATH is None and not os.path.exists(SORTED_DATA_PATH)) or \
        (RAW_DATA_PATH is not None and not os.path.exists(RAW_DATA_PATH) and not os.path.exists(SORTED_DATA_PATH)):
    raise Exception("GET NO DATA_PATH")
