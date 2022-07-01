import environ


# environment setup
env = environ.Env()
env_path = "./.env"
environ.Env.read_env(env_path)

# variables for kafka settings
BOOTSTRAP_SERVERS = env("BOOTSTRAP_SERVERS", default="localhost:9092")
RAW_DATA_PATH = env("RAW_DATA_PATH", default=None)
SORTED_DATA_PATH = env("SORTED_DATA_PATH", default=None)

# validate RAW_DATA_PATH and SORTED_DATA_PATH
if RAW_DATA_PATH is None and SORTED_DATA_PATH is None:
    raise Exception("GET NO DATA_PATH")
