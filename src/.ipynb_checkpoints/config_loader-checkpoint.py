import yaml

def load_config(path):
    """Read a YAML config file and return it as a Python dictionary."""
    with open(path, "r") as f:
        return yaml.safe_load(f)