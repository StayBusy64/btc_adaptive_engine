from pathlib import Path
import yaml


def load_config(config_path: str = "config/config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
