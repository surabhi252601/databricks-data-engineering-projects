
import yaml
from pathlib import Path


def load_config(config_name: str) -> dict:
    """
    Load pipeline configuration from YAML.

    Args:
        config_name: Name of the configuration file without extension.

    Returns:
        Dictionary containing pipeline configuration.
    """

    project_root = Path(__file__).resolve().parents[2]

    config_path = (
        project_root
        / "configs"
        / f"{config_name}.yaml"
    )

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"Invalid configuration format: {config_path}"
        )

    return config
