"""
Digital-Artha Agent Quickstart

Interactive setup wizard that configures the agent environment.
"""

import os
import sys


def main():
    """Run interactive setup for the Digital-Artha agent."""
    print("=" * 60)
    print("  Digital-Artha Agent — Quickstart Setup")
    print("=" * 60)

    # Check for .env
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")

    config = {}

    # Databricks host
    config["DATABRICKS_HOST"] = input(
        "\nDatabricks workspace URL (e.g., https://xxx.cloud.databricks.com): "
    ).strip().rstrip("/")

    # Catalog and schema
    config["CATALOG"] = input("Catalog name [digital_artha]: ").strip() or "digital_artha"
    config["SCHEMA"] = input("Schema name [main]: ").strip() or "main"

    # Genie Space ID
    config["GENIE_SPACE_ID"] = input("Genie Space ID (from 07-deploy-genie.py): ").strip()

    # LLM endpoint
    config["LLM_ENDPOINT"] = input(
        "LLM endpoint [databricks-meta-llama-3-1-70b-instruct]: "
    ).strip() or "databricks-meta-llama-3-1-70b-instruct"

    # MLflow
    config["MLFLOW_TRACKING_URI"] = "databricks"
    config["MLFLOW_REGISTRY_URI"] = "databricks-uc"

    # Write .env
    with open(env_path, "w") as f:
        for key, value in config.items():
            f.write(f"{key}={value}\n")

    print(f"\nConfiguration saved to {env_path}")
    print("\nNext steps:")
    print("  1. Run: uv run start-app")
    print("  2. Open: http://localhost:8000")
    print("  3. Start chatting with Digital-Artha!")


if __name__ == "__main__":
    main()
