"""
Digital-Artha Agent Quickstart

Interactive setup wizard that configures the agent environment.
"""

import os


def main():
    """Run interactive setup for the Digital-Artha agent."""
    print("=" * 60)
    print("  Digital-Artha Agent — Quickstart Setup")
    print("=" * 60)
    print()
    print("This wizard will configure your .env file for the agent.")
    print("You'll need your Databricks workspace URL, warehouse ID,")
    print("and Genie Space ID (from running 07-deploy-genie.py).")
    print()

    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    config = {}

    # Databricks host
    config["DATABRICKS_HOST"] = input(
        "Databricks workspace URL (e.g., https://xxx.cloud.databricks.com): "
    ).strip().rstrip("/")

    # Catalog and schema
    config["CATALOG"] = input("Catalog name [digital_artha]: ").strip() or "digital_artha"
    config["SCHEMA"] = input("Schema name [main]: ").strip() or "main"

    # SQL Warehouse ID (REQUIRED for fraud lookup + loan eligibility)
    print()
    print("SQL Warehouse ID is REQUIRED for fraud alerts and loan eligibility tools.")
    print("Find it: Databricks UI → SQL Warehouses → select warehouse → copy ID from URL.")
    config["WAREHOUSE_ID"] = input("SQL Warehouse ID: ").strip()
    if not config["WAREHOUSE_ID"]:
        print("WARNING: WAREHOUSE_ID not set. Fraud and eligibility tools will not work.")

    # Genie Space ID
    print()
    print("Genie Space ID enables natural language analytics queries.")
    print("Get it from the output of 07-deploy-genie.py notebook.")
    config["GENIE_SPACE_ID"] = input("Genie Space ID (optional): ").strip()

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

    print()
    print(f"Configuration saved to {os.path.abspath(env_path)}")
    print()
    print("Next steps:")
    print("  1. Run:  uv run start-app")
    print("  2. Open: http://localhost:8000")
    print("  3. Check health: http://localhost:8000/health")
    print("  4. Start chatting with Digital-Artha!")
    print()
    print("Try these queries:")
    print('  - "Show me recent fraud alerts"')
    print('  - "What are RBI guidelines on UPI fraud prevention?"')
    print('  - "I\'m a 25-year-old street vendor in Maharashtra earning ₹1.5L"')
    print('  - "यूपीआई फ्रॉड के बारे में बताओ" (Hindi)')


if __name__ == "__main__":
    main()
