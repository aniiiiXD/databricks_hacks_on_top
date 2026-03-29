# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow Test — Diagnosing Free Edition Compatibility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Set the missing Spark config BEFORE importing mlflow

# COMMAND ----------

try:
    spark.conf.set("spark.mlflow.modelRegistryUri", "databricks-uc")
    print("✅ spark.conf.set succeeded")
except Exception as e:
    print(f"❌ spark.conf.set failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Import mlflow

# COMMAND ----------

try:
    import mlflow
    print(f"✅ import mlflow succeeded (version: {mlflow.__version__})")
except Exception as e:
    print(f"❌ import mlflow failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Set experiment

# COMMAND ----------

try:
    username = spark.sql("SELECT current_user()").collect()[0][0]
    mlflow.set_experiment(f"/Users/{username}/mlflow-test")
    print(f"✅ set_experiment succeeded for {username}")
except Exception as e:
    print(f"❌ set_experiment failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Log params and metrics

# COMMAND ----------

try:
    with mlflow.start_run(run_name="test_run"):
        mlflow.log_param("test_param", "hello")
        mlflow.log_metric("test_metric", 42.0)
        mlflow.log_metric("accuracy", 0.95)
        run_id = mlflow.active_run().info.run_id
    print(f"✅ Logging succeeded! Run ID: {run_id}")
except Exception as e:
    print(f"❌ Logging failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Log a sklearn model

# COMMAND ----------

try:
    from sklearn.ensemble import IsolationForest
    model = IsolationForest(n_estimators=10, random_state=42)
    model.fit([[1,2],[3,4],[5,6]])

    with mlflow.start_run(run_name="model_test"):
        mlflow.sklearn.log_model(model, "test_model")
        run_id = mlflow.active_run().info.run_id
    print(f"✅ Model logging succeeded! Run ID: {run_id}")
except Exception as e:
    print(f"❌ Model logging failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Register model to Unity Catalog

# COMMAND ----------

try:
    mlflow.set_registry_uri("databricks-uc")
    result = mlflow.register_model(f"runs:/{run_id}/test_model", "digital_artha.main.test_model")
    print(f"✅ Model registration succeeded! Version: {result.version}")
except Exception as e:
    print(f"❌ Model registration failed (expected on Free Edition): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("MLFLOW COMPATIBILITY REPORT")
print("=" * 50)
print("Copy this to share with Databricks team or mentors")
print()
print(f"Platform: Databricks Free Edition (Serverless)")
print(f"MLflow version: {mlflow.__version__}")
print()
print("Run the cells above and note which ones show ✅ vs ❌")
