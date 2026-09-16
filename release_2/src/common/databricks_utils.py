"""Databricks runtime and environment utilities."""

import os
from typing import Any, Optional
from .logger import get_logger, register_secret_to_mask

logger = get_logger(__name__)


def get_dbutils() -> Optional[Any]:
    """Attempts to retrieve the Databricks `dbutils` object.
    
    Returns:
        dbutils object if running in Databricks runtime, None otherwise.
    """
    try:
        # If in Databricks notebook/job, dbutils is often in global scope or via dbruntime
        import IPython
        ipython = IPython.get_ipython()
        if ipython and "dbutils" in ipython.user_ns:
            return ipython.user_ns["dbutils"]
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None


def get_databricks_secret(
    scope: str,
    key: str,
    fallback_env_var: Optional[str] = None
) -> Optional[str]:
    """Retrieves secret from Databricks Secret Store, falling back to environment variables.
    
    Args:
        scope: Databricks secret scope name.
        key: Secret key name within scope.
        fallback_env_var: Optional environment variable name to check if dbutils is unavailable.
        
    Returns:
        Secret string if found, or None.
    """
    dbutils = get_dbutils()
    if dbutils:
        try:
            secret = dbutils.secrets.get(scope=scope, key=key)
            if secret:
                register_secret_to_mask(secret)
                logger.info("Successfully retrieved secret '%s' from scope '%s'", key, scope)
                return secret
        except Exception as e:
            logger.warning("Could not read secret from dbutils scope '%s', key '%s': %s", scope, key, e)

    # Fallback to environment variables
    env_candidate = fallback_env_var or key.upper()
    secret = os.getenv(env_candidate) or os.getenv("HENRIK_API_KEY")
    if secret:
        register_secret_to_mask(secret)
        logger.info("Retrieved secret from environment variable '%s'", env_candidate)
        return secret

    logger.warning("Secret '%s' not found in dbutils scope '%s' or environment variables", key, scope)
    return None


def get_spark_session() -> Optional[Any]:
    """Returns the active SparkSession or None if PySpark is not available."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception:
        return None
