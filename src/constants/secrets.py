import os

mongodb_url = os.getenv('DB_SENSOR_URL', None)

# kafka api
api_key = os.getenv('API_KEY', None)
api_secret = os.getenv('API_SECRET_KEY', None)
api_url = os.getenv('BOOTSTRAP_SERVER', None)

# kafka schema 
schema_url = os.getenv('ENDPOINT_SCHEMA_URL', None)
schema_api_key = os.getenv('SCHEMA_REGISTRY_API_KEY', None)
schema_api_secret = os.getenv('SCHEMA_REGISTRY_API_SECRET', None)