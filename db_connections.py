import pymssql
from dotenv import load_dotenv
from logger import get_logger
import os

load_dotenv('/home/duplicate/.env')

log = get_logger('db_connections')

class DatabaseConnections:
    def __init__(self):
        self.local = None
        self.azure = None

    def connect(self):
        try:
            self.local = pymssql.connect(
                server=os.getenv('LOCAL_SERVER'),
                user=os.getenv('LOCAL_USER'),
                password=os.getenv('LOCAL_PASSWORD'),
                database=os.getenv('LOCAL_DATABASE')
            )
            log.info("Conexión local establecida")
        except Exception as e:
            log.error(f"Error conexión local: {e}")
            raise

        try:
            self.azure = pymssql.connect(
                server=os.getenv('AZURE_SERVER'),
                user=os.getenv('AZURE_USER'),
                password=os.getenv('AZURE_PASSWORD'),
                database=os.getenv('AZURE_DATABASE')
            )
            log.info("Conexión Azure establecida")
        except Exception as e:
            log.error(f"Error conexión Azure: {e}")
            raise

        return self

    def disconnect(self):
        if self.local:
            self.local.close()
            log.info("Conexión local cerrada")
        if self.azure:
            self.azure.close()
            log.info("Conexión Azure cerrada")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        if exc_type:
            log.error(f"Excepción durante la ejecución: {exc_val}")