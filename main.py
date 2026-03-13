from db_connections import DatabaseConnections
from synchronizer import sincronizar

with DatabaseConnections() as db:
    sincronizar(db)