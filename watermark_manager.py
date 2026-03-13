import json
import os
from logger import get_logger

log = get_logger('watermark_manager')

WATERMARK_FILE = '/home/duplicate/watermark.json'

class WatermarkManager:
    def __init__(self):
        self.data = self._load()

    def _load(self):
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, 'r') as f:
                log.info("Watermark cargado")
                return json.load(f)
        log.info("No existe watermark previo, iniciando vacío")
        return {}

    def _save(self):
        with open(WATERMARK_FILE, 'w') as f:
            json.dump(self.data, f, indent=4)

    def get_last_lsn(self, tabla: str):
        lsn = self.data.get(tabla)
        if lsn:
            log.info(f"{tabla} | LSN previo encontrado: {lsn}")
            return bytes.fromhex(lsn)
        log.info(f"{tabla} | Sin LSN previo")
        return None

    def save_last_lsn(self, tabla: str, lsn: bytes):
        self.data[tabla] = lsn.hex()
        self._save()
        log.info(f"{tabla} | LSN actualizado: {lsn.hex()}")

    def get_min_lsn(self, cursor, capture_instance: str):
        cursor.execute(
            "SELECT sys.fn_cdc_get_min_lsn(%s) AS min_lsn",
            (capture_instance,)
        )
        min_lsn = cursor.fetchone()['min_lsn']
        log.info(f"{capture_instance} | LSN mínimo: {min_lsn.hex()}")
        return min_lsn