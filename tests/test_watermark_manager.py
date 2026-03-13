import pytest
import json
import os
from unittest.mock import MagicMock, patch
from watermark_manager import WatermarkManager

WATERMARK_FILE = '/home/duplicate/watermark.json'
LSN_HEX = '000002ae0001a4180004'
LSN_BYTES = bytes.fromhex(LSN_HEX)

class TestGetLastLsn:
    def test_retorna_none_sin_archivo(self, tmp_path, monkeypatch):
        monkeypatch.setattr('watermark_manager.WATERMARK_FILE', str(tmp_path / 'watermark.json'))
        wm = WatermarkManager()
        assert wm.get_last_lsn('dbo_sections') is None

    def test_retorna_lsn_correcto(self, tmp_path, monkeypatch):
        watermark_file = tmp_path / 'watermark.json'
        watermark_file.write_text(json.dumps({'dbo_sections': LSN_HEX}))
        monkeypatch.setattr('watermark_manager.WATERMARK_FILE', str(watermark_file))
        wm = WatermarkManager()
        assert wm.get_last_lsn('dbo_sections') == LSN_BYTES

class TestSaveLastLsn:
    def test_crea_archivo_si_no_existe(self, tmp_path, monkeypatch):
        watermark_file = tmp_path / 'watermark.json'
        monkeypatch.setattr('watermark_manager.WATERMARK_FILE', str(watermark_file))
        wm = WatermarkManager()
        wm.save_last_lsn('dbo_sections', LSN_BYTES)
        assert watermark_file.exists()
        data = json.loads(watermark_file.read_text())
        assert data['dbo_sections'] == LSN_HEX

    def test_actualiza_sin_afectar_otras_tablas(self, tmp_path, monkeypatch):
        watermark_file = tmp_path / 'watermark.json'
        watermark_file.write_text(json.dumps({
            'dbo_sections' : LSN_HEX,
            'dbo_families' : LSN_HEX
        }))
        monkeypatch.setattr('watermark_manager.WATERMARK_FILE', str(watermark_file))
        wm = WatermarkManager()

        nuevo_lsn = bytes.fromhex('000002ae0001d6900005')
        wm.save_last_lsn('dbo_sections', nuevo_lsn)

        data = json.loads(watermark_file.read_text())
        assert data['dbo_sections'] == nuevo_lsn.hex()
        assert data['dbo_families'] == LSN_HEX

class TestGetMinLsn:
    def test_retorna_lsn_minimo(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = {'min_lsn': LSN_BYTES}
        wm = WatermarkManager()
        resultado = wm.get_min_lsn(cursor, 'dbo_sections')
        assert resultado == LSN_BYTES