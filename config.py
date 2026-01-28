import os
from pathlib import Path

class Config:
    # Path configurations
    BASE_DIR = Path(__file__).parent
    UPLOAD_FOLDER = BASE_DIR / 'uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    
    # Allowed file extensions
    ALLOWED_EXTENSIONS = {
        'csv': 'text/csv',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'xlsb': 'application/vnd.ms-excel.sheet.binary',
        'xltx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
        'ods': 'application/vnd.oasis.opendocument.spreadsheet',
        'parquet': 'application/octet-stream',
        'json': 'application/json'
    }
    
    # KQL configurations
    KQL_DEFAULT_TABLE = 'Data'
    KQL_TIMEOUT = 30  # seconds
    
    # Visualization defaults
    DEFAULT_CHART_WIDTH = 1200
    DEFAULT_CHART_HEIGHT = 600
    
    @staticmethod
    def ensure_directories():
        """Create necessary directories"""
        Config.UPLOAD_FOLDER.mkdir(exist_ok=True)
