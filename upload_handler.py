import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
from typing import Dict, Any, Optional, Tuple
import json
import pyarrow.parquet as pq
import pyarrow as pa
from odf import opendocument, teletype
from odf.table import Table, TableRow, TableCell
from odf.text import P

class FileUploadHandler:
    """Handles file uploads and converts to DataFrame"""
    
    def __init__(self):
        self.supported_extensions = {
            '.csv': self._read_csv,
            '.xlsx': self._read_excel,
            '.xls': self._read_excel,
            '.xlsb': self._read_excel_binary,
            '.xltx': self._read_excel,
            '.ods': self._read_ods,
            '.parquet': self._read_parquet,
            '.json': self._read_json
        }
    
    def process_file(self, file_path: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Process uploaded file and return DataFrame with metadata"""
        ext = file_path.suffix.lower()
        
        if ext not in self.supported_extensions:
            raise ValueError(f"Unsupported file extension: {ext}")
        
        # Read the file
        df = self.supported_extensions[ext](file_path)
        
        # Generate metadata
        metadata = self._generate_metadata(df, file_path)
        
        return df, metadata
    
    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        
        delimiters = [',', ';', '\t', '|']
        detected_delimiter = ','
        max_count = 0
        
        for delim in delimiters:
            count = first_line.count(delim)
            if count > max_count:
                max_count = count
                detected_delimiter = delim
        
        return pd.read_csv(file_path, delimiter=detected_delimiter, 
                          encoding='utf-8', engine='python')
    
    def _read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel files (xlsx, xls, xltx)"""
        try:
            excel_file = pd.ExcelFile(file_path)
            
            if len(excel_file.sheet_names) > 1:
                for sheet in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet)
                    if len(df) > 0:
                        return df
            else:
                return pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {str(e)}")
    
    def _read_excel_binary(self, file_path: Path) -> pd.DataFrame:
        return pd.read_excel(file_path, engine='pyxlsb')
    
    def _read_ods(self, file_path: Path) -> pd.DataFrame:
        try:
            doc = opendocument.load(file_path)
            
            tables = doc.getElementsByType(Table)
            if not tables:
                raise ValueError("No tables found in ODS file")
            
            table = tables[0]
            rows = table.getElementsByType(TableRow)
            
            data = []
            for row in rows:
                row_data = []
                cells = row.getElementsByType(TableCell)
                for cell in cells:
                    text_elements = cell.getElementsByType(P)
                    cell_text = ''
                    for elem in text_elements:
                        cell_text += teletype.extractText(elem)
                    row_data.append(cell_text)
                data.append(row_data)
            
            # Convert to Data
            df = pd.DataFrame(data[1:], columns=data[0] if data else [])
            return df
            
        except Exception as e:
            raise ValueError(f"Error reading ODS file: {str(e)}")
    
    def _read_parquet(self, file_path: Path) -> pd.DataFrame:
        return pd.read_parquet(file_path)
    
    def _read_json(self, file_path: Path) -> pd.DataFrame:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # data array
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.DataFrame(value)
            return pd.DataFrame([data])
        else:
            raise ValueError("Invalid JSON structure")
    
    def _generate_metadata(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        metadata = {
            'file_name': file_path.name,
            'file_size': file_path.stat().st_size,
            'file_path': str(file_path),
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'missing_values': df.isnull().sum().to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'basic_stats': {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            metadata['basic_stats'][col] = {
                'mean': float(df[col].mean()),
                'std': float(df[col].std()),
                'min': float(df[col].min()),
                'max': float(df[col].max()),
                'median': float(df[col].median())
            }
        
        return metadata
