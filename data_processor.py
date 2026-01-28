import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
from scipy import stats

class DataProcessor:
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.original_df = df.copy()
        self.transformations = []
    
    def clean_data(self, methods: Dict[str, Any] = None) -> pd.DataFrame:
        default_methods = {
            'fill_missing': 'mean',  # mean, median, mode, or value
            'fill_value': 0,
            'remove_duplicates': True,
            'remove_outliers': False,
            'outlier_method': 'zscore',  # zscore or iqr
            'zscore_threshold': 3,
            'convert_types': True
        }
        
        if methods:
            default_methods.update(methods)
        
        methods = default_methods
        
        # Store transformation
        self.transformations.append({
            'operation': 'clean_data',
            'methods': methods,
            'timestamp': datetime.now().isoformat()
        })
        
        # duplicates
        if methods['remove_duplicates']:
            initial_rows = len(self.df)
            self.df = self.df.drop_duplicates()
            self.transformations[-1]['duplicates_removed'] = initial_rows - len(self.df)
        
        if methods['fill_missing']:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if self.df[col].isnull().any():
                    if methods['fill_missing'] == 'mean':
                        fill_value = self.df[col].mean()
                    elif methods['fill_missing'] == 'median':
                        fill_value = self.df[col].median()
                    elif methods['fill_missing'] == 'mode':
                        fill_value = self.df[col].mode()[0]
                    elif methods['fill_missing'] == 'value':
                        fill_value = methods['fill_value']
                    else:
                        fill_value = methods['fill_value']
                    
                    self.df[col].fillna(fill_value, inplace=True)
        
        if methods['remove_outliers']:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if methods['outlier_method'] == 'zscore':
                    z_scores = np.abs(stats.zscore(self.df[col].dropna()))
                    mask = z_scores < methods['zscore_threshold']
                    valid_indices = self.df[col].dropna().index[mask]
                    self.df = self.df.loc[valid_indices]
                elif methods['outlier_method'] == 'iqr':
                    Q1 = self.df[col].quantile(0.25)
                    Q3 = self.df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    mask = (self.df[col] >= lower_bound) & (self.df[col] <= upper_bound)
                    self.df = self.df[mask]
        
        # Convert data types
        if methods['convert_types']:
            for col in self.df.columns:
                #convert to numeric
                try:
                    self.df[col] = pd.to_numeric(self.df[col], errors='ignore')
                except:
                    pass
                
                # convert to datetime
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='ignore')
                except:
                    pass
        
        return self.df
    
    def aggregate_data(self, group_by: List[str], 
                      aggregations: Dict[str, List[str]]) -> pd.DataFrame:
        
        agg_dict = {}
        for col, agg_funcs in aggregations.items():
            if col in self.df.columns:
                for func in agg_funcs:
                    if func in ['sum', 'mean', 'median', 'min', 'max', 'count', 'std']:
                        agg_dict[col] = func
        
        if not agg_dict:
            agg_dict = {col: 'mean' for col in self.df.select_dtypes(include=[np.number]).columns}
        
        aggregated = self.df.groupby(group_by).agg(agg_dict).reset_index()
        
        self.transformations.append({
            'operation': 'aggregate_data',
            'group_by': group_by,
            'aggregations': aggregations,
            'result_shape': aggregated.shape,
            'timestamp': datetime.now().isoformat()
        })
        
        return aggregated
    
    def pivot_data(self, index: str, columns: str, values: str, 
                  aggfunc: str = 'mean') -> pd.DataFrame:
        
        pivot_table = pd.pivot_table(
            self.df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=0
        )
        
        self.transformations.append({
            'operation': 'pivot_data',
            'index': index,
            'columns': columns,
            'values': values,
            'aggfunc': aggfunc,
            'timestamp': datetime.now().isoformat()
        })
        
        return pivot_table
    
    def filter_data(self, conditions: List[Dict[str, Any]]) -> pd.DataFrame:
        
        mask = pd.Series([True] * len(self.df))
        
        for condition in conditions:
            column = condition.get('column')
            operator = condition.get('operator')
            value = condition.get('value')
            
            if column not in self.df.columns:
                continue
            
            if operator == 'equals':
                mask &= (self.df[column] == value)
            elif operator == 'not_equals':
                mask &= (self.df[column] != value)
            elif operator == 'greater_than':
                mask &= (self.df[column] > value)
            elif operator == 'less_than':
                mask &= (self.df[column] < value)
            elif operator == 'contains':
                mask &= self.df[column].astype(str).str.contains(str(value), case=False)
            elif operator == 'in':
                mask &= self.df[column].isin(value)
            elif operator == 'not_in':
                mask &= ~self.df[column].isin(value)
        
        filtered_df = self.df[mask]
        
        self.transformations.append({
            'operation': 'filter_data',
            'conditions': conditions,
            'rows_filtered': len(self.df) - len(filtered_df),
            'timestamp': datetime.now().isoformat()
        })
        
        return filtered_df
    
    def calculate_statistics(self) -> Dict[str, Any]:
        
        stats_dict = {
            'overall': {
                'total_rows': len(self.df),
                'total_columns': len(self.df.columns),
                'total_cells': len(self.df) * len(self.df.columns),
                'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'columns': {},
            'correlation_matrix': {},
            'missing_data': {}
        }
        
        # Column statistics
        for col in self.df.columns:
            col_stats = {
                'dtype': str(self.df[col].dtype),
                'unique_values': int(self.df[col].nunique()),
                'missing_values': int(self.df[col].isnull().sum()),
                'missing_percentage': float((self.df[col].isnull().sum() / len(self.df)) * 100)
            }
            
            if pd.api.types.is_numeric_dtype(self.df[col]):
                col_stats.update({
                    'mean': float(self.df[col].mean()),
                    'std': float(self.df[col].std()),
                    'min': float(self.df[col].min()),
                    'max': float(self.df[col].max()),
                    'median': float(self.df[col].median()),
                    'q1': float(self.df[col].quantile(0.25)),
                    'q3': float(self.df[col].quantile(0.75)),
                    'skew': float(self.df[col].skew()),
                    'kurtosis': float(self.df[col].kurtosis())
                })
            
            if pd.api.types.is_string_dtype(self.df[col]):
                col_stats.update({
                    'most_common': self.df[col].mode().iloc[0] if not self.df[col].mode().empty else None,
                    'most_common_count': int(self.df[col].value_counts().iloc[0]) if not self.df[col].value_counts().empty else 0
                })
            
            stats_dict['columns'][col] = col_stats
        
        numeric_df = self.df.select_dtypes(include=[np.number])
        if len(numeric_df.columns) > 1:
            corr_matrix = numeric_df.corr()
            stats_dict['correlation_matrix'] = corr_matrix.to_dict()
        
        missing_pattern = self.df.isnull().sum().sort_values(ascending=False)
        stats_dict['missing_data']['pattern'] = missing_pattern.to_dict()
        stats_dict['missing_data']['total_missing'] = int(self.df.isnull().sum().sum())
        
        return stats_dict
    
    def get_sample(self, n: int = 10, random: bool = True) -> pd.DataFrame:
        if random:
            return self.df.sample(min(n, len(self.df)))
        else:
            return self.df.head(n)
    
    def export_data(self, format: str = 'csv', path: str = None) -> str:
        if format == 'csv':
            return self.df.to_csv(index=False)
        elif format == 'json':
            return self.df.to_json(orient='records', indent=2)
        elif format == 'excel':
            if not path:
                raise ValueError("Path required for Excel export")
            self.df.to_excel(path, index=False)
            return path
        elif format == 'parquet':
            if not path:
                raise ValueError("Path required for Parquet export")
            self.df.to_parquet(path)
            return path
        else:
            raise ValueError(f"Unsupported export format: {format}")
