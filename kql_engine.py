import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import re
from datetime import datetime, timedelta

class KQLEngine:
    
    def __init__(self, df: pd.DataFrame, table_name: str = 'Data'):
        self.df = df.copy()
        self.table_name = table_name
        self.functions = self._initialize_functions()
    
    def _initialize_functions(self) -> Dict[str, Any]:
        return {
            'where': self._where,
            'summarize': self._summarize,
            'project': self._project,
            'extend': self._extend,
            'sort': self._sort,
            'take': self._take,
            'count': self._count,
            'distinct': self._distinct,
            'join': self._join,
            'union': self._union,
            'sample': self._sample,
            'top': self._top,
            'make_list': self._make_list,
            'mv_expand': self._mv_expand
        }
    
    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            # Parse query into operations
            operations = self._parse_query(query)
            result = self.df.copy()
            
            for operation in operations:
                func_name = operation['function']
                args = operation.get('args', [])
                kwargs = operation.get('kwargs', {})
                
                if func_name in self.functions:
                    result = self.functions[func_name](result, *args, **kwargs)
                else:
                    raise ValueError(f"Unknown function: {func_name}")
            
            return result
            
        except Exception as e:
            raise ValueError(f"Query execution error: {str(e)}")
    
    def _parse_query(self, query: str) -> List[Dict[str, Any]]:
        # Normalize query
        query = query.strip()
        parts = query.split('|')
        operations = []
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Parse function and arguments
            match = re.match(r'(\w+)\s*(.*)', part)
            if match:
                func_name = match.group(1).lower()
                arg_string = match.group(2).strip()
                
                # Parse arguments
                args, kwargs = self._parse_arguments(arg_string)
                
                operations.append({
                    'function': func_name,
                    'args': args,
                    'kwargs': kwargs
                })
        
        return operations
    
    def _parse_arguments(self, arg_string: str) -> tuple:
        args = []
        kwargs = {}
        
        if not arg_string:
            return args, kwargs
        
        tokens = []
        current_token = ""
        paren_depth = 0
        
        for char in arg_string:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            
            if char == ',' and paren_depth == 0:
                tokens.append(current_token.strip())
                current_token = ""
            else:
                current_token += char
        
        if current_token:
            tokens.append(current_token.strip())
        
        for token in tokens:
            if '=' in token:
                key, value = token.split('=', 1)
                key = key.strip()
                value = self._parse_value(value.strip())
                kwargs[key] = value
            else:
                args.append(self._parse_value(token))
        
        return args, kwargs
    
    def _parse_value(self, value: str) -> Any:
        # strings
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        elif value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        
        # numbers
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # booleans
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        
        # lists
        if value.startswith('(') and value.endswith(')'):
            items = value[1:-1].split(',')
            return [self._parse_value(item.strip()) for item in items]
        
        return value
    
    # KQL Function
    def _where(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if not args:
            return df
        
        condition = args[0]
        
        if isinstance(condition, str):
            condition = condition.replace('==', '=').replace('!=', '!=')
            
            conditions = condition.split('and')
            
            mask = pd.Series([True] * len(df))
            
            for cond in conditions:
                cond = cond.strip()
                
                if '=' in cond and not '!=' in cond and not '>=' in cond and not '<=' in cond:
                    col, val = cond.split('=', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] == val)
                
                elif '!=' in cond:
                    col, val = cond.split('!=', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] != val)
                
                elif '>=' in cond:
                    col, val = cond.split('>=', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] >= val)
                
                elif '<=' in cond:
                    col, val = cond.split('<=', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] <= val)
                
                elif '>' in cond:
                    col, val = cond.split('>', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] > val)
                
                elif '<' in cond:
                    col, val = cond.split('<', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= (df[col] < val)
                
                elif 'contains' in cond:
                    col, val = cond.split('contains', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= df[col].astype(str).str.contains(str(val), case=False)
                
                elif 'startswith' in cond:
                    col, val = cond.split('startswith', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= df[col].astype(str).str.startswith(str(val))
                
                elif 'endswith' in cond:
                    col, val = cond.split('endswith', 1)
                    col = col.strip()
                    val = self._parse_value(val.strip())
                    mask &= df[col].astype(str).str.endswith(str(val))
            
            return df[mask]
        
        return df
    
    def _summarize(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        """Aggregate data"""
        if not args:
            return df
        
        agg_expr = args[0]
        
        agg_dict = {}
        group_by = []
        
        for expr in agg_expr.split(','):
            expr = expr.strip()
            
            if '=' in expr:
                result_col, func_expr = expr.split('=', 1)
                result_col = result_col.strip()
                
                # function
                if '(' in func_expr and ')' in func_expr:
                    func_name = func_expr.split('(')[0].strip()
                    source_col = func_expr.split('(')[1].split(')')[0].strip()
                    
                    if func_name == 'count':
                        agg_dict[result_col] = pd.NamedAgg(column=source_col, aggfunc='size')
                    elif func_name in ['sum', 'mean', 'min', 'max', 'std', 'median']:
                        agg_dict[result_col] = pd.NamedAgg(column=source_col, aggfunc=func_name)
                    elif func_name == 'dcount':
                        agg_dict[result_col] = pd.NamedAgg(column=source_col, aggfunc='nunique')
            else:
                group_by.append(expr)
        
        if not agg_dict:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                agg_dict[col] = pd.NamedAgg(column=col, aggfunc='mean')
        
        if group_by:
            result = df.groupby(group_by).agg(**agg_dict).reset_index()
        else:
            result = df.agg(agg_dict).reset_index()
        
        return result
    
    def _project(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if not args:
            return df
        
        columns = []
        for arg in args:
            if isinstance(arg, list):
                columns.extend(arg)
            else:
                columns.append(arg)
        
        # Filter columns
        existing_cols = [col for col in columns if col in df.columns]
        
        if existing_cols:
            return df[existing_cols]
        else:
            return df
    
    def _extend(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        result = df.copy()
        
        for arg in args:
            if isinstance(arg, str) and '=' in arg:
                col_name, expression = arg.split('=', 1)
                col_name = col_name.strip()
                expression = expression.strip()
                
                try:
                    if expression.startswith('tolower('):
                        inner = expression[8:-1]
                        result[col_name] = df[inner].astype(str).str.lower()
                    elif expression.startswith('toupper('):
                        inner = expression[8:-1]
                        result[col_name] = df[inner].astype(str).str.upper()
                    elif '+' in expression:
                        parts = expression.split('+')
                        result[col_name] = sum(df[p.strip()] for p in parts if p.strip() in df.columns)
                    elif '*' in expression:
                        parts = expression.split('*')
                        product = 1
                        for p in parts:
                            p = p.strip()
                            if p in df.columns:
                                product *= df[p]
                        result[col_name] = product
                except:
                    pass
        
        return result
    
    def _sort(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if not args:
            return df.sort_index()
        
        order = kwargs.get('order', 'asc')
        
        columns = []
        for arg in args:
            if isinstance(arg, list):
                columns.extend(arg)
            else:
                columns.append(arg)
        
        ascending = order.lower() != 'desc'
        
        return df.sort_values(by=columns, ascending=ascending)
    
    def _take(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if args:
            n = int(args[0])
            return df.head(n)
        return df
    
    def _count(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({'count': [len(df)]})
    
    def _distinct(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if args:
            columns = []
            for arg in args:
                if isinstance(arg, list):
                    columns.extend(arg)
                else:
                    columns.append(arg)
            return df.drop_duplicates(subset=columns)
        return df.drop_duplicates()
    
    def _sample(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if args:
            n = int(args[0])
            return df.sample(n=min(n, len(df)))
        return df.sample(frac=0.1)
    
    def _top(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if len(args) >= 2:
            n = int(args[0])
            column = args[1]
            by = kwargs.get('by', 'desc')
            
            if column in df.columns:
                ascending = by.lower() != 'desc'
                return df.nlargest(n, column) if not ascending else df.nsmallest(n, column)
        
        return df.head(10)
    
    def _make_list(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if args:
            column = args[0]
            if column in df.columns:
                return pd.DataFrame({f'{column}_list': [df[column].tolist()]})
        return df
    
    def _mv_expand(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if args:
            column = args[0]
            if column in df.columns and df[column].apply(lambda x: isinstance(x, list)).any():
                return df.explode(column)
        return df
    
    def _join(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        # multiple DataFrames to come
        return df
    
    def _union(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        """Union with another DataFrame"""
        # multiple DataFrames to comw
        return df
    
    def get_query_examples(self) -> List[Dict[str, str]]:
        examples = [
            {
                'name': 'Filter data',
                'query': f'{self.table_name} | where column_name > 100',
                'description': 'Filter rows where column_name is greater than 100'
            },
            {
                'name': 'Summarize data',
                'query': f'{self.table_name} | summarize avg_value=avg(numeric_column) by category_column',
                'description': 'Calculate average of numeric_column grouped by category_column'
            },
            {
                'name': 'Select columns',
                'query': f'{self.table_name} | project column1, column2, column3',
                'description': 'Select specific columns'
            },
            {
                'name': 'Sort data',
                'query': f'{self.table_name} | sort by timestamp desc',
                'description': 'Sort by timestamp in descending order'
            },
            {
                'name': 'Top records',
                'query': f'{self.table_name} | top 10 by value desc',
                'description': 'Get top 10 records by value'
            },
            {
                'name': 'Count distinct',
                'query': f'{self.table_name} | distinct user_id | count',
                'description': 'Count distinct user IDs'
            },
            {
                'name': 'Multiple operations',
                'query': f'{self.table_name} | where status == "success" | summarize count() by date',
                'description': 'Count successful events by date'
            }
        ]
        return examples
