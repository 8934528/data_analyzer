from matplotlib.pyplot import plot
import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as sp
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import json
from plotly.utils import PlotlyJSONEncoder

class DataVisualizer:
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def create_histogram(self, column: str, 
                        title: str = None,
                        color: str = None,
                        nbins: int = None) -> Dict[str, Any]:
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found")
        
        if not pd.api.types.is_numeric_dtype(self.df[column]):
            raise ValueError(f"Column '{column}' must be numeric")
        
        fig = px.histogram(
            self.df,
            x=column,
            title=title or f'Histogram of {column}',
            color=color,
            nbins=nbins,
            marginal='box'
        )
        
        return json.loads(fig.to_json())
    
    def create_scatter_plot(self, x_column: str, y_column: str,
                          color_column: str = None,
                          size_column: str = None,
                          title: str = None) -> Dict[str, Any]:
        if x_column not in self.df.columns:
            raise ValueError(f"Column '{x_column}' not found")
        if y_column not in self.df.columns:
            raise ValueError(f"Column '{y_column}' not found")
        
        fig = px.scatter(
            self.df,
            x=x_column,
            y=y_column,
            color=color_column,
            size=size_column,
            title=title or f'{y_column} vs {x_column}',
            hover_data=self.df.columns.tolist()
        )
        
        return json.loads(fig.to_json())
    
    def create_line_plot(self, x_column: str, y_column: str,
                        color_column: str = None,
                        title: str = None) -> Dict[str, Any]:
        if x_column not in self.df.columns:
            raise ValueError(f"Column '{x_column}' not found")
        if y_column not in self.df.columns:
            raise ValueError(f"Column '{y_column}' not found")
        
        # Sort by x_column if it's datetime or numeric
        df_sorted = self.df.sort_values(by=x_column)
        
        fig = px.line(
            df_sorted,
            x=x_column,
            y=y_column,
            color=color_column,
            title=title or f'{y_column} over {x_column}',
            markers=True
        )
        
        return json.loads(fig.to_json())
    
    def create_bar_chart(self, x_column: str, y_column: str,
                        color_column: str = None,
                        title: str = None,
                        orientation: str = 'v') -> Dict[str, Any]:
        if x_column not in self.df.columns:
            raise ValueError(f"Column '{x_column}' not found")
        if y_column not in self.df.columns:
            raise ValueError(f"Column '{y_column}' not found")
        
        if orientation == 'v':
            fig = px.bar(
                self.df,
                x=x_column,
                y=y_column,
                color=color_column,
                title=title or f'{y_column} by {x_column}'
            )
        else:
            fig = px.bar(
                self.df,
                y=x_column,
                x=y_column,
                color=color_column,
                title=title or f'{y_column} by {x_column}',
                orientation='h'
            )
        
        return json.loads(fig.to_json())
    
    def create_box_plot(self, x_column: str, y_column: str,
                       color_column: str = None,
                       title: str = None) -> Dict[str, Any]:
        if x_column not in self.df.columns:
            raise ValueError(f"Column '{x_column}' not found")
        if y_column not in self.df.columns:
            raise ValueError(f"Column '{y_column}' not found")
        
        fig = px.box(
            self.df,
            x=x_column,
            y=y_column,
            color=color_column,
            title=title or f'Box Plot of {y_column} by {x_column}',
            points='all'
        )
        
        return json.loads(fig.to_json())
    
    def create_heatmap(self, x_column: str, y_column: str,
                      values_column: str = None,
                      title: str = None) -> Dict[str, Any]:
        if x_column not in self.df.columns:
            raise ValueError(f"Column '{x_column}' not found")
        if y_column not in self.df.columns:
            raise ValueError(f"Column '{y_column}' not found")
        
        if values_column:
            if values_column not in self.df.columns:
                raise ValueError(f"Column '{values_column}' not found")
            
            pivot_data = self.df.pivot_table(
                index=y_column,
                columns=x_column,
                values=values_column,
                aggfunc='mean',
                fill_value=0
            )
        else:
            # count heatmap
            pivot_data = pd.crosstab(self.df[y_column], self.df[x_column])
        
        fig = px.imshow(
            pivot_data,
            title=title or f'Heatmap: {y_column} vs {x_column}',
            labels=dict(x=x_column, y=y_column, color=values_column or 'count'),
            color_continuous_scale='Viridis'
        )
        
        return json.loads(fig.to_json())
    
    def create_correlation_matrix(self, title: str = None) -> Dict[str, Any]:
        numeric_df = self.df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            raise ValueError("Need at least 2 numeric columns for correlation matrix")
        
        corr_matrix = numeric_df.corr()
        
        fig = px.imshow(
            corr_matrix,
            title=title or 'Correlation Matrix',
            color_continuous_scale='RdBu_r',
            zmin=-1,
            zmax=1,
            text_auto='.2f'
        )
        
        return json.loads(fig.to_json())
    
    def create_pie_chart(self, names_column: str, values_column: str,
                        title: str = None) -> Dict[str, Any]:
        if names_column not in self.df.columns:
            raise ValueError(f"Column '{names_column}' not found")
        if values_column not in self.df.columns:
            raise ValueError(f"Column '{values_column}' not found")
        
        fig = px.pie(
            self.df,
            names=names_column,
            values=values_column,
            title=title or f'Distribution of {values_column} by {names_column}',
            hole=0.3
        )
        
        return json.loads(fig.to_json())
    
    def create_dashboard(self, config: Dict[str, Any]) -> Dict[str, Any]:
        
        rows = config.get('rows', 2)
        cols = config.get('cols', 2)
        plots = config.get('plots', [])
        
        fig = sp.make_subplots(
            rows=rows,
            cols=cols,
            subplot_titles=[plot.get('title', f'Plot {i+1}') for i in range(len(plots))]
        )
        
        plot_index = 0
        for row in range(1, rows + 1):
            for col in range(1, cols + 1):
                if plot_index < len(plots):
                    plot_config = plots[plot_index]
                    plot_type = plot_config.get('type')
                    
                    if plot_type == 'histogram':
                        trace = self._create_histogram_trace(plot_config)
                    elif plot_type == 'scatter':
                        trace = self._create_scatter_trace(plot_config)
                    elif plot_type == 'line':
                        trace = self._create_line_trace(plot_config)
                    elif plot_type == 'bar':
                        trace = self._create_bar_trace(plot_config)
                    else:
                        trace = None
                    
                    if trace:
                        fig.add_trace(trace, row=row, col=col)
                    
                    plot_index += 1
        
        fig.update_layout(height=800, title_text=config.get('title', 'Dashboard'))
        
        return json.loads(fig.to_json())
    
    def _create_histogram_trace(self, config: Dict[str, Any]) -> go.Histogram:
        column = config.get('column')
        if column in self.df.columns and pd.api.types.is_numeric_dtype(self.df[column]):
            return go.Histogram(x=self.df[column], name=config.get('name', column))
        return None
    
    def _create_scatter_trace(self, config: Dict[str, Any]) -> go.Scatter:
        x_col = config.get('x_column')
        y_col = config.get('y_column')
        
        if x_col in self.df.columns and y_col in self.df.columns:
            return go.Scatter(
                x=self.df[x_col],
                y=self.df[y_col],
                mode='markers',
                name=config.get('name', f'{y_col} vs {x_col}')
            )
        return None
    
    def _create_line_trace(self, config: Dict[str, Any]) -> go.Scatter:
        x_col = config.get('x_column')
        y_col = config.get('y_column')
        
        if x_col in self.df.columns and y_col in self.df.columns:
            df_sorted = self.df.sort_values(by=x_col)
            return go.Scatter(
                x=df_sorted[x_col],
                y=df_sorted[y_col],
                mode='lines+markers',
                name=config.get('name', f'{y_col} over {x_col}')
            )
        return None
    
    def _create_bar_trace(self, config: Dict[str, Any]) -> go.Bar:
        x_col = config.get('x_column')
        y_col = config.get('y_column')
        
        if x_col in self.df.columns and y_col in self.df.columns:
            return go.Bar(
                x=self.df[x_col],
                y=self.df[y_col],
                name=config.get('name', f'{y_col} by {x_col}')
            )
        return None
    
    def get_available_visualizations(self) -> List[Dict[str, Any]]:
        visualizations = [
            {
                'type': 'histogram',
                'name': 'Histogram',
                'description': 'Distribution of a numeric variable',
                'required_columns': 1,
                'column_types': ['numeric']
            },
            {
                'type': 'scatter',
                'name': 'Scatter Plot',
                'description': 'Relationship between two numeric variables',
                'required_columns': 2,
                'column_types': ['numeric', 'numeric']
            },
            {
                'type': 'line',
                'name': 'Line Plot',
                'description': 'Trend over time or ordered categories',
                'required_columns': 2,
                'column_types': ['any', 'numeric']
            },
            {
                'type': 'bar',
                'name': 'Bar Chart',
                'description': 'Comparison across categories',
                'required_columns': 2,
                'column_types': ['any', 'numeric']
            },
            {
                'type': 'box',
                'name': 'Box Plot',
                'description': 'Distribution comparison across categories',
                'required_columns': 2,
                'column_types': ['any', 'numeric']
            },
            {
                'type': 'heatmap',
                'name': 'Heatmap',
                'description': 'Matrix visualization of relationships',
                'required_columns': 2,
                'column_types': ['any', 'any']
            },
            {
                'type': 'correlation',
                'name': 'Correlation Matrix',
                'description': 'Correlations between all numeric variables',
                'required_columns': 2,
                'column_types': ['numeric']
            },
            {
                'type': 'pie',
                'name': 'Pie Chart',
                'description': 'Proportional composition',
                'required_columns': 2,
                'column_types': ['any', 'numeric']
            }
        ]
        
        return visualizations
