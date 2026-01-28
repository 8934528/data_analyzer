from flask import Flask, render_template, request, jsonify, send_file, session
import pandas as pd
import numpy as np
from pathlib import Path
import json
import tempfile
import os
from datetime import datetime

from config import Config
from upload_handler import FileUploadHandler
from data_processor import DataProcessor
from kql_engine import KQLEngine
from visualization import DataVisualizer

# Flask app
app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-123')

Config.ensure_directories()

data_store = {}
file_metadata = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Check file extension
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in Config.ALLOWED_EXTENSIONS:
            return jsonify({'error': f'File type not supported: {file_ext}'}), 400
        
        # Save uploaded file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = f"{timestamp}_{file.filename}"
        file_path = Config.UPLOAD_FOLDER / safe_filename
        file.save(file_path)
        
        upload_handler = FileUploadHandler()
        df, metadata = upload_handler.process_file(file_path)
        
        # Generate session ID
        session_id = f"session_{timestamp}"
        
        data_store[session_id] = {
            'dataframe': df,
            'processor': DataProcessor(df),
            'visualizer': DataVisualizer(df),
            'kql_engine': KQLEngine(df),
            'file_path': str(file_path),
            'metadata': metadata
        }
        
        session['session_id'] = session_id
        
        response_data = {
            'session_id': session_id,
            'message': 'File uploaded successfully',
            'metadata': metadata,
            'preview': {
                'columns': list(df.columns),
                'first_rows': df.head(10).fillna('').to_dict('records'),
                'shape': {'rows': len(df), 'columns': len(df.columns)}
            }
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/preview', methods=['GET'])
def preview_data():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        df = data_info['dataframe']
        
        rows = min(int(request.args.get('rows', 10)), 100)
        page = int(request.args.get('page', 1))
        start_idx = (page - 1) * rows
        end_idx = start_idx + rows
        
        preview_df = df.iloc[start_idx:end_idx]
        
        response_data = {
            'data': preview_df.fillna('').to_dict('records'),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'page': page,
            'rows_per_page': rows,
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/statistics', methods=['GET'])
def get_statistics():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        processor = data_info['processor']
        
        stats = processor.calculate_statistics()
        
        return jsonify(stats)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clean', methods=['POST'])
def clean_data():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        processor = data_info['processor']
        
        methods = request.json.get('methods', {})
        
        cleaned_df = processor.clean_data(methods)
        
        data_info['dataframe'] = cleaned_df
        data_info['processor'] = DataProcessor(cleaned_df)
        data_info['visualizer'] = DataVisualizer(cleaned_df)
        data_info['kql_engine'] = KQLEngine(cleaned_df)
        
        response_data = {
            'message': 'Data cleaned successfully',
            'transformations': processor.transformations[-1] if processor.transformations else None,
            'new_shape': {'rows': len(cleaned_df), 'columns': len(cleaned_df.columns)}
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/kql/query', methods=['POST'])
def execute_kql_query():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        kql_engine = data_info['kql_engine']
        
        # Get query from request
        query = request.json.get('query', '')
        
        if not query:
            return jsonify({'error': 'No query provided'}), 400
        
        # Execute query
        result_df = kql_engine.execute_query(query)
        
        # Convert result to JSON
        result_data = {
            'data': result_df.fillna('').to_dict('records'),
            'columns': list(result_df.columns),
            'row_count': len(result_df),
            'query': query
        }
        
        return jsonify(result_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/kql/examples', methods=['GET'])
def get_kql_examples():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        kql_engine = data_info['kql_engine']
        
        examples = kql_engine.get_query_examples()
        
        return jsonify({'examples': examples})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/visualize', methods=['POST'])
def create_visualization():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        visualizer = data_info['visualizer']
        
        viz_type = request.json.get('type')
        params = request.json.get('params', {})
        
        if viz_type == 'histogram':
            result = visualizer.create_histogram(**params)
        elif viz_type == 'scatter':
            result = visualizer.create_scatter_plot(**params)
        elif viz_type == 'line':
            result = visualizer.create_line_plot(**params)
        elif viz_type == 'bar':
            result = visualizer.create_bar_chart(**params)
        elif viz_type == 'box':
            result = visualizer.create_box_plot(**params)
        elif viz_type == 'heatmap':
            result = visualizer.create_heatmap(**params)
        elif viz_type == 'correlation':
            result = visualizer.create_correlation_matrix(**params)
        elif viz_type == 'pie':
            result = visualizer.create_pie_chart(**params)
        elif viz_type == 'dashboard':
            result = visualizer.create_dashboard(params)
        else:
            return jsonify({'error': f'Unknown visualization type: {viz_type}'}), 400
        
        return jsonify({'plotly_json': result})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/visualize/available', methods=['GET'])
def get_available_visualizations():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        visualizer = data_info['visualizer']
        
        available_viz = visualizer.get_available_visualizations()
        
        return jsonify({'visualizations': available_viz})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export', methods=['POST'])
def export_data():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        processor = data_info['processor']
        
        export_format = request.json.get('format', 'csv')
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{export_format}') as tmp_file:
            tmp_path = tmp_file.name
        
        output_path = processor.export_data(format=export_format, path=tmp_path)
        
        # Send file
        return send_file(
            output_path,
            as_attachment=True,
            download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.{export_format}',
            mimetype='application/octet-stream'
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/aggregate', methods=['POST'])
def aggregate_data():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        processor = data_info['processor']
        
        group_by = request.json.get('group_by', [])
        aggregations = request.json.get('aggregations', {})
        
        # Aggregate data
        aggregated_df = processor.aggregate_data(group_by, aggregations)
        
        data_info['dataframe'] = aggregated_df
        data_info['processor'] = DataProcessor(aggregated_df)
        data_info['visualizer'] = DataVisualizer(aggregated_df)
        data_info['kql_engine'] = KQLEngine(aggregated_df)
        
        response_data = {
            'message': 'Data aggregated successfully',
            'data': aggregated_df.fillna('').to_dict('records'),
            'columns': list(aggregated_df.columns),
            'shape': {'rows': len(aggregated_df), 'columns': len(aggregated_df.columns)}
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/filter', methods=['POST'])
def filter_data():
    try:
        session_id = session.get('session_id')
        if not session_id or session_id not in data_store:
            return jsonify({'error': 'No data available. Please upload a file first.'}), 400
        
        data_info = data_store[session_id]
        processor = data_info['processor']
        
        conditions = request.json.get('conditions', [])
        
        # Filter data
        filtered_df = processor.filter_data(conditions)
        
        data_info['dataframe'] = filtered_df
        data_info['processor'] = DataProcessor(filtered_df)
        data_info['visualizer'] = DataVisualizer(filtered_df)
        data_info['kql_engine'] = KQLEngine(filtered_df)
        
        response_data = {
            'message': 'Data filtered successfully',
            'data': filtered_df.fillna('').to_dict('records'),
            'columns': list(filtered_df.columns),
            'shape': {'rows': len(filtered_df), 'columns': len(filtered_df.columns)}
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/session/clear', methods=['POST'])
def clear_session():
    """Clear current session"""
    try:
        session_id = session.get('session_id')
        if session_id and session_id in data_store:
            file_path = Path(data_store[session_id]['file_path'])
            if file_path.exists():
                file_path.unlink()
            
            del data_store[session_id]
        
        session.clear()
        
        return jsonify({'message': 'Session cleared successfully'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
