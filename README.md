# data_analyzer

A comprehensive web-based data analysis platform that supports multiple file formats, KQL-based querying, interactive visualizations, and data transformation tools.

![version](https://img.shields.io/badge/version-1.0.0-blue)
![python](https://img.shields.io/badge/python-3.8%252B-green)
![license](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Features

### Multi-Format Support

CSV: Comma-separated values files
Excel: .xlsx, .xls, .xlsb, .xltx formats
OpenDocument: .ods spreadsheets
Parquet: Efficient columnar storage format
JSON: JavaScript Object Notation

### KQL-Based Query Engine

Kusto Query Language (KQL) syntax for data exploration
Real-time query execution
Built-in query examples and templates
Support for filtering, aggregation, and transformation

### Interactive Visualizations

Histograms, scatter plots, line charts, bar charts
Box plots, heatmaps, correlation matrices, pie charts
Interactive Plotly charts with zoom and pan
Customizable chart parameters

### Data Transformation

Aggregation with group by operations
Advanced filtering with multiple conditions
Pivot table creation
Real-time data preview

### Export Capabilities

Multiple output formats: CSV, Excel, JSON, Parquet
Download processed data
Session management

### User-Friendly Interface

Modern web-based UI
Drag & drop file upload
Real-time preview
Responsive design

---

## project structure

    data_analyzer/
        ├── app.py
        ├── config.py
        ├── data_processor.py
        ├── kql_engine.py
        ├── visualization.py
        ├── upload_handler.py
        ├── requirements.txt
        ├── templates/
        │   └── index.html
        ├── static/
        │   └── styles.css
        ├── uploads/
        │   └── .gitkeep
        └── tests/
            └── test_data_processor.py

## Quick Start

Prerequisites
Python 3.8 or higher

pip (Python package manager)

## Installation

1. Clone the repository

        bash
        git clone <repository-url>
        cd data_analyzer

2. Create virtual environment

        bash
        python -m venv venv

        # On Windows:
        venv\Scripts\activate

        # On macOS/Linux:
        source venv/bin/activate

3. Install dependencies

        bash
        pip install -r requirements.txt

4. Run the application

        bash
        python app.py

5. Access the platform

- Open your browser and navigate to:

        text
        http://localhost:5000

---

## Usage Guide

1. Upload Data

    - Click on the upload area or drag & drop files
    - Supported formats: CSV, Excel (.xlsx, .xls, .xlsb), ODS, Parquet, JSON
    - Maximum file size: 16MB

2. Preview Data

    - View first 10 rows of your data
    - See column information and data types
    - Navigate through data pages

3. Analyze Statistics

    - Get comprehensive statistics for each column
    - View distribution metrics for numeric data
    - Check missing value patterns
    - Generate correlation matrices

4. Clean Data

    - Fill missing values using various methods
    - Remove outliers using Z-score or IQR
    - Eliminate duplicate rows
    - Convert data types automatically

5. Query with KQL

    - *Kusto Query Language (KQL) Examples*

    | Query Purpose                                     | KQL Syntax                                                             |
    |---------------------------------------------------|------------------------------------------------------------------------|
    | Filter data and summarize total sales by region   | `Data \| where sales > 1000 \| summarize total = sum(sales) by region` |
    | Select specific columns and sort results          | `Data \| project column1, column2 \| sort by column1 desc`             |
    | Count records by category and show top 10         | `Data \| summarize count() by category \| top 10 by count_`            |

6. Create Visualizations

    - Select chart type from dropdown
    - Choose columns for X and Y axes
    - Customize colors and parameters
    - Generate interactive plots

7. Transform Data

    - Aggregate: Group data and apply functions (sum, mean, count)
    - Filter: Apply multiple conditions to filter rows
    - Pivot: Create pivot tables from your data

8. Export Results

    - Export cleaned/transformed data in multiple formats
    - Choose from CSV, Excel, JSON, or Parquet
    - Download directly to your computer

## File Size Limits

- **Default maximum file size:** 16MB  
- **Configurable via:** `config.py`

## Supported File Formats

| Format           | Extension        | Description                          |
|------------------|------------------|--------------------------------------|
| CSV              | `.csv`           | Comma-separated values               |
| Excel            | `.xlsx`, `.xls`  | Excel spreadsheets                   |
| Excel Binary     | `.xlsb`          | Binary Excel format                  |
| Excel Template   | `.xltx`          | Excel template                       |
| OpenDocument     | `.ods`           | LibreOffice / OpenOffice format      |
| Parquet          | `.parquet`       | Columnar storage format              |
| JSON             | `.json`          | JavaScript Object Notation           |

---

## KQL Query Examples

### Basic Filtering

        kql
        Data | where age > 18
        Data | where status == "active" and value > 100

### Aggregation

        kql
        Data | summarize avg_salary=avg(salary) by department
        Data | summarize count() by category, status

### Sorting and Limiting

        kql
        Data | sort by timestamp desc | take 100
        Data | top 10 by revenue desc

### Column Selection

        kql
        Data | project id, name, email
        Data | project-away sensitive_data  # Remove specific columns

### String Operations

        kql
        Data | extend lower_name=tolower(name)
        Data | where name contains "john"

### Date Operations

        kql
        Data | where timestamp > ago(7d)
        Data | summarize count() by bin(timestamp, 1d)

---

## Visualization Types

### Available Charts

1. *Histogram* - Distribution of numeric data
2. Scatter Plot - *Relationship between two numeric variables*
3. *Line Chart* - Trends over time or ordered categories
4. Bar Chart - *Comparison across categories*
5. *Box Plot* - Distribution comparison
6. Heatmap - *Matrix visualization*
7. *Correlation Matrix* - Relationships between variables
8. Pie Chart - *Proportional composition*

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

### Development Setup

bash
        # Install development dependencies
        pip install -r requirements-dev.txt

        # Run tests
        pytest tests/

        # Run linting
        flake8 .

## Acknowledgments

- Pandas for data manipulation
- Plotly for interactive visualizations
- Flask for web framework
- Bootstrap for UI components

---

*Happy Data Analyzing!*

---
