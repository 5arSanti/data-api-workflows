import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from datetime import datetime
import json

def fetch_vacancies():
    """Fetch job vacancies from the API"""
    url = "https://www.buscadordeempleo.gov.co/backbue/v1//vacantes/resultados"
    response = requests.get(url)
    return response.json()['resultados']

def process_vacancies(vacancies):
    """Process and transform the vacancy data"""
    # Convert to DataFrame
    df = pd.DataFrame(vacancies)
    
    # Clean and transform data
    df['FECHA_PUBLICACION'] = pd.to_datetime(df['FECHA_PUBLICACION'])
    df['FECHA_VENCIMIENTO'] = pd.to_datetime(df['FECHA_VENCIMIENTO'])
    
    # Extract salary ranges
    df['SALARIO_MIN'] = df['RANGO_SALARIAL'].str.extract(r'\$(\d+\.?\d*)').astype(float)
    df['SALARIO_MAX'] = df['RANGO_SALARIAL'].str.extract(r'\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)')[1].astype(float)
    
    # Clean text fields
    text_columns = ['TITULO_VACANTE', 'DESCRIPCION_VACANTE', 'CARGO']
    for col in text_columns:
        df[col] = df[col].str.replace('Ã³', 'ó').str.replace('Ã­', 'í')
    
    return df

def analyze_vacancies(df):
    """Perform basic analysis on the vacancy data"""
    # Create analysis directory if it doesn't exist
    import os
    os.makedirs('analysis', exist_ok=True)
    
    # 1. Vacancies by Department
    plt.figure(figsize=(12, 6))
    dept_counts = df['DEPARTAMENTO'].value_counts().head(10)
    sns.barplot(x=dept_counts.values, y=dept_counts.index)
    plt.title('Top 10 Departments by Number of Vacancies')
    plt.tight_layout()
    plt.savefig('analysis/vacancies_by_department.png')
    plt.close()
    
    # 2. Salary Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='SALARIO_MIN', bins=30)
    plt.title('Distribution of Minimum Salaries')
    plt.tight_layout()
    plt.savefig('analysis/salary_distribution.png')
    plt.close()
    
    # 3. Contract Types
    plt.figure(figsize=(10, 6))
    contract_counts = df['TIPO_CONTRATO'].value_counts()
    sns.barplot(x=contract_counts.values, y=contract_counts.index)
    plt.title('Distribution of Contract Types')
    plt.tight_layout()
    plt.savefig('analysis/contract_types.png')
    plt.close()
    
    # Save summary statistics
    summary_stats = {
        'total_vacancies': len(df),
        'departments_count': df['DEPARTAMENTO'].nunique(),
        'avg_salary_min': df['SALARIO_MIN'].mean(),
        'avg_salary_max': df['SALARIO_MAX'].mean(),
        'most_common_contract': df['TIPO_CONTRATO'].mode()[0]
    }
    
    with open('analysis/summary_stats.json', 'w') as f:
        json.dump(summary_stats, f, indent=4)

def store_vacancies(df):
    """Store the processed data in MySQL"""
    # Create SQLAlchemy engine
    engine = create_engine('mysql+mysqlconnector://user:password@localhost:3306/mydatabase')
    
    # Create table and store data
    df.to_sql('job_vacancies', engine, if_exists='replace', index=False)
    
    # Create indexes for common queries
    with engine.connect() as conn:
        conn.execute('CREATE INDEX idx_departamento ON job_vacancies(DEPARTAMENTO)')
        conn.execute('CREATE INDEX idx_fecha_publicacion ON job_vacancies(FECHA_PUBLICACION)')
        conn.execute('CREATE INDEX idx_cargo ON job_vacancies(CARGO)')

def main():
    # Fetch and process data
    vacancies = fetch_vacancies()
    df = process_vacancies(vacancies)
    
    # Analyze data
    analyze_vacancies(df)
    
    # Store in database
    store_vacancies(df)
    
    print("Data processing completed successfully!")

if __name__ == "__main__":
    main() 