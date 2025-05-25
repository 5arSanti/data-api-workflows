import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from datetime import datetime
import json
import os

def fetch_vacancies():
    url = "https://www.buscadordeempleo.gov.co/backbue/v1//vacantes/resultados"
    response = requests.get(url)
    return response.json()['resultados']

def process_vacancies(vacancies):
    df = pd.DataFrame(vacancies)
    
    df['FECHA_PUBLICACION'] = pd.to_datetime(df['FECHA_PUBLICACION'])
    df['FECHA_VENCIMIENTO'] = pd.to_datetime(df['FECHA_VENCIMIENTO'])
    
    df['SALARIO_MIN'] = df['RANGO_SALARIAL'].str.extract(r'\$(\d+\.?\d*)').astype(float)
    df['SALARIO_MAX'] = df['RANGO_SALARIAL'].str.extract(r'\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)')[1].astype(float)
    
    text_columns = ['TITULO_VACANTE', 'DESCRIPCION_VACANTE', 'CARGO']
    for col in text_columns:
        df[col] = df[col].str.replace('Ã³', 'ó').str.replace('Ã­', 'í')
    
    return df

def analyze_vacancies(df):
    os.makedirs('analysis', exist_ok=True)
    
    plt.figure(figsize=(12, 6))
    dept_counts = df['DEPARTAMENTO'].value_counts().head(10)
    sns.barplot(x=dept_counts.values, y=dept_counts.index)
    plt.title('Top 10 Departments by Number of Vacancies')
    plt.tight_layout()
    plt.savefig('analysis/vacancies_by_department.png')
    plt.close()
    
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='SALARIO_MIN', bins=30)
    plt.title('Distribution of Minimum Salaries')
    plt.tight_layout()
    plt.savefig('analysis/salary_distribution.png')
    plt.close()
    
    plt.figure(figsize=(10, 6))
    contract_counts = df['TIPO_CONTRATO'].value_counts()
    sns.barplot(x=contract_counts.values, y=contract_counts.index)
    plt.title('Distribution of Contract Types')
    plt.tight_layout()
    plt.savefig('analysis/contract_types.png')
    plt.close()
    
    summary_stats = {
        'Métrica': [
            'Total de Vacantes',
            'Número de Departamentos',
            'Salario Mínimo Promedio',
            'Salario Máximo Promedio',
            'Tipo de Contrato Más Común'
        ],
        'Valor': [
            len(df),
            df['DEPARTAMENTO'].nunique(),
            f"${df['SALARIO_MIN'].mean():,.2f}",
            f"${df['SALARIO_MAX'].mean():,.2f}",
            df['TIPO_CONTRATO'].mode()[0]
        ]
    }
    
    return pd.DataFrame(summary_stats)

def store_vacancies(df):
    engine = create_engine('mysql+mysqlconnector://root:rootpassword@localhost:3306/vacancies')
    
    df.to_sql('job_vacancies', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        conn.execute('CREATE INDEX idx_departamento ON job_vacancies(DEPARTAMENTO)')
        conn.execute('CREATE INDEX idx_fecha_publicacion ON job_vacancies(FECHA_PUBLICACION)')
        conn.execute('CREATE INDEX idx_cargo ON job_vacancies(CARGO)')
