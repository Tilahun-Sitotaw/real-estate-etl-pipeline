"""
Setup script for Real Estate ETL Pipeline.
"""

from setuptools import setup, find_packages

setup(
    name="real_estate_etl",
    version="1.0.0",
    author="Data Engineering Group",
    description="Scalable ETL Pipeline for Nationwide Real Estate Assessment and Sales Analysis",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pandas>=2.0.0",
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.0",
        "psutil>=5.9.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "real-estate-etl=etl_pipeline.main:main"
        ]
    }
)
