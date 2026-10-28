"""
Database Management Module for Real Estate ETL Pipeline.

Manages PostgreSQL connection lifecycle, schema migrations, table creation,
index creation, transaction control, and table backups for full reload scenarios.
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

from etl_pipeline.logger import get_logger

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    try:
        import psycopg as psycopg2
        from psycopg import sql
        execute_values = None
    except ImportError:
        psycopg2 = None
        sql = None
        execute_values = None


class DatabaseError(Exception):
    """Raised when database operations encounter an error."""
    pass


class DatabaseManager:
    """
    Manages PostgreSQL database interactions for the real estate ETL system.
    
    Provides schema initialization, index creation, backup table generation,
    and connection validation.
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize the DatabaseManager.
        
        Args:
            db_config: Dictionary containing host, port, database, user, password, schema
        """
        self.logger = get_logger()
        self.config = db_config
        self.connection = None
        
    def get_connection(self):
        """Establish or return an active PostgreSQL connection."""
        if not psycopg2:
            raise DatabaseError("psycopg2 is not installed. Please install psycopg2-binary.")
            
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(
                    host=self.config.get('host', 'localhost'),
                    port=self.config.get('port', 5432),
                    dbname=self.config.get('database', 'real_estate'),
                    user=self.config.get('user', 'postgres'),
                    password=self.config.get('password', 'postgres'),
                    connect_timeout=10
                )
                self.connection.autocommit = False
            except Exception as e:
                raise DatabaseError(f"Failed to connect to PostgreSQL: {e}")
                
        return self.connection

    def test_connection(self) -> bool:
        """Validate database connectivity."""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                res = cur.fetchone()
                return bool(res and res[0] == 1)
        except Exception as e:
            self.logger.warning(f"Database connection test failed: {e}")
            return False

    def initialize_schema(self, schema_file: Optional[str] = None) -> None:
        """
        Initialize database tables, types, and views.
        
        Args:
            schema_file: Path to SQL schema definition file.
        """
        if schema_file and Path(schema_file).exists():
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
        else:
            default_path = Path(__file__).resolve().parent.parent / 'database' / 'schema.sql'
            if default_path.exists():
                with open(default_path, 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
            else:
                schema_sql = self._get_fallback_schema_sql()

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            conn.commit()
            self.logger.info("Successfully initialized PostgreSQL database schema and indexes.")
        except Exception as e:
            conn.rollback()
            raise DatabaseError(f"Failed to initialize schema: {e}")

    def create_backup_table(self, source_table: str = 'properties') -> Optional[str]:
        """
        Create a timestamped backup copy of the target table before full reload.
        
        Returns:
            Name of the backup table created, or None if source table is empty/nonexistent.
        """
        conn = self.get_connection()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_table = f"{source_table}_backup_{timestamp}"
        
        try:
            with conn.cursor() as cur:
                # Check if source table exists and has rows
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    );
                """, (self.config.get('schema', 'public'), source_table))
                exists = cur.fetchone()[0]
                
                if exists:
                    cur.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {source_table};")
                    conn.commit()
                    self.logger.info(f"Created table backup: {backup_table}")
                    return backup_table
                return None
        except Exception as e:
            conn.rollback()
            self.logger.warning(f"Could not create backup table: {e}")
            return None

    def close(self) -> None:
        """Close active database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.logger.info("Closed PostgreSQL database connection.")

    def _get_fallback_schema_sql(self) -> str:
        """Inline fallback schema DDL."""
        return """
        CREATE TABLE IF NOT EXISTS dim_property_types (
            property_type_code VARCHAR(10) PRIMARY KEY,
            category_name VARCHAR(50) NOT NULL,
            description TEXT
        );

        INSERT INTO dim_property_types (property_type_code, category_name)
        VALUES 
            ('D', 'Detached'),
            ('S', 'Semi-Detached'),
            ('T', 'Terraced'),
            ('F', 'Flat/Maisonette'),
            ('O', 'Other')
        ON CONFLICT (property_type_code) DO NOTHING;

        CREATE TABLE IF NOT EXISTS properties (
            transaction_id VARCHAR(100) PRIMARY KEY,
            price NUMERIC(15, 2) NOT NULL,
            date_of_transfer DATE NOT NULL,
            sale_year INT NOT NULL,
            sale_month INT NOT NULL,
            months_since_transaction INT,
            postcode VARCHAR(20) NOT NULL,
            town_city VARCHAR(150) NOT NULL,
            district VARCHAR(150),
            county VARCHAR(150),
            region VARCHAR(100),
            country VARCHAR(100),
            property_type VARCHAR(10) NOT NULL,
            property_type_category VARCHAR(50) NOT NULL,
            duration_type VARCHAR(30),
            is_freehold BOOLEAN NOT NULL DEFAULT TRUE,
            is_new_build BOOLEAN NOT NULL DEFAULT FALSE,
            address_line VARCHAR(500),
            square_meters NUMERIC(10, 2),
            price_per_square_meter NUMERIC(12, 2),
            latitude NUMERIC(10, 7),
            longitude NUMERIC(10, 7),
            data_quality_score SMALLINT NOT NULL,
            dedup_hash VARCHAR(64) NOT NULL UNIQUE,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_properties_postcode ON properties(postcode);
        CREATE INDEX IF NOT EXISTS idx_properties_town_city ON properties(town_city);
        CREATE INDEX IF NOT EXISTS idx_properties_date_of_transfer ON properties(date_of_transfer);
        CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
        CREATE INDEX IF NOT EXISTS idx_properties_city_year ON properties(town_city, sale_year);
        """
