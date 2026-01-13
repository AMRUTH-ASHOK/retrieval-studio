"""
Query Builder Utility for Safe SQL Query Construction
Provides parameterized query building to prevent SQL injection
"""
from typing import Dict, Any, List, Optional
import re


def escape_identifier(identifier: str) -> str:
    """
    Escape SQL identifier (table name, column name, etc.)
    Only allows alphanumeric, underscore, and dot characters
    
    Args:
        identifier: SQL identifier to escape
    
    Returns:
        Escaped identifier
    """
    # Remove any characters that aren't alphanumeric, underscore, or dot
    # This prevents SQL injection through identifier names
    if not identifier or not isinstance(identifier, str):
        raise ValueError(f"Invalid identifier: {identifier}")
    
    # Allow alphanumeric, underscore, dot (for catalog.schema.table)
    if not re.match(r'^[a-zA-Z0-9_.]+$', identifier):
        raise ValueError(f"Invalid identifier format: {identifier}")
    
    return identifier


def build_select_query(
    table: str,
    columns: List[str] = None,
    where: Dict[str, Any] = None,
    order_by: str = None,
    limit: int = None,
    distinct: bool = False
) -> tuple[str, Dict[str, Any]]:
    """
    Build a parameterized SELECT query
    
    Args:
        table: Table name (catalog.schema.table)
        columns: List of column names (defaults to *)
        where: Dictionary of WHERE conditions {column: value}
        order_by: ORDER BY clause (column name)
        limit: LIMIT value
        distinct: Whether to use SELECT DISTINCT
    
    Returns:
        Tuple of (query_string, parameters_dict)
    """
    # Escape table name
    table_parts = table.split('.')
    escaped_table = '.'.join(escape_identifier(part) for part in table_parts)
    
    # Build SELECT clause
    if columns:
        escaped_columns = [escape_identifier(col) for col in columns]
        select_clause = ', '.join(escaped_columns)
    else:
        select_clause = '*'
    
    if distinct:
        query = f"SELECT DISTINCT {select_clause} FROM {escaped_table}"
    else:
        query = f"SELECT {select_clause} FROM {escaped_table}"
    
    parameters = {}
    
    # Build WHERE clause
    if where:
        conditions = []
        for i, (col, value) in enumerate(where.items()):
            escaped_col = escape_identifier(col)
            param_name = f"param_{i}"
            conditions.append(f"{escaped_col} = :{param_name}")
            parameters[param_name] = value
        
        query += " WHERE " + " AND ".join(conditions)
    
    # Build ORDER BY clause
    if order_by:
        escaped_order_by = escape_identifier(order_by)
        query += f" ORDER BY {escaped_order_by}"
    
    # Build LIMIT clause
    if limit:
        query += f" LIMIT {int(limit)}"
    
    return query, parameters


def build_insert_query(
    table: str,
    data: Dict[str, Any]
) -> tuple[str, Dict[str, Any]]:
    """
    Build a parameterized INSERT query
    
    Args:
        table: Table name (catalog.schema.table)
        data: Dictionary of column: value pairs
    
    Returns:
        Tuple of (query_string, parameters_dict)
    """
    # Escape table name
    table_parts = table.split('.')
    escaped_table = '.'.join(escape_identifier(part) for part in table_parts)
    
    # Escape column names
    columns = [escape_identifier(col) for col in data.keys()]
    
    # Build parameter names
    parameters = {}
    param_names = []
    for i, col in enumerate(columns):
        param_name = f"param_{i}"
        param_names.append(f":{param_name}")
        parameters[param_name] = data[col]
    
    query = f"""
        INSERT INTO {escaped_table} ({', '.join(columns)})
        VALUES ({', '.join(param_names)})
    """
    
    return query.strip(), parameters


def build_update_query(
    table: str,
    set_values: Dict[str, Any],
    where: Dict[str, Any]
) -> tuple[str, Dict[str, Any]]:
    """
    Build a parameterized UPDATE query
    
    Args:
        table: Table name (catalog.schema.table)
        set_values: Dictionary of column: value pairs to set
        where: Dictionary of WHERE conditions {column: value}
    
    Returns:
        Tuple of (query_string, parameters_dict)
    """
    # Escape table name
    table_parts = table.split('.')
    escaped_table = '.'.join(escape_identifier(part) for part in table_parts)
    
    parameters = {}
    param_index = 0
    
    # Build SET clause
    set_clauses = []
    for col, value in set_values.items():
        escaped_col = escape_identifier(col)
        param_name = f"param_{param_index}"
        set_clauses.append(f"{escaped_col} = :{param_name}")
        parameters[param_name] = value
        param_index += 1
    
    # Build WHERE clause
    where_clauses = []
    for col, value in where.items():
        escaped_col = escape_identifier(col)
        param_name = f"param_{param_index}"
        where_clauses.append(f"{escaped_col} = :{param_name}")
        parameters[param_name] = value
        param_index += 1
    
    query = f"""
        UPDATE {escaped_table}
        SET {', '.join(set_clauses)}
        WHERE {' AND '.join(where_clauses)}
    """
    
    return query.strip(), parameters


def sanitize_string(value: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize string value for safe SQL usage
    This is a fallback - prefer parameterized queries
    
    Args:
        value: String value to sanitize
        max_length: Optional maximum length
    
    Returns:
        Sanitized string
    """
    if not isinstance(value, str):
        value = str(value)
    
    # Truncate if needed
    if max_length and len(value) > max_length:
        value = value[:max_length]
    
    # Replace single quotes with double single quotes (SQL escaping)
    # Note: This is a fallback - parameterized queries are preferred
    return value.replace("'", "''")
