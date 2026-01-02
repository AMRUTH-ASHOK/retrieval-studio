"""
Leaderboard API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List

from backend.models.schemas import LeaderboardEntry
from backend.auth import get_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.query_builder import escape_identifier

router = APIRouter()


@router.get("/{run_id}", response_model=List[LeaderboardEntry])
async def get_leaderboard(run_id: str, sql_connector=Depends(get_sql_connector)):
    """Get leaderboard for a specific run"""
    try:
        from utils.query_builder import sanitize_string
        
        # Validate and sanitize run_id for table name
        # Only allow alphanumeric, underscore, and hyphen
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', run_id):
            raise HTTPException(status_code=400, detail="Invalid run_id format")
        
        safe_run_id = run_id.replace("-", "_")
        # Escape the table name parts
        table_name = f"{escape_identifier(settings.CATALOG)}.{escape_identifier(settings.SCHEMA)}.rl_leaderboard_{escape_identifier(safe_run_id)}"
        
        query = f"""
            SELECT 
                strategy,
                avg_recall_at_5,
                avg_recall_at_10,
                avg_ndcg_at_5,
                avg_ndcg_at_10,
                avg_latency_ms,
                num_queries
            FROM {table_name}
            ORDER BY avg_recall_at_10 DESC
        """
        
        results = sql_connector.execute(query)
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Leaderboard not found: {str(e)}")


@router.get("/project/{project_id}/aggregate")
async def get_project_leaderboard(project_id: str, sql_connector=Depends(get_sql_connector)):
    """Get aggregated leaderboard across all runs in a project"""
    try:
        from utils.query_builder import sanitize_string
        
        project_id_safe = sanitize_string(project_id)
        
        query = f"""
            SELECT 
                r.run_id,
                r.created_at,
                e.strategy,
                AVG(CAST(get_json_object(e.metrics, '$.recall_at_10') AS DOUBLE)) as avg_recall_at_10,
                AVG(CAST(get_json_object(e.metrics, '$.ndcg_at_10') AS DOUBLE)) as avg_ndcg_at_10,
                COUNT(*) as num_queries
            FROM {escape_identifier(settings.CATALOG)}.{escape_identifier(settings.SCHEMA)}.rl_runs r
            JOIN {escape_identifier(settings.CATALOG)}.{escape_identifier(settings.SCHEMA)}.rl_eval_results e
            ON r.run_id = e.run_id
            WHERE r.project_id = ?
            GROUP BY r.run_id, r.created_at, e.strategy
            ORDER BY r.created_at DESC, avg_recall_at_10 DESC
        """
        
        results = sql_connector.execute(query, [project_id_safe])
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
