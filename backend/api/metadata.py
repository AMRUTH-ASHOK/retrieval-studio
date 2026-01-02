"""
Data types and strategies API endpoints
"""
from fastapi import APIRouter
from typing import List

from backend.models.schemas import DataTypeInfo, StrategyInfo
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from core.data_types import get_all_data_types
from core.strategies import get_all_strategies

router = APIRouter()


@router.get("/data-types", response_model=List[DataTypeInfo])
async def get_data_types():
    """Get all available data types"""
    data_types = get_all_data_types()
    return [
        {
            "name": dt.get_name(),
            "display_name": dt.get_display_name(),
            "input_schema": dt.get_input_schema(),
            "compatible_strategies": dt.get_compatible_strategies()
        }
        for dt in data_types
    ]


@router.get("/strategies", response_model=List[StrategyInfo])
async def get_strategies():
    """Get all available chunking strategies"""
    strategies = get_all_strategies()
    return strategies
