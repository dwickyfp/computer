"""
Credit usage schemas.

Defines schemas for credit usage data response.
"""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class WeeklyMonthlyUsage(BaseModel):
    """Weekly and Monthly credit usage stats."""
    
    current_week: float = Field(..., description="Total credits used in current week")
    current_month: float = Field(..., description="Total credits used in current month")
    previous_week: float = Field(..., description="Total credits used in previous week")
    previous_month: float = Field(..., description="Total credits used in previous month")


class DailyUsage(BaseModel):
    """Daily credit usage for chart."""
    
    date: datetime = Field(..., description="Usage date")
    credits: float = Field(..., description="Credits used")


class CreditUsageResponse(BaseModel):
    """Response schema for credit usage data."""
    
    summary: WeeklyMonthlyUsage
    daily_usage: List[DailyUsage] = Field(..., description="Daily usage for the last 30 days")
