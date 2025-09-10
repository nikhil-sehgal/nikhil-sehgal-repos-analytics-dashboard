"""
Data storage and file management system for GitHub Analytics Tracker.
Handles JSON file operations, data aggregation, and file organization.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from utils import (
    safe_json_load, safe_json_save, ensure_directory, 
    get_current_utc_timestamp, format_date_for_data_key,
    format_month_for_data_key, DataFileManager
)


@dataclass
class DailyMetrics:
    """Daily metrics data structure."""
    date: str
    views: int
    unique_visitors: int
    clones: int
    unique_cloners: int
    timestamp: str
    
    def to_dict(self) -> Dict:
        return {
            'views': self.views,
            'unique_visitors': self.unique_visitors,
            'clones': self.clones,
            'unique_cloners': self.unique_cloners,
            'timestamp': self.timestamp
        }


class AnalyticsDataStorage:
    """Manages analytics data storage with JSON files."""
    
    def __init__(self, base_path: str = "data"):
        self.base_path = base_path
        self.file_manager = DataFileManager(base_path)
        self.logger = logging.getLogger("github_analytics.storage")
        ensure_directory(base_path)
    
    def store_daily_metrics(self, owner: str, repo: str, date: datetime, 
                          views: int, unique_visitors: int, clones: int, 
                          unique_cloners: int) -> bool:
        """Store daily metrics for a repository."""
        try:
            year = date.year
            date_key = format_date_for_data_key(date)
            
            # Load existing data for the year
            daily_data = self.file_manager.load_daily_data(owner, repo, year)
            
            # Add/update the daily entry
            daily_data[date_key] = {
                'views': views,
                'unique_visitors': unique_visitors,
                'clones': clones,
                'unique_cloners': unique_cloners,
                'timestamp': get_current_utc_timestamp()
            }
            
            # Save updated data
            success = self.file_manager.save_daily_data(owner, repo, year, daily_data)
            
            if success:
                self.logger.info(f"Stored daily metrics for {owner}/{repo} on {date.date()}")
            else:
                self.logger.error(f"Failed to store daily metrics for {owner}/{repo}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error storing daily metrics: {e}")
            return False    

    def store_repository_metadata(self, owner: str, repo: str, metadata: Dict[str, int]) -> bool:
        """Store repository metadata (stars, forks, etc.)."""
        try:
            current_date = datetime.now(timezone.utc)
            date_key = format_date_for_data_key(current_date)
            
            # Load existing metadata
            metadata_data = self.file_manager.load_repository_metadata(owner, repo)
            
            # Add timestamp to metadata
            timestamped_metadata = metadata.copy()
            timestamped_metadata['timestamp'] = get_current_utc_timestamp()
            
            # Store by date
            metadata_data[date_key] = timestamped_metadata
            
            # Save updated data
            success = self.file_manager.save_repository_metadata(owner, repo, metadata_data)
            
            if success:
                self.logger.info(f"Stored repository metadata for {owner}/{repo}: "
                               f"{metadata['stars']} stars, {metadata['forks']} forks")
            else:
                self.logger.error(f"Failed to store repository metadata for {owner}/{repo}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error storing repository metadata: {e}")
            return False

    def store_referrers_data(self, owner: str, repo: str, referrers: Dict[str, int], 
                           month: Optional[str] = None) -> bool:
        """Store referrers data for a repository."""
        try:
            if month is None:
                month = format_month_for_data_key(datetime.now(timezone.utc))
            
            # Load existing referrers data
            referrers_data = self.file_manager.load_referrers_data(owner, repo)
            
            # Update referrers for the month
            if month not in referrers_data:
                referrers_data[month] = {}
            
            # Merge with existing data (add counts)
            for referrer, count in referrers.items():
                if referrer in referrers_data[month]:
                    referrers_data[month][referrer] += count
                else:
                    referrers_data[month][referrer] = count
            
            # Save updated data
            success = self.file_manager.save_referrers_data(owner, repo, referrers_data)
            
            if success:
                self.logger.info(f"Stored referrers data for {owner}/{repo} for {month}")
            else:
                self.logger.error(f"Failed to store referrers data for {owner}/{repo}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error storing referrers data: {e}")
            return False
    
    def get_daily_metrics(self, owner: str, repo: str, year: int) -> Dict[str, Any]:
        """Get daily metrics for a repository and year."""
        return self.file_manager.load_daily_data(owner, repo, year)
    
    def get_referrers_data(self, owner: str, repo: str) -> Dict[str, Any]:
        """Get referrers data for a repository."""
        return self.file_manager.load_referrers_data(owner, repo)
    
    def get_date_range_metrics(self, owner: str, repo: str, 
                             start_date: datetime, end_date: datetime) -> List[DailyMetrics]:
        """Get metrics for a specific date range."""
        metrics = []
        
        # Determine which years to check
        start_year = start_date.year
        end_year = end_date.year
        
        for year in range(start_year, end_year + 1):
            daily_data = self.get_daily_metrics(owner, repo, year)
            
            for date_key, data in daily_data.items():
                try:
                    # Parse date key (MM-DD format)
                    month, day = date_key.split('-')
                    date = datetime(year, int(month), int(day), tzinfo=timezone.utc)
                    
                    # Check if date is in range
                    if start_date <= date <= end_date:
                        metrics.append(DailyMetrics(
                            date=date.isoformat(),
                            views=data.get('views', 0),
                            unique_visitors=data.get('unique_visitors', 0),
                            clones=data.get('clones', 0),
                            unique_cloners=data.get('unique_cloners', 0),
                            timestamp=data.get('timestamp', '')
                        ))
                except (ValueError, KeyError):
                    continue
        
        # Sort by date
        metrics.sort(key=lambda x: x.date)
        return metrics    

    def calculate_monthly_aggregates(self, owner: str, repo: str, year: int) -> Dict[str, Dict[str, int]]:
        """Calculate monthly aggregates from daily data."""
        daily_data = self.get_daily_metrics(owner, repo, year)
        monthly_totals = {}
        
        for date_key, data in daily_data.items():
            try:
                month, day = date_key.split('-')
                month_key = f"{year}-{month}"
                
                if month_key not in monthly_totals:
                    monthly_totals[month_key] = {
                        'views': 0,
                        'unique_visitors': 0,
                        'clones': 0,
                        'unique_cloners': 0,
                        'days_count': 0
                    }
                
                monthly_totals[month_key]['views'] += data.get('views', 0)
                monthly_totals[month_key]['unique_visitors'] += data.get('unique_visitors', 0)
                monthly_totals[month_key]['clones'] += data.get('clones', 0)
                monthly_totals[month_key]['unique_cloners'] += data.get('unique_cloners', 0)
                monthly_totals[month_key]['days_count'] += 1
                
            except (ValueError, KeyError):
                continue
        
        return monthly_totals
    
    def get_summary_statistics(self, owner: str, repo: str, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get summary statistics for a repository."""
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        if start_date is None:
            start_date = datetime(end_date.year - 1, end_date.month, end_date.day, tzinfo=timezone.utc)
        
        metrics = self.get_date_range_metrics(owner, repo, start_date, end_date)
        
        if not metrics:
            return {
                'total_views': 0,
                'total_unique_visitors': 0,
                'total_clones': 0,
                'total_unique_cloners': 0,
                'days_with_data': 0,
                'average_daily_views': 0,
                'average_daily_visitors': 0,
                'peak_views_day': None,
                'peak_views_count': 0
            }
        
        total_views = sum(m.views for m in metrics)
        total_visitors = sum(m.unique_visitors for m in metrics)
        total_clones = sum(m.clones for m in metrics)
        total_unique_cloners = sum(m.unique_cloners for m in metrics)
        
        # Find peak day
        peak_day = max(metrics, key=lambda x: x.views)
        
        return {
            'total_views': total_views,
            'total_unique_visitors': total_visitors,
            'total_clones': total_clones,
            'total_unique_cloners': total_unique_cloners,
            'days_with_data': len(metrics),
            'average_daily_views': total_views / len(metrics) if metrics else 0,
            'average_daily_visitors': total_visitors / len(metrics) if metrics else 0,
            'peak_views_day': peak_day.date,
            'peak_views_count': peak_day.views,
            'date_range': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            }
        }
    
    def store_historical_data(self, owner: str, repo: str, 
                            views_daily: List[Dict], clones_daily: List[Dict]) -> bool:
        """Store historical data from GitHub API (initial 14-day data)."""
        try:
            success_count = 0
            
            # Process views data - store all historical data
            for day_data in views_daily:
                timestamp = day_data.get('timestamp', '')
                if timestamp:
                    date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    
                    # Check if data already exists for this date
                    year = date.year
                    date_key = format_date_for_data_key(date)
                    daily_data = self.file_manager.load_daily_data(owner, repo, year)
                    
                    # Only store if data doesn't exist for this date
                    if date_key not in daily_data:
                        success = self.store_daily_metrics(
                            owner, repo, date,
                            views=day_data.get('count', 0),
                            unique_visitors=day_data.get('uniques', 0),
                            clones=0,  # Will be updated from clones data
                            unique_cloners=0
                        )
                        if success:
                            success_count += 1
            
            # Process clones data and update existing entries
            for day_data in clones_daily:
                timestamp = day_data.get('timestamp', '')
                if timestamp:
                    date = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    year = date.year
                    date_key = format_date_for_data_key(date)
                    
                    # Load existing data
                    daily_data = self.file_manager.load_daily_data(owner, repo, year)
                    
                    # Update clones data if entry exists, or create new entry
                    if date_key in daily_data:
                        daily_data[date_key]['clones'] = day_data.get('count', 0)
                        daily_data[date_key]['unique_cloners'] = day_data.get('uniques', 0)
                    else:
                        daily_data[date_key] = {
                            'views': 0,
                            'unique_visitors': 0,
                            'clones': day_data.get('count', 0),
                            'unique_cloners': day_data.get('uniques', 0),
                            'timestamp': get_current_utc_timestamp()
                        }
                        success_count += 1
                    
                    self.file_manager.save_daily_data(owner, repo, year, daily_data)
            
            self.logger.info(f"Stored {success_count} days of historical data for {owner}/{repo}")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Error storing historical data: {e}")
            return False  
  
    def export_to_csv(self, owner: str, repo: str, 
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None) -> str:
        """Export data to CSV format."""
        metrics = self.get_date_range_metrics(owner, repo, start_date or datetime(2020, 1, 1, tzinfo=timezone.utc), 
                                            end_date or datetime.now(timezone.utc))
        
        csv_lines = ['Date,Views,Unique Visitors,Clones,Unique Cloners']
        
        for metric in metrics:
            date_str = datetime.fromisoformat(metric.date).strftime('%Y-%m-%d')
            csv_lines.append(f"{date_str},{metric.views},{metric.unique_visitors},{metric.clones},{metric.unique_cloners}")
        
        return '\n'.join(csv_lines)
    
    def cleanup_old_data(self, owner: str, repo: str, retention_days: int):
        """Clean up old data based on retention policy."""
        if retention_days <= 0:
            return  # Keep all data
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        
        # This would implement cleanup logic
        # For now, we keep all data as specified in requirements
        self.logger.info(f"Cleanup policy: keeping all data (retention_days={retention_days})")


class DataAggregator:
    """Aggregates and processes analytics data for reporting."""
    
    def __init__(self, storage: AnalyticsDataStorage):
        self.storage = storage
        self.logger = logging.getLogger("github_analytics.aggregator")
    
    def generate_dashboard_data(self, owner: str, repo: str, 
                              days: int = 30) -> Dict[str, Any]:
        """Generate data structure for dashboard consumption."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        
        # Get metrics for the period
        metrics = self.storage.get_date_range_metrics(owner, repo, start_date, end_date)
        
        # Get referrers data
        referrers = self.storage.get_referrers_data(owner, repo)
        
        # Get summary statistics
        summary = self.storage.get_summary_statistics(owner, repo, start_date, end_date)
        
        # Format for dashboard
        daily_data = {}
        for metric in metrics:
            date_key = datetime.fromisoformat(metric.date).strftime('%Y-%m-%d')
            daily_data[date_key] = metric.to_dict()
        
        # Get current month referrers
        current_month = format_month_for_data_key(datetime.now(timezone.utc))
        current_referrers = referrers.get(current_month, {})
        
        return {
            'repository': f"{owner}/{repo}",
            'period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat(),
                'days': days
            },
            'summary': summary,
            'daily_data': daily_data,
            'referrers': current_referrers,
            'generated_at': get_current_utc_timestamp()
        }


if __name__ == "__main__":
    # Example usage and testing
    from utils import setup_logging
    from datetime import timedelta
    
    logger = setup_logging("DEBUG")
    
    # Create storage instance
    storage = AnalyticsDataStorage("test_data")
    
    # Test storing data
    test_date = datetime.now(timezone.utc)
    success = storage.store_daily_metrics(
        "test-owner", "test-repo", test_date,
        views=45, unique_visitors=12, clones=8, unique_cloners=3
    )
    logger.info(f"Store test: {success}")
    
    # Test referrers
    test_referrers = {"github.com": 100, "google.com": 50}
    success = storage.store_referrers_data("test-owner", "test-repo", test_referrers)
    logger.info(f"Referrers test: {success}")
    
    # Test aggregation
    aggregator = DataAggregator(storage)
    dashboard_data = aggregator.generate_dashboard_data("test-owner", "test-repo")
    logger.info(f"Dashboard data generated: {len(dashboard_data['daily_data'])} days")
    
    # Test CSV export
    csv_data = storage.export_to_csv("test-owner", "test-repo")
    logger.info(f"CSV export: {len(csv_data.split(chr(10)))} lines")
