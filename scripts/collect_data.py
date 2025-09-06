#!/usr/bin/env python3
"""
Main data collection script for GitHub Analytics Tracker.
Integrates API client with data storage system and handles the complete workflow.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import ConfigManager, Repository
from github_api import create_api_client, GitHubDataCollector, GitHubAPIError
from data_storage import AnalyticsDataStorage
from utils import setup_logging, get_current_utc_timestamp


class DataCollectionOrchestrator:
    """Orchestrates the complete data collection workflow."""
    
    def __init__(self, github_token: str, config_path: str = "config.json", 
                 data_path: str = "data"):
        self.github_token = github_token
        self.config_manager = ConfigManager(config_path)
        self.storage = AnalyticsDataStorage(data_path)
        self.logger = logging.getLogger("github_analytics.orchestrator")
        
        # Initialize API client
        try:
            self.api_client = create_api_client(github_token)
            self.collector = GitHubDataCollector(self.api_client)
            self.logger.info("GitHub API client initialized successfully")
        except GitHubAPIError as e:
            self.logger.error(f"Failed to initialize GitHub API client: {e}")
            raise
    
    def collect_repository_data(self, repository: Repository, 
                              include_historical: bool = False) -> bool:
        """Collect data for a single repository."""
        owner = repository.owner
        repo_name = repository.name
        
        self.logger.info(f"Starting data collection for {owner}/{repo_name}")
        
        try:
            # Collect current traffic data
            traffic_data = self.collector.collect_current_traffic_data(owner, repo_name)
            
            # Store current day's data
            current_date = datetime.now(timezone.utc)
            success = self.storage.store_daily_metrics(
                owner, repo_name, current_date,
                traffic_data.views, traffic_data.unique_visitors,
                traffic_data.clones, traffic_data.unique_cloners
            )
            
            if not success:
                self.logger.error(f"Failed to store daily metrics for {owner}/{repo_name}")
                return False
            
            # Collect and store referrers data
            try:
                referrers = self.collector.collect_referrers_data(owner, repo_name)
                if referrers:
                    self.storage.store_referrers_data(owner, repo_name, referrers)
                    self.logger.info(f"Stored {len(referrers)} referrers for {owner}/{repo_name}")
            except GitHubAPIError as e:
                self.logger.warning(f"Failed to collect referrers for {owner}/{repo_name}: {e}")
            
            # Collect historical data if requested (for new repositories)
            if include_historical:
                try:
                    views_daily, clones_daily = self.collector.collect_historical_traffic_data(owner, repo_name)
                    if views_daily or clones_daily:
                        self.storage.store_historical_data(owner, repo_name, views_daily, clones_daily)
                        self.logger.info(f"Stored historical data for {owner}/{repo_name}")
                except GitHubAPIError as e:
                    self.logger.warning(f"Failed to collect historical data for {owner}/{repo_name}: {e}")
            
            # Update last_updated timestamp in config
            self.config_manager.update_last_updated(owner, repo_name)
            
            self.logger.info(f"Successfully collected data for {owner}/{repo_name}")
            return True
            
        except GitHubAPIError as e:
            self.logger.error(f"GitHub API error for {owner}/{repo_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error for {owner}/{repo_name}: {e}")
            return False
    
    def collect_all_repositories(self, include_historical: bool = False) -> Dict[str, bool]:
        """Collect data for all enabled repositories."""
        repositories = self.config_manager.get_enabled_repositories()
        
        if not repositories:
            self.logger.warning("No enabled repositories found in configuration")
            return {}
        
        self.logger.info(f"Starting data collection for {len(repositories)} repositories")
        
        results = {}
        for repository in repositories:
            repo_key = f"{repository.owner}/{repository.name}"
            
            # Check if this is a new repository (no last_updated)
            is_new_repo = repository.last_updated is None
            should_include_historical = include_historical or is_new_repo
            
            try:
                success = self.collect_repository_data(repository, should_include_historical)
                results[repo_key] = success
                
                if success:
                    self.logger.info(f"✓ {repo_key}")
                else:
                    self.logger.error(f"✗ {repo_key}")
                    
            except Exception as e:
                self.logger.error(f"✗ {repo_key}: {e}")
                results[repo_key] = False
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        self.logger.info(f"Collection completed: {successful}/{total} repositories successful")
        
        return results
    
    def validate_configuration(self) -> bool:
        """Validate configuration and repository access."""
        try:
            config = self.config_manager.load_config()
            repositories = config.repositories
            
            if not repositories:
                self.logger.error("No repositories configured")
                return False
            
            self.logger.info(f"Validating access to {len(repositories)} repositories...")
            
            valid_count = 0
            for repo in repositories:
                if not repo.enabled:
                    continue
                    
                try:
                    repo_info = self.api_client.get_repository_info(repo.owner, repo.name)
                    self.logger.info(f"✓ {repo.owner}/{repo.name} - {repo_info.get('full_name')}")
                    valid_count += 1
                except GitHubAPIError as e:
                    self.logger.error(f"✗ {repo.owner}/{repo.name} - {e}")
            
            self.logger.info(f"Validation completed: {valid_count} repositories accessible")
            return valid_count > 0
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def generate_collection_report(self, results: Dict[str, bool]) -> Dict:
        """Generate a summary report of the collection run."""
        successful_repos = [repo for repo, success in results.items() if success]
        failed_repos = [repo for repo, success in results.items() if not success]
        
        return {
            'timestamp': get_current_utc_timestamp(),
            'total_repositories': len(results),
            'successful_repositories': len(successful_repos),
            'failed_repositories': len(failed_repos),
            'success_rate': len(successful_repos) / len(results) if results else 0,
            'successful_repos': successful_repos,
            'failed_repos': failed_repos
        }


def main():
    """Main entry point for the data collection script."""
    parser = argparse.ArgumentParser(description='GitHub Analytics Data Collector')
    parser.add_argument('--config', default='config.json', 
                       help='Path to configuration file')
    parser.add_argument('--data-dir', default='data',
                       help='Directory to store data files')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    parser.add_argument('--log-file', 
                       help='Log file path (optional)')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate configuration and repository access')
    parser.add_argument('--include-historical', action='store_true',
                       help='Include historical data collection (14 days)')
    parser.add_argument('--repository', 
                       help='Collect data for specific repository (owner/name)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level, args.log_file)
    
    # Get GitHub token
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        logger.error("GITHUB_TOKEN environment variable not set")
        sys.exit(1)
    
    try:
        # Initialize orchestrator
        orchestrator = DataCollectionOrchestrator(
            github_token=github_token,
            config_path=args.config,
            data_path=args.data_dir
        )
        
        # Validate configuration
        if not orchestrator.validate_configuration():
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        if args.validate_only:
            logger.info("Validation completed successfully")
            sys.exit(0)
        
        # Collect data
        if args.repository:
            # Single repository mode
            owner, repo_name = args.repository.split('/')
            repository = orchestrator.config_manager.get_repository(owner, repo_name)
            
            if not repository:
                logger.error(f"Repository {args.repository} not found in configuration")
                sys.exit(1)
            
            if not repository.enabled:
                logger.error(f"Repository {args.repository} is disabled")
                sys.exit(1)
            
            success = orchestrator.collect_repository_data(repository, args.include_historical)
            sys.exit(0 if success else 1)
        
        else:
            # All repositories mode
            results = orchestrator.collect_all_repositories(args.include_historical)
            
            # Generate report
            report = orchestrator.generate_collection_report(results)
            
            # Log summary
            logger.info(f"Collection Summary:")
            logger.info(f"  Total: {report['total_repositories']}")
            logger.info(f"  Successful: {report['successful_repositories']}")
            logger.info(f"  Failed: {report['failed_repositories']}")
            logger.info(f"  Success Rate: {report['success_rate']:.1%}")
            
            if report['failed_repos']:
                logger.warning(f"Failed repositories: {', '.join(report['failed_repos'])}")
            
            # Exit with error code if any failures
            sys.exit(0 if report['failed_repositories'] == 0 else 1)
    
    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()