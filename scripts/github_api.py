"""
GitHub API client for collecting repository analytics data.
Handles authentication, rate limiting, and data transformation.
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import json
from dataclasses import dataclass
from utils import get_current_utc_timestamp


@dataclass
class TrafficData:
    """Container for repository traffic data."""
    views: int
    unique_visitors: int
    clones: int
    unique_cloners: int
    timestamp: str
    
    @classmethod
    def from_api_response(cls, views_data: Dict, clones_data: Dict) -> 'TrafficData':
        """Create TrafficData from GitHub API responses."""
        return cls(
            views=views_data.get('count', 0),
            unique_visitors=views_data.get('uniques', 0),
            clones=clones_data.get('count', 0),
            unique_cloners=clones_data.get('uniques', 0),
            timestamp=get_current_utc_timestamp()
        )
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'views': self.views,
            'unique_visitors': self.unique_visitors,
            'clones': self.clones,
            'unique_cloners': self.unique_cloners,
            'timestamp': self.timestamp
        }


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class GitHubAPIClient:
    """GitHub API client with authentication and rate limiting."""
    
    def __init__(self, token: str, user_agent: str = "GitHub-Analytics-Tracker/1.0"):
        self.token = token
        self.base_url = "https://api.github.com"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': user_agent
        })
        self.logger = logging.getLogger("github_analytics.api")
        
        # Rate limiting
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = None
        self.last_request_time = 0
        
    def _make_request(self, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> Dict:
        """Make HTTP request with retry logic and rate limiting."""
        
        for attempt in range(max_retries + 1):
            try:
                # Check rate limiting
                self._check_rate_limit()
                
                # Add minimum delay between requests
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < 0.1:  # 100ms minimum between requests
                    time.sleep(0.1 - time_since_last)
                
                self.logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                response = self.session.get(url, params=params, timeout=30)
                self.last_request_time = time.time()
                
                # Update rate limit info
                self._update_rate_limit_info(response)
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 403:
                    # Rate limit exceeded
                    if 'rate limit' in response.text.lower():
                        self.logger.warning("Rate limit exceeded, waiting...")
                        self._handle_rate_limit(response)
                        continue
                    else:
                        raise GitHubAPIError(
                            f"Access forbidden: {response.text}",
                            response.status_code,
                            response.json() if response.content else None
                        )
                
                elif response.status_code == 404:
                    raise GitHubAPIError(
                        f"Repository not found or not accessible",
                        response.status_code
                    )
                
                elif response.status_code >= 500:
                    # Server error, retry
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + (time.time() % 1)  # Exponential backoff with jitter
                        self.logger.warning(f"Server error {response.status_code}, retrying in {wait_time:.1f}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise GitHubAPIError(
                            f"Server error: {response.status_code}",
                            response.status_code
                        )
                
                else:
                    raise GitHubAPIError(
                        f"HTTP {response.status_code}: {response.text}",
                        response.status_code,
                        response.json() if response.content else None
                    )
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) + (time.time() % 1)
                    self.logger.warning(f"Request failed: {e}, retrying in {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue
                else:
                    raise GitHubAPIError(f"Request failed after {max_retries} retries: {e}")
        
        raise GitHubAPIError(f"Request failed after {max_retries} retries")
    
    def _check_rate_limit(self):
        """Check if we're approaching rate limits."""
        if self.rate_limit_remaining is not None and self.rate_limit_remaining < 10:
            if self.rate_limit_reset:
                wait_time = self.rate_limit_reset - time.time()
                if wait_time > 0:
                    self.logger.warning(f"Rate limit low ({self.rate_limit_remaining}), waiting {wait_time:.1f}s")
                    time.sleep(wait_time + 1)
    
    def _update_rate_limit_info(self, response: requests.Response):
        """Update rate limit information from response headers."""
        self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 5000))
        reset_timestamp = response.headers.get('X-RateLimit-Reset')
        if reset_timestamp:
            self.rate_limit_reset = int(reset_timestamp)
        
        self.logger.debug(f"Rate limit remaining: {self.rate_limit_remaining}")
    
    def _handle_rate_limit(self, response: requests.Response):
        """Handle rate limit exceeded response."""
        reset_timestamp = response.headers.get('X-RateLimit-Reset')
        if reset_timestamp:
            wait_time = int(reset_timestamp) - time.time() + 1
            self.logger.warning(f"Rate limit exceeded, waiting {wait_time:.1f}s")
            time.sleep(max(wait_time, 0))
        else:
            # Fallback wait time
            self.logger.warning("Rate limit exceeded, waiting 60s")
            time.sleep(60)
    
    def test_authentication(self) -> bool:
        """Test if the API token is valid."""
        try:
            url = f"{self.base_url}/user"
            response = self._make_request(url)
            self.logger.info(f"Authentication successful for user: {response.get('login', 'unknown')}")
            return True
        except GitHubAPIError as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def get_repository_info(self, owner: str, repo: str) -> Dict:
        """Get basic repository information."""
        url = f"{self.base_url}/repos/{owner}/{repo}"
        return self._make_request(url)
    
    def get_traffic_views(self, owner: str, repo: str) -> Dict:
        """Get repository traffic views data."""
        url = f"{self.base_url}/repos/{owner}/{repo}/traffic/views"
        return self._make_request(url)
    
    def get_traffic_clones(self, owner: str, repo: str) -> Dict:
        """Get repository traffic clones data."""
        url = f"{self.base_url}/repos/{owner}/{repo}/traffic/clones"
        return self._make_request(url)
    
    def get_traffic_referrers(self, owner: str, repo: str) -> List[Dict]:
        """Get repository traffic referrers data."""
        url = f"{self.base_url}/repos/{owner}/{repo}/traffic/popular/referrers"
        return self._make_request(url)
    
    def get_traffic_paths(self, owner: str, repo: str) -> List[Dict]:
        """Get repository traffic popular paths data."""
        url = f"{self.base_url}/repos/{owner}/{repo}/traffic/popular/paths"
        return self._make_request(url)


class GitHubDataCollector:
    """High-level data collector that combines API calls and data transformation."""
    
    def __init__(self, api_client: GitHubAPIClient):
        self.api_client = api_client
        self.logger = logging.getLogger("github_analytics.collector")
    
    def collect_current_traffic_data(self, owner: str, repo: str) -> TrafficData:
        """Collect current traffic data for a repository."""
        self.logger.info(f"Collecting traffic data for {owner}/{repo}")
        
        try:
            # Get views and clones data
            views_data = self.api_client.get_traffic_views(owner, repo)
            clones_data = self.api_client.get_traffic_clones(owner, repo)
            
            # Extract current totals
            views_total = views_data.get('count', 0)
            views_uniques = views_data.get('uniques', 0)
            clones_total = clones_data.get('count', 0)
            clones_uniques = clones_data.get('uniques', 0)
            
            traffic_data = TrafficData(
                views=views_total,
                unique_visitors=views_uniques,
                clones=clones_total,
                unique_cloners=clones_uniques,
                timestamp=get_current_utc_timestamp()
            )
            
            self.logger.info(f"Collected data: {views_total} views, {views_uniques} unique visitors, "
                           f"{clones_total} clones, {clones_uniques} unique cloners")
            
            return traffic_data
            
        except GitHubAPIError as e:
            self.logger.error(f"Failed to collect traffic data for {owner}/{repo}: {e}")
            raise
    
    def collect_historical_traffic_data(self, owner: str, repo: str) -> Tuple[List[Dict], List[Dict]]:
        """Collect historical traffic data (up to 14 days) for a repository."""
        self.logger.info(f"Collecting historical traffic data for {owner}/{repo}")
        
        try:
            views_data = self.api_client.get_traffic_views(owner, repo)
            clones_data = self.api_client.get_traffic_clones(owner, repo)
            
            # Extract daily data
            views_daily = views_data.get('views', [])
            clones_daily = clones_data.get('clones', [])
            
            # Fill in missing days with zeros for complete 14-day dataset
            views_complete = self._fill_missing_days(views_daily)
            clones_complete = self._fill_missing_days(clones_daily)
            
            # Log detailed data from API
            self.logger.info(f"Views API data: {len(views_daily)} active days, {len(views_complete)} total days")
            for day in views_complete:
                timestamp = day.get('timestamp', 'N/A')
                count = day.get('count', 0)
                uniques = day.get('uniques', 0)
                self.logger.info(f"  Views {timestamp}: {count} views, {uniques} unique visitors")
            
            self.logger.info(f"Clones API data: {len(clones_daily)} active days, {len(clones_complete)} total days")
            for day in clones_complete:
                timestamp = day.get('timestamp', 'N/A')
                count = day.get('count', 0)
                uniques = day.get('uniques', 0)
                self.logger.info(f"  Clones {timestamp}: {count} clones, {uniques} unique cloners")
            
            return views_complete, clones_complete
            
        except GitHubAPIError as e:
            self.logger.error(f"Failed to collect historical data for {owner}/{repo}: {e}")
            raise
    
    def _fill_missing_days(self, daily_data: List[Dict]) -> List[Dict]:
        """Fill missing days with zero values for complete 14-day dataset."""
        from datetime import datetime, timedelta, timezone
        
        # Create a set of dates that have data
        existing_dates = set()
        data_by_date = {}
        
        for day in daily_data:
            timestamp = day.get('timestamp', '')
            if timestamp:
                date = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).date()
                existing_dates.add(date)
                data_by_date[date] = day
        
        # Generate complete 14-day range ending today
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=13)  # 14 days total including today
        
        complete_data = []
        current_date = start_date
        
        while current_date <= end_date:
            if current_date in existing_dates:
                # Use existing data
                complete_data.append(data_by_date[current_date])
            else:
                # Add zero entry for missing day
                complete_data.append({
                    'timestamp': current_date.strftime('%Y-%m-%dT00:00:00Z'),
                    'count': 0,
                    'uniques': 0
                })
            current_date += timedelta(days=1)
        
        return complete_data
    
    def collect_repository_metadata(self, owner: str, repo: str) -> Dict[str, int]:
        """Collect repository metadata (stars, forks, etc.)."""
        self.logger.info(f"Collecting repository metadata for {owner}/{repo}")
        
        try:
            repo_info = self.api_client.get_repository_info(owner, repo)
            
            metadata = {
                'stars': repo_info.get('stargazers_count', 0),
                'forks': repo_info.get('forks_count', 0),
                'watchers': repo_info.get('watchers_count', 0),
                'open_issues': repo_info.get('open_issues_count', 0),
                'size': repo_info.get('size', 0)  # Repository size in KB
            }
            
            self.logger.info(f"Repository metadata: {metadata['stars']} stars, {metadata['forks']} forks, "
                           f"{metadata['watchers']} watchers, {metadata['open_issues']} open issues")
            
            return metadata
            
        except GitHubAPIError as e:
            self.logger.error(f"Failed to collect repository metadata for {owner}/{repo}: {e}")
            raise

    def collect_referrers_data(self, owner: str, repo: str) -> Dict[str, int]:
        """Collect referrers data for a repository."""
        self.logger.info(f"Collecting referrers data for {owner}/{repo}")
        
        try:
            referrers_data = self.api_client.get_traffic_referrers(owner, repo)
            
            # Transform to simple dict and log details
            referrers = {}
            self.logger.info(f"Referrers API data: {len(referrers_data)} referrers")
            for referrer in referrers_data:
                referrer_name = referrer.get('referrer', 'unknown')
                count = referrer.get('count', 0)
                uniques = referrer.get('uniques', 0)
                referrers[referrer_name] = count
                self.logger.info(f"  Referrer {referrer_name}: {count} views, {uniques} unique visitors")
            
            self.logger.info(f"Collected {len(referrers)} referrers")
            return referrers
            
        except GitHubAPIError as e:
            self.logger.error(f"Failed to collect referrers data for {owner}/{repo}: {e}")
            raise
    
    def collect_all_repository_data(self, owner: str, repo: str) -> Dict:
        """Collect all available data for a repository."""
        self.logger.info(f"Collecting all data for {owner}/{repo}")
        
        try:
            # Test repository access
            repo_info = self.api_client.get_repository_info(owner, repo)
            
            # Collect traffic data
            traffic_data = self.collect_current_traffic_data(owner, repo)
            
            # Collect historical data
            views_daily, clones_daily = self.collect_historical_traffic_data(owner, repo)
            
            # Collect referrers
            referrers = self.collect_referrers_data(owner, repo)
            
            return {
                'repository': {
                    'owner': owner,
                    'name': repo,
                    'full_name': repo_info.get('full_name'),
                    'private': repo_info.get('private', False),
                    'updated_at': repo_info.get('updated_at')
                },
                'current_traffic': traffic_data.to_dict(),
                'daily_views': views_daily,
                'daily_clones': clones_daily,
                'referrers': referrers,
                'collected_at': get_current_utc_timestamp()
            }
            
        except GitHubAPIError as e:
            self.logger.error(f"Failed to collect all data for {owner}/{repo}: {e}")
            raise


def create_api_client(token: str) -> GitHubAPIClient:
    """Factory function to create and test API client."""
    client = GitHubAPIClient(token)
    
    if not client.test_authentication():
        raise GitHubAPIError("Invalid GitHub token or authentication failed")
    
    return client


if __name__ == "__main__":
    # Example usage and testing
    import os
    from utils import setup_logging
    
    # Setup logging
    logger = setup_logging("DEBUG")
    
    # Get token from environment
    token = os.getenv('GITHUB_TOKEN')
    if not token:
        logger.error("GITHUB_TOKEN environment variable not set")
        exit(1)
    
    try:
        # Create API client
        client = create_api_client(token)
        collector = GitHubDataCollector(client)
        
        # Test with a repository
        owner = "nikhil-sehgal"
        repo = "bedrock"
        
        # Collect all data
        all_data = collector.collect_all_repository_data(owner, repo)
        
        # Print results
        print(f"\nRepository: {all_data['repository']['full_name']}")
        print(f"Current Traffic: {all_data['current_traffic']}")
        print(f"Historical Days: {len(all_data['daily_views'])} views, {len(all_data['daily_clones'])} clones")
        print(f"Referrers: {len(all_data['referrers'])}")
        
        # Save to file for inspection
        with open('sample_data.json', 'w') as f:
            json.dump(all_data, f, indent=2)
        
        logger.info("Data collection test completed successfully")
        
    except GitHubAPIError as e:
        logger.error(f"GitHub API error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
