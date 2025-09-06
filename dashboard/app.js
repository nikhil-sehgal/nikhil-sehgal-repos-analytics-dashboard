// GitHub Analytics Dashboard JavaScript

class GitHubAnalyticsDashboard {
    constructor() {
        this.charts = {};
        this.currentRepo = null;
        this.dateRange = {
            start: null,
            end: null
        };
        this.data = {
            daily: {},
            referrers: {},
            summary: {}
        };

        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setDefaultDateRange();
        this.showLoading();

        // Initialize data loader for real data
        this.dataLoader = new GitHubDataLoader(
            null, // No token needed for public repo access
            'nikhil-sehgal',
            'nikhil-sehgal-repos-data-collector'
        );

        // Load real data
        this.loadRealData();
    }

    setupEventListeners() {
        // Repository selector
        const repoSelector = document.getElementById('repo-selector');
        repoSelector.addEventListener('change', (e) => {
            this.currentRepo = e.target.value;
            if (this.currentRepo) {
                this.loadRepositoryData();
            }
        });

        // Date range inputs
        const startDate = document.getElementById('start-date');
        const endDate = document.getElementById('end-date');

        startDate.addEventListener('change', (e) => {
            this.dateRange.start = e.target.value;
            this.updateCharts();
        });

        endDate.addEventListener('change', (e) => {
            this.dateRange.end = e.target.value;
            this.updateCharts();
        });

        // Period buttons
        document.querySelectorAll('.period-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');

                const period = parseInt(e.target.dataset.period);
                this.setDateRangeFromPeriod(period);
                this.updateCharts();
            });
        });

        // Export button
        document.querySelector('.export-btn').addEventListener('click', () => {
            this.exportToCSV();
        });
    }

    setDefaultDateRange() {
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 30);

        document.getElementById('start-date').value = startDate.toISOString().split('T')[0];
        document.getElementById('end-date').value = endDate.toISOString().split('T')[0];

        this.dateRange.start = startDate.toISOString().split('T')[0];
        this.dateRange.end = endDate.toISOString().split('T')[0];
    }

    setDateRangeFromPeriod(days) {
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - days);

        document.getElementById('start-date').value = startDate.toISOString().split('T')[0];
        document.getElementById('end-date').value = endDate.toISOString().split('T')[0];

        this.dateRange.start = startDate.toISOString().split('T')[0];
        this.dateRange.end = endDate.toISOString().split('T')[0];
    }

    showLoading() {
        document.getElementById('loading').style.display = 'flex';
        document.getElementById('dashboard').style.display = 'none';
        document.getElementById('error').style.display = 'none';
    }

    showError(message) {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('dashboard').style.display = 'none';
        document.getElementById('error').style.display = 'block';
        document.getElementById('error-message').textContent = message;
    }

    showDashboard() {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('dashboard').style.display = 'block';
        document.getElementById('error').style.display = 'none';
    }

    loadSampleData() {
        // Generate sample data for demonstration
        this.data.daily = this.generateSampleDailyData();
        this.data.referrers = this.generateSampleReferrersData();
        this.data.summary = this.calculateSummaryStats();
    }

    generateSampleDailyData() {
        const data = {};
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 90);

        for (let i = 0; i < 90; i++) {
            const date = new Date(startDate);
            date.setDate(date.getDate() + i);
            const dateKey = date.toISOString().split('T')[0];

            // Generate realistic sample data with some trends
            const baseViews = 30 + Math.sin(i / 7) * 10; // Weekly pattern
            const baseClones = 5 + Math.sin(i / 14) * 3; // Bi-weekly pattern

            data[dateKey] = {
                views: Math.max(0, Math.floor(baseViews + Math.random() * 20)),
                unique_visitors: Math.max(0, Math.floor(baseViews * 0.3 + Math.random() * 8)),
                clones: Math.max(0, Math.floor(baseClones + Math.random() * 10)),
                unique_cloners: Math.max(0, Math.floor(baseClones * 0.4 + Math.random() * 3)),
                timestamp: date.toISOString()
            };
        }

        return data;
    }

    generateSampleReferrersData() {
        return {
            'github.com': 1234,
            'google.com': 567,
            'twitter.com': 234,
            'reddit.com': 156,
            'stackoverflow.com': 89,
            'direct': 445
        };
    }

    calculateSummaryStats() {
        const dailyData = Object.values(this.data.daily);

        return {
            totalViews: dailyData.reduce((sum, day) => sum + day.views, 0),
            totalVisitors: dailyData.reduce((sum, day) => sum + day.unique_visitors, 0),
            totalClones: dailyData.reduce((sum, day) => sum + day.clones, 0),
            totalUniqueCloners: dailyData.reduce((sum, day) => sum + day.unique_cloners, 0)
        };
    }

    renderDashboard() {
        this.showDashboard();
        this.updateSummaryCards();
        this.createCharts();
        this.updateReferrersList();
        this.updateDataTable();
        this.updateLastUpdated();
    }

    updateSummaryCards() {
        const summary = this.data.summary;

        document.getElementById('total-views').textContent = summary.totalViews.toLocaleString();
        document.getElementById('total-visitors').textContent = summary.totalVisitors.toLocaleString();
        document.getElementById('total-clones').textContent = summary.totalClones.toLocaleString();
        document.getElementById('total-unique-cloners').textContent = summary.totalUniqueCloners.toLocaleString();
    }

    createCharts() {
        this.createViewsChart();
        this.createClonesChart();
        this.createMonthlyChart();
    }

    createViewsChart() {
        const ctx = document.getElementById('views-chart').getContext('2d');

        const dates = Object.keys(this.data.daily).sort();
        const views = dates.map(date => this.data.daily[date].views);
        const visitors = dates.map(date => this.data.daily[date].unique_visitors);

        if (this.charts.views) {
            this.charts.views.destroy();
        }

        this.charts.views = new Chart(ctx, {
            type: 'line',
            data: {
                labels: dates.map(date => new Date(date).toLocaleDateString()),
                datasets: [
                    {
                        label: 'Views',
                        data: views,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4
                    },
                    {
                        label: 'Unique Visitors',
                        data: visitors,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
    }

    createClonesChart() {
        const ctx = document.getElementById('clones-chart').getContext('2d');

        const dates = Object.keys(this.data.daily).sort();
        const clones = dates.map(date => this.data.daily[date].clones);
        const uniqueCloners = dates.map(date => this.data.daily[date].unique_cloners);

        if (this.charts.clones) {
            this.charts.clones.destroy();
        }

        this.charts.clones = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: dates.map(date => new Date(date).toLocaleDateString()),
                datasets: [
                    {
                        label: 'Total Clones',
                        data: clones,
                        backgroundColor: 'rgba(59, 130, 246, 0.8)',
                        borderColor: '#3b82f6',
                        borderWidth: 1
                    },
                    {
                        label: 'Unique Cloners',
                        data: uniqueCloners,
                        backgroundColor: 'rgba(16, 185, 129, 0.8)',
                        borderColor: '#10b981',
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    }

    createMonthlyChart() {
        const ctx = document.getElementById('monthly-chart').getContext('2d');

        // Group data by month
        const monthlyData = {};
        Object.entries(this.data.daily).forEach(([date, data]) => {
            const month = date.substring(0, 7); // YYYY-MM
            if (!monthlyData[month]) {
                monthlyData[month] = { views: 0, unique_visitors: 0, clones: 0, unique_cloners: 0 };
            }
            monthlyData[month].views += data.views;
            monthlyData[month].unique_visitors += data.unique_visitors;
            monthlyData[month].clones += data.clones;
            monthlyData[month].unique_cloners += data.unique_cloners;
        });

        const months = Object.keys(monthlyData).sort();
        const monthlyViews = months.map(month => monthlyData[month].views);

        if (this.charts.monthly) {
            this.charts.monthly.destroy();
        }

        this.charts.monthly = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: months.map(month => new Date(month + '-01').toLocaleDateString('en-US', { year: 'numeric', month: 'short' })),
                datasets: [{
                    label: 'Monthly Views',
                    data: monthlyViews,
                    backgroundColor: 'rgba(245, 158, 11, 0.8)',
                    borderColor: '#f59e0b',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    }

    updateReferrersList() {
        const referrersList = document.getElementById('referrers-list');
        const referrers = this.data.referrers;
        const maxCount = Math.max(...Object.values(referrers));

        referrersList.innerHTML = '';

        Object.entries(referrers)
            .sort(([, a], [, b]) => b - a)
            .slice(0, 6)
            .forEach(([referrer, count]) => {
                const percentage = (count / maxCount) * 100;
                const item = document.createElement('div');
                item.className = 'referrer-item';

                item.innerHTML = `
                    <div class="referrer-info">
                        <span class="referrer-name">${referrer}</span>
                        <span class="referrer-url">${this.getReferrerDescription(referrer)}</span>
                    </div>
                    <div class="referrer-stats">
                        <span class="referrer-count">${count.toLocaleString()}</span>
                        <div class="referrer-bar">
                            <div class="referrer-fill" style="width: ${percentage}%;"></div>
                        </div>
                    </div>
                `;

                referrersList.appendChild(item);
            });
    }

    getReferrerDescription(referrer) {
        const descriptions = {
            'github.com': 'Direct GitHub traffic',
            'google.com': 'Search engine traffic',
            'twitter.com': 'Social media traffic',
            'reddit.com': 'Community discussions',
            'stackoverflow.com': 'Developer community',
            'direct': 'Direct visits'
        };
        return descriptions[referrer] || 'External referrer';
    }

    updateDataTable() {
        const tbody = document.querySelector('#daily-data-table tbody');
        const dates = Object.keys(this.data.daily).sort().reverse().slice(0, 30);

        tbody.innerHTML = '';

        dates.forEach(date => {
            const data = this.data.daily[date];
            const row = document.createElement('tr');

            row.innerHTML = `
                <td>${new Date(date).toLocaleDateString()}</td>
                <td>${data.views}</td>
                <td>${data.unique_visitors}</td>
                <td>${data.clones}</td>
                <td>${data.unique_cloners}</td>
            `;

            tbody.appendChild(row);
        });
    }

    updateLastUpdated() {
        const now = new Date();
        document.getElementById('last-updated').textContent =
            now.toLocaleDateString() + ' ' + now.toLocaleTimeString() + ' UTC';
    }

    updateCharts() {
        // Filter data based on date range
        // This would be implemented when connecting to real data
        console.log('Updating charts for date range:', this.dateRange);
    }

    async loadRepositoryData() {
        this.showLoading();

        if (this.currentRepo) {
            const [owner, name] = this.currentRepo.split('/');
            await this.loadRealDataForRepo(owner, name);
        }

        this.renderDashboard();
    }

    async loadRealData() {
        // Load data for the default repository (bedrock)
        await this.loadRealDataForRepo('nikhil-sehgal', 'bedrock');
        this.renderDashboard();
    }

    async loadRealDataForRepo(owner, name) {
        try {
            // Load daily metrics data
            const currentYear = new Date().getFullYear();
            const dailyData = await this.dataLoader.loadDailyData(owner, name, currentYear);

            if (dailyData && Object.keys(dailyData).length > 0) {
                this.data.daily = this.convertDailyData(dailyData);
            } else {
                console.log('No real data found, using sample data');
                this.loadSampleData();
                return;
            }

            // Try to load referrers data (optional)
            try {
                const referrersData = await this.dataLoader.loadReferrersData(owner, name);
                if (referrersData) {
                    this.data.referrers = referrersData;
                }
            } catch (error) {
                console.log('No referrers data found, using empty data');
                this.data.referrers = {};
            }

        } catch (error) {
            console.error('Error loading real data:', error);
            console.log('Falling back to sample data');
            this.loadSampleData();
        }
    }

    convertDailyData(dailyData) {
        // Convert the daily metrics format to the format expected by the dashboard
        const converted = {};

        for (const [year, yearData] of Object.entries(dailyData)) {
            for (const [date, dayData] of Object.entries(yearData)) {
                converted[date] = {
                    views: dayData.views || 0,
                    unique_visitors: dayData.unique_visitors || 0,
                    clones: dayData.clones || 0,
                    unique_cloners: dayData.unique_cloners || 0
                };
            }
        }

        return converted;
    }

    exportToCSV() {
        const dates = Object.keys(this.data.daily).sort();
        let csv = 'Date,Views,Unique Visitors,Clones,Unique Cloners\n';

        dates.forEach(date => {
            const data = this.data.daily[date];
            csv += `${date},${data.views},${data.unique_visitors},${data.clones},${data.unique_cloners}\n`;
        });

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `github-analytics-${this.currentRepo || 'data'}-${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    }
}

// Retry function for error state
function retryLoad() {
    window.location.reload();
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new GitHubAnalyticsDashboard();
});

// GitHub API integration functions (to be implemented)
class GitHubDataLoader {
    constructor(token, dataRepoOwner, dataRepoName) {
        this.token = token;
        this.dataRepoOwner = dataRepoOwner;
        this.dataRepoName = dataRepoName;
        this.baseUrl = 'https://raw.githubusercontent.com';
    }

  async loadDailyData(repoOwner, repoName, year) {
    const url = `${this.baseUrl}/${repoName}/main/${repoOwner}/bedrock/daily_metrics.json`;
    const response = await fetch(url);
    if (!response.ok) 
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    return await response.json();
    }


    async loadReferrersData(repoOwner, repoName) {
        try {
            const url = `${this.baseUrl}/${repoName}/main/${repoOwner}/bedrock/referrers.json`;
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return await response.json();
        } catch (error) {
            console.error('Error loading referrers data:', error);
            return {};
        }
    }
}