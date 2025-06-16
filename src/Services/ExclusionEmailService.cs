using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;

namespace groveale.Services
{
    public interface IExclusionEmailService
    {
        Task<HashSet<string>> LoadEmailsFromPersonFieldAsync(
            TimeSpan? cacheExpiry = null);

        void ClearCache();
        CacheInfo GetCacheInfo();
    }

    public class ExclusionEmailService : IExclusionEmailService
    {
        private static readonly SemaphoreSlim _semaphore = new(1, 1);
        private static CachedEmails _cachedEmails;

        private readonly GraphServiceClient _graphServiceClient;
        private readonly ILogger<ExclusionEmailService> _logger;
        private readonly ISettingsService _settingsService;

        // Cache configuration
        private static readonly TimeSpan DefaultCacheExpiry = TimeSpan.FromMinutes(30);

        public ExclusionEmailService(ILogger<ExclusionEmailService> logger, ISettingsService settingsService)
        {
            _settingsService = settingsService;

            var credential = new DefaultAzureCredential();

            _graphServiceClient = new GraphServiceClient(
                credential,
                new[] { "https://graph.microsoft.com/.default" });
                
            _logger = logger;
        }

        public async Task<HashSet<string>> LoadEmailsFromPersonFieldAsync(
            TimeSpan? cacheExpiry = null)
        {
            var expiry = cacheExpiry ?? DefaultCacheExpiry;

            // Check cache first
            if (_cachedEmails != null && !_cachedEmails.IsExpired)
            {
                _logger?.LogInformation($"Cache hit. Returning {_cachedEmails.Emails.Count} emails");
                return new HashSet<string>(_cachedEmails.Emails, StringComparer.OrdinalIgnoreCase);
            }

            // Use semaphore to prevent multiple concurrent requests
            await _semaphore.WaitAsync();
            try
            {
                // Double-check pattern - another thread might have populated cache while we waited
                if (_cachedEmails != null && !_cachedEmails.IsExpired)
                {
                    _logger?.LogInformation($"Cache hit after wait. Returning {_cachedEmails.Emails.Count} emails");
                    return new HashSet<string>(_cachedEmails.Emails, StringComparer.OrdinalIgnoreCase);
                }

                _logger?.LogInformation("Cache miss. Loading emails from SharePoint...");

                // Load fresh data
                var emails = await LoadEmailsFromSharePointAsync(_settingsService.SPOSiteId, _settingsService.SPOListId, _settingsService.SPOFieldName);

                // Store in cache
                _cachedEmails = new CachedEmails(emails, DateTime.UtcNow.Add(expiry));

                _logger?.LogInformation($"Cached {emails.Count} emails. Cache expires at {_cachedEmails.ExpiresAt:yyyy-MM-dd HH:mm:ss} UTC");
                return emails;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task<HashSet<string>> LoadEmailsFromSharePointAsync(string siteId, string listId, string personField)
        {
            var emails = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                var pageIterator = PageIterator<ListItem, ListItemCollectionResponse>
                    .CreatePageIterator(
                        _graphServiceClient,
                        await _graphServiceClient.Sites[siteId].Lists[listId].Items.GetAsync(requestConfig =>
                        {
                            requestConfig.QueryParameters.Expand = new string[] { "fields" };
                            requestConfig.QueryParameters.Top = 1000;
                            requestConfig.QueryParameters.Select = new string[] { "id", "fields" };
                        }),
                        ProcessListItem);

                await pageIterator.IterateAsync();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"Failed to load emails from SharePoint list {listId}");
                throw new InvalidOperationException($"Failed to load emails from SharePoint list: {ex.Message}", ex);
            }

            return emails;

            bool ProcessListItem(ListItem item)
            {
                try
                {
                    if (item?.Fields?.AdditionalData?.TryGetValue(personField, out var personValue) == true && personValue != null)
                    {
                        emails.Add(personValue.ToString().ToLowerInvariant());
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, $"Error processing item {item?.Id}");
                }

                return true;
            }
        }

        // Method to manually clear cache (useful for testing or forced refresh)
        public void ClearCache()
        {
            _cachedEmails = null;
        }

        // Get cache information
        public CacheInfo GetCacheInfo()
        {
            if (_cachedEmails == null)
            {
                return new CacheInfo { IsEmpty = true };
            }

            return new CacheInfo
            {
                IsEmpty = false,
                EmailCount = _cachedEmails.Emails.Count,
                CachedAt = _cachedEmails.CachedAt,
                ExpiresAt = _cachedEmails.ExpiresAt,
                IsExpired = _cachedEmails.IsExpired,
                TimeUntilExpiry = _cachedEmails.IsExpired ? TimeSpan.Zero : _cachedEmails.ExpiresAt - DateTime.UtcNow
            };
        }

    }

    // Supporting classes
    public class CachedEmails
    {
        public HashSet<string> Emails { get; }
        public DateTime ExpiresAt { get; }
        public DateTime CachedAt { get; }
        public bool IsExpired => DateTime.UtcNow > ExpiresAt;

        public CachedEmails(HashSet<string> emails, DateTime expiresAt)
        {
            Emails = emails;
            ExpiresAt = expiresAt;
            CachedAt = DateTime.UtcNow;
        }
    }

    public class CacheInfo
    {
        public bool IsEmpty { get; set; }
        public int EmailCount { get; set; }
        public DateTime CachedAt { get; set; }
        public DateTime ExpiresAt { get; set; }
        public bool IsExpired { get; set; }
        public TimeSpan TimeUntilExpiry { get; set; }
    }
}