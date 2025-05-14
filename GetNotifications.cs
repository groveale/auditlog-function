using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groveale
{
    public class GetNotifications
    {
        private readonly ILogger<GetNotifications> _logger;
        private readonly IM365ActivityService _m365ActivityService;

        private readonly IAzureTableService _azureTableService;
        private readonly IKeyVaultService _keyVaultService;
        private readonly ISettingsService _settingsService;

        public GetNotifications(ILogger<GetNotifications> logger, IM365ActivityService m365ActivityService, IAzureTableService azureTableService, IKeyVaultService keyVaultService, ISettingsService settingsService)
        {
            _logger = logger;
            _m365ActivityService = m365ActivityService;
            _azureTableService = azureTableService;
            _keyVaultService = keyVaultService;
            _settingsService = settingsService;
        }

        [Function("GetNotifications")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // Get content type from query parameters
            string contentType = req.Query["contentType"];
            if (string.IsNullOrEmpty(contentType))
            {
                _logger.LogError("Audit content type is not provided.");
                return new BadRequestObjectResult("Audit content type is not provided.");
            }

            try
            {
                var notifications = await _m365ActivityService.GetAvailableNotificationsAsync(contentType);

                if (notifications == null || notifications.Count == 0)
                {
                    _logger.LogInformation("No notifications found.");
                    return new OkObjectResult("No notifications found.");
                }
                _logger.LogInformation($"Found {notifications.Count} notifications.");

                // Create the EncryptionService
                 _logger.LogInformation("Attempting to create EncryptionService");
                var encryptionService = await DeterministicEncryptionService.CreateAsync(_settingsService, _keyVaultService);
                 _logger.LogInformation("Encryption Service created");

                // log an encryption test
                var test = encryptionService.Encrypt("test");
                _logger.LogInformation("Encryption Test: {Test}", test);

                // Get copilot audit records
                var copilotAuditRecords = await _m365ActivityService.GetCopilotActivityNotificationsAsync(notifications, encryptionService);
                var RAWCopilotInteractions = await _m365ActivityService.GetCopilotActivityNotificationsRAWAsync(notifications);

                // store the new lists in the table
                foreach (var interaction in copilotAuditRecords)
                {
                    await _azureTableService.AddCopilotInteractionDetailsAsync(interaction);
                }

                // store the raw copilot interactions in the table
                foreach (var interaction in RAWCopilotInteractions)
                {
                    await _azureTableService.AddCopilotInteractionRAWAysnc(interaction);
                }

                
                // Group the copilot audit records by user and extract the CopilotEventData
                var groupedCopilotEventData = copilotAuditRecords
                    .GroupBy(record => record.UserId)
                    .ToDictionary(
                        group => group.Key, 
                        group => group.Select(record => record.CopilotEventData).ToList()
                    );

                // log the grouped data
                _logger.LogInformation($"Found data for: {groupedCopilotEventData.Count} users");
                

                // Log or process the grouped data as needed
                foreach (var userId in groupedCopilotEventData.Keys)
                {
                    _logger.LogInformation($"UserId: {userId}");

                    await _azureTableService.AddCopilotInteractionDailyAggregationForUserAsync(groupedCopilotEventData[userId], userId);

                    _logger.LogInformation($"Process events for UserId: {encryptionService.Decrypt(userId)}");
                }

                return new OkObjectResult(groupedCopilotEventData);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting notifications. Message: {Message}", ex.Message);
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}
