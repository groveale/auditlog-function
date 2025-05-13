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

        public GetNotifications(ILogger<GetNotifications> logger, IM365ActivityService m365ActivityService, IAzureTableService azureTableService)
        {
            _logger = logger;
            _m365ActivityService = m365ActivityService;
            _azureTableService = azureTableService;
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

                // Get copilot audit records
                var copilotAuditRecords = await _m365ActivityService.GetCopilotActivityNotificationsAsync(notifications);
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
                

                // Log or process the grouped data as needed
                foreach (var userId in groupedCopilotEventData.Keys)
                {
                    _logger.LogInformation($"UserId: {userId}");

                    await _azureTableService.AddCopilotInteractionDailyAggregationForUserAsync(groupedCopilotEventData[userId], userId);

                    _logger.LogInformation($"Process events for UserId: {userId}");
                }

                return new OkObjectResult(groupedCopilotEventData);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting notifications.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}
