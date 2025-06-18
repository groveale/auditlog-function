using groveale.Models;
using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace groveale
{
    public class ReceiveEvents
    {
        private readonly ILogger<ReceiveEvents> _logger;
        private readonly ISettingsService _settingsService;
        private readonly IM365ActivityService _m365ActivityService;
        private readonly IAzureTableService _azureTableService; 
        private readonly IKeyVaultService _keyVaultService;
        private readonly IExclusionEmailService _exclusionEmailService;

        public ReceiveEvents(ILogger<ReceiveEvents> logger, ISettingsService settingsService, IM365ActivityService m365ActivityService, IAzureTableService azureTableService, IKeyVaultService keyVaultService, IExclusionEmailService exclusionEmailService)
        {
            _logger = logger;
            _settingsService = settingsService;
            _m365ActivityService = m365ActivityService;
            _azureTableService = azureTableService;
            _keyVaultService = keyVaultService;
            _exclusionEmailService = exclusionEmailService;

        }

        [Function("ReceiveEvents")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // check if the webhook is paused
            bool webhookPaused = await _azureTableService.GetWebhookStateAsync();

            _logger.LogInformation($"Webhook state: {webhookPaused}");

            if (webhookPaused)
            {
                _logger.LogInformation("Webhook is paused, returning 503.");
                // Log the webhook trigger event
                await _azureTableService.LogWebhookTriggerAsync(new LogEvent
                {
                    EventId = Guid.NewGuid().ToString(),
                    EventName = "WebhookTrigger",
                    EventMessage = "Webhook triggered",
                    EventDetails = "Webhook is paused",
                    EventCategory = "Webhook",
                    EventTime = DateTime.UtcNow
                });
                return new StatusCodeResult(StatusCodes.Status503ServiceUnavailable);
            }

            try
            {
                // Log the webhook trigger event
                await _azureTableService.LogWebhookTriggerAsync(new LogEvent
                {
                    EventId = Guid.NewGuid().ToString(),
                    EventName = "WebhookTrigger",
                    EventMessage = "Webhook triggered",
                    EventDetails = "Webhook event",
                    EventCategory = "Webhook",
                    EventTime = DateTime.UtcNow
                });
            }
            catch
            {
                _logger.LogError("Error logging event.");
                // continue
            }

            // validate that the headers contains the correct Webhook-AuthID and matches the configured value
            var authId = req.Headers["Webhook-AuthID"];
            if (string.IsNullOrEmpty(authId) || authId != _settingsService.AuthGuid)
            {
                _logger.LogError("Invalid Webhook-AuthID header.");
                // Log the webhook trigger event
                await _azureTableService.LogWebhookTriggerAsync(new LogEvent
                {
                    EventId = Guid.NewGuid().ToString(),
                    EventName = "WebhookTrigger",
                    EventMessage = "Webhook triggered",
                    EventDetails = "Invalid Webhook-AuthID header",
                    EventCategory = "Webhook",
                    EventTime = DateTime.UtcNow
                });
                return new BadRequestObjectResult("Invalid Webhook-AuthID header.");
            }

            // Parse the request body to extract the validation code from the payload
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            try 
            {
                var payload = JObject.Parse(requestBody);
                var validationCodeFromPayload = payload["validationCode"]?.ToString();

                // If the validation code is present, validate it against the Webhook-ValidationCode header
                // This is M365 initial validation request
                if (!string.IsNullOrEmpty(validationCodeFromPayload))
                {
                    var validationHeaderCode = req.Headers["Webhook-ValidationCode"];
                    if (validationHeaderCode != validationCodeFromPayload)
                    {
                        _logger.LogError("Invalid Webhook-ValidationCode header.");
                        return new BadRequestObjectResult("Invalid Webhook-ValidationCode header.");
                    }
                    return new OkResult();
                }
            }
            catch (JsonReaderException)
            {
                // The payload is not a JSON object, its a list of notifications (Hopefully)
                // Could probably use a better way to determine if the payload is a list of notifications
            }
            
            try
            {
                // Deserialize the request body into a list of NotificationResponse objects
                var notifications = JsonConvert.DeserializeObject<List<NotificationResponse>>(requestBody);

                // process the response
                //var newLists = await _m365ActivityService.GetListCreatedNotificationsAsync(notifications);

                // Create the EncyptionService
                // Encrypt the upn, data added to the tables will be encrypted
                var encryptionService = await DeterministicEncryptionService.CreateAsync(_settingsService, _keyVaultService);

                // Just call it - caching happens automatically
                var exclusionEmails = await _exclusionEmailService.LoadEmailsFromPersonFieldAsync();

                // Get copilot audit records
                var copilotAuditRecords = await _m365ActivityService.GetCopilotActivityNotificationsAsync(notifications, encryptionService, exclusionEmails);

                // TODO remove this for prod
                 
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

                    foreach (var copilotEventData in groupedCopilotEventData[userId])
                    {

                        await _azureTableService.AddSingleCopilotInteractionDailyAggregationForUserAsync(copilotEventData, userId);
                    }

                    // Add web plugin interactions to the table (AISystemPlugin == "BingWebSearch")
                    var webPluginInteractions = groupedCopilotEventData[userId]
                        .Where(data => data.AISystemPlugin.Any(plugin => plugin.Name == "BingWebSearch"))
                        .ToList();
                
                    // If there are no web plugin interactions, skip this step
                    if (webPluginInteractions.Count > 0)
                    {
                        // Add the web plugin interactions to the table
                        await _azureTableService.AddSpecificCopilotInteractionDailyAggregationForUserAsync(AppType.WebPlugin, userId, webPluginInteractions.Count());
                    }
                    
                    // Finally add the total copilot interaction for the user to the table
                    await _azureTableService.AddSpecificCopilotInteractionDailyAggregationForUserAsync(AppType.All, userId, groupedCopilotEventData[userId].Count());

                    // get record for agents
                    var copilotAuditRecordsToAdd = groupedCopilotEventData[userId]
                        .Where(record => !string.IsNullOrEmpty(record.AgentId))
                        .ToList();
                    
                    // group by agentId
                    var groupedCopilotAgentRecords = copilotAuditRecordsToAdd?
                        .GroupBy(record => record.AgentId)
                        .ToDictionary(
                            group => group.Key, 
                            group => group.Select(record => record.AgentName).ToList()
                        );

                    // Go through each group and add to the table
                    foreach (var agentId in groupedCopilotAgentRecords.Keys)
                    {
                        var agentName = groupedCopilotAgentRecords[agentId].FirstOrDefault();
                        var agentInteractions = groupedCopilotAgentRecords[agentId].Count();
                        
                        _logger.LogInformation($"AgentId: {agentId} - AgentName: {agentName} - Interactions: {agentInteractions}");

                        await _azureTableService.AddAgentInteractionsDailyAggregationForUserAsync(agentId, userId, agentInteractions, agentName);
                    }

                }
            

                return new OkResult();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing notifications.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}
