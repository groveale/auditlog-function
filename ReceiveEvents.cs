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

        public ReceiveEvents(ILogger<ReceiveEvents> logger, ISettingsService settingsService, IM365ActivityService m365ActivityService, IAzureTableService azureTableService)
        {
            _logger = logger;
            _settingsService = settingsService;
            _m365ActivityService = m365ActivityService;
            _azureTableService = azureTableService;

        }

        [Function("ReceiveEvents")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

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

                // Get copilot audit records
                var copilotAuditRecords = await _m365ActivityService.GetCopilotActivityNotificationsAsync(notifications);

                // store the new lists in the table
                foreach (var interaction in copilotAuditRecords)
                {
                    await _azureTableService.AddCopilotInteractionAysnc(interaction);
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
