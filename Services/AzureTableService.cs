using Azure;
using Azure.Data.Tables;
using groveale.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace groveale.Services
{
    public interface IAzureTableService
    {
        Task AddListCreationRecordAsync(ListAuditObj entity);
        Task AddCopilotInteractionRAWAysnc(dynamic entity);
        Task AddCopilotInteractionDetailsAsync(AuditData entity);
        Task AddCopilotInteractionDailyAggregationForUserAsync(List<CopilotEventData> entity, string userId);
        Task LogWebhookTriggerAsync(LogEvent webhookEvent);
    }

    public class AzureTableService : IAzureTableService
    {
        private readonly TableServiceClient _serviceClient;
        private readonly string _listCreationTable = "ListCreationEvents";
        private readonly string _copilotInteractionTable = "RAWCopilotInteractions";
        private readonly string _copilotInteractionDetailsTable = "CopilotInteractionDetails";
        private readonly string _copilotInteractionDailyAggregationForUserTable = "CopilotInteractionDailyAggregationByUser";
        private readonly TableClient copilotInteractionDetailsTableClient;
        private readonly TableClient copilotInteractionDailyAggregationsTableClient;
        private readonly string _webhookEventsTable = "WebhookTriggerEvents";

        private readonly ILogger<AzureTableService> _logger;

        public AzureTableService(ISettingsService settingsService, ILogger<AzureTableService> logger)
        {
            _serviceClient = new TableServiceClient(
                new Uri(settingsService.StorageAccountUri),
                new TableSharedKeyCredential(settingsService.StorageAccountName, settingsService.StorageAccountKey));

            _logger = logger;

            copilotInteractionDetailsTableClient = _serviceClient.GetTableClient(_copilotInteractionDetailsTable);
            copilotInteractionDetailsTableClient.CreateIfNotExists();

            copilotInteractionDailyAggregationsTableClient = _serviceClient.GetTableClient(_copilotInteractionDailyAggregationForUserTable);
            copilotInteractionDailyAggregationsTableClient.CreateIfNotExists();
        }

        public async Task AddCopilotInteractionRAWAysnc(dynamic entity)
        {
            var tableClient = _serviceClient.GetTableClient(_copilotInteractionTable);
            tableClient.CreateIfNotExists();

            // Ensure the creationTime is specified as UTC
            DateTime eventTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
            var tableEntity = new TableEntity(eventTime.ToString("yyyy-MM-dd"), Guid.NewGuid().ToString())
            {
                { "Data", entity.ToString() }
            };

            try
            {
                await tableClient.AddEntityAsync(tableEntity);
                _logger.LogInformation($"Added copilot interaction event at {eventTime}");
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
            {
                // Merge the entity if it already exists
                await tableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error adding copilot interaction event to table storage.");
                throw;
            }
        }

        public async Task AddCopilotInteractionDetailsAsync (AuditData copilotInteraction)
        {
            
            // Get values with null checks
            // string userId = copilotInteraction["UserId"]?.ToString() ?? "unknown";
            // string appHostFromDict = copilotInteraction["CopilotEventData"]?["AppHost"]?.ToString() ?? "unknown";
            // string creationTimeFromDict = copilotInteraction["CreationTime"].ToString() ?? DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

            // // Create entity with validation
            // DateTime eventTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
            // var tableEntity = new TableEntity(eventTime.ToString("yyyy-MM-dd"), Guid.NewGuid().ToString())
            // {
            //     { "User", userId },
            //     { "AppHost", appHostFromDict },
            //     { "CreationTime", DateTime.SpecifyKind(DateTime.Parse(creationTimeFromDict), DateTimeKind.Utc) }
            // };

            // Extract types from the Contexts list and join them as a comma-separated string
            // AppChat will have a context type of rhe name of the app or service within context
            // Example: Some examples of supported apps and services include M365 Office (docx, pptx, xlsx), TeamsMeeting, TeamsChannel, and TeamsChat. If Copilot is used in Excel, then context will be the identifier of the Excel Spreadsheet and the file type.
            // Check if Contexts is non-null before accessing it
            string contextsTypes = copilotInteraction.CopilotEventData.Contexts != null
                ? string.Join(", ", copilotInteraction.CopilotEventData.Contexts.Select(context => context.Type))
                : string.Empty;

            // Check if AISystemPlugin is non-null before accessing it
            string aiPlugin = copilotInteraction.CopilotEventData.AISystemPlugin != null
                ? string.Join(", ", copilotInteraction.CopilotEventData.AISystemPlugin.Select(plugin => plugin.Id))
                : string.Empty;

            DateTime eventTime = DateTime.SpecifyKind(copilotInteraction.CreationTime, DateTimeKind.Utc);
            var tableEntity = new TableEntity(eventTime.ToString("yyyy-MM-dd"), copilotInteraction.Id.ToString())
            {
                { "User", copilotInteraction.UserId },
                { "AppHost", copilotInteraction.CopilotEventData.AppHost },
                { "CreationTime", eventTime },
                { "Contexts", contextsTypes },
                { "AISystemPlugin", aiPlugin }
            };

            
            try
            {
                await copilotInteractionDetailsTableClient.AddEntityAsync(tableEntity);
                _logger.LogInformation($"Added copilot interaction event at {eventTime}");
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
            {
                // Merge the entity if it already exists
                await copilotInteractionDetailsTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error adding copilot interaction event to table storage.");
                throw;
            }
        }

        public async Task AddCopilotInteractionDailyAggregationForUserAsync(List<CopilotEventData> entity, string userId)
        {
            // group the event data via appHost
            var wordInteractions = entity.Where(e => e.AppHost == "Word").Count();
            var excelInteractions = entity.Where(e => e.AppHost == "Excel").Count();
            var powerPointInteractions = entity.Where(e => e.AppHost == "PowerPoint").Count();
            var onenoteInteractions = entity.Where(e => e.AppHost == "OneNote").Count();
            var outlookInteractions = entity.Where(e => e.AppHost == "Outlook").Count(); 
            var loopInteractions = entity.Where(e => e.AppHost == "Loop").Count();
            var teamsInteractions = entity
                .Where(e => e.AppHost == "Teams" && e.Contexts != null && e.Contexts.Any(c => c.Type.StartsWith("Teams")))
                .Count();
            var copilotChat = entity
                .Where(e => e.AppHost == "Office"
                    || e.AppHost == "Edge"
                    || (e.AppHost == "Teams" && e.Contexts != null && e.Contexts.Any(c => string.IsNullOrEmpty(c.Type))))
                .Count();
            var designerInteractions = entity.Where(e => e.AppHost == "Designer").Count();
            var sharePointInteractions = entity.Where(e => e.AppHost == "SharePoint").Count();
            var adminCenterInteractions = entity.Where(e => e.AppHost == "M365AdminCenter").Count();
            var webPluginInteractions = entity
                .Where(e => e.AISystemPlugin != null && e.AISystemPlugin.Any(p => p.Id == "BingWebSearch"))
                .Count();
            var copilotAction = entity.Where(e => e.AppHost == "OAIAutomationAgent").Count();
            var copilotStudioInteractions = entity.Where(e => e.AppHost == "Copilot Studio").Count();

            var totalInteractions = entity.Count();


            // Ensure the creationTime is specified as UTC
            DateTime eventTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
           

            // Attempt to retrieve existing entity
            try 
            {
                var retrieveOperation = await copilotInteractionDailyAggregationsTableClient.GetEntityIfExistsAsync<TableEntity>(eventTime.ToString("yyyy-MM-dd"), userId);
                if (retrieveOperation.HasValue)
                {
                    var existingEntity = retrieveOperation.Value;
                    // Update existing entity
                    existingEntity["TotalCount"] = (int)existingEntity["TotalCount"] + totalInteractions;
                    existingEntity["WordInteractions"] = (int)existingEntity["WordInteractions"] + wordInteractions;
                    existingEntity["ExcelInteractions"] = (int)existingEntity["ExcelInteractions"] + excelInteractions;
                    existingEntity["PowerPointInteractions"] = (int)existingEntity["PowerPointInteractions"] + powerPointInteractions;
                    existingEntity["OneNoteInteractions"] = (int)existingEntity["OneNoteInteractions"] + onenoteInteractions;
                    existingEntity["OutlookInteractions"] = (int)existingEntity["OutlookInteractions"] + outlookInteractions;
                    existingEntity["LoopInteractions"] = (int)existingEntity["LoopInteractions"] + loopInteractions;
                    existingEntity["TeamsInteractions"] = (int)existingEntity["TeamsInteractions"] + teamsInteractions;
                    existingEntity["CopilotChat"] = (int)existingEntity["CopilotChat"] + copilotChat;
                    existingEntity["DesignerInteractions"] = (int)existingEntity["DesignerInteractions"] + designerInteractions;
                    existingEntity["SharePointInteractions"] = (int)existingEntity["SharePointInteractions"] + sharePointInteractions;
                    existingEntity["AdminCenterInteractions"] = (int)existingEntity["AdminCenterInteractions"] + adminCenterInteractions;
                    existingEntity["WebPluginInteractions"] = (int)existingEntity["WebPluginInteractions"] + webPluginInteractions;
                    existingEntity["CopilotAction"] = (int)existingEntity["CopilotAction"] + copilotAction;
                    existingEntity["CopilotStudioInteractions"] = (int)existingEntity["CopilotStudioInteractions"] + copilotStudioInteractions;


                    // Update the entity
                    await copilotInteractionDailyAggregationsTableClient.UpdateEntityAsync(existingEntity, ETag.All, TableUpdateMode.Merge);
                    _logger.LogInformation($"Updated copilot interaction daily aggregation for user {userId} at {eventTime}");
                }
                else
                {
                    // Entity doesn't exist, create a new one
                    var tableEntity = new TableEntity(eventTime.ToString("yyyy-MM-dd"), userId)
                    {
                        { "TotalCount", entity.Count },
                        { "WordInteractions", wordInteractions },
                        { "ExcelInteractions", excelInteractions },
                        { "PowerPointInteractions", powerPointInteractions },
                        { "OneNoteInteractions", onenoteInteractions },
                        { "OutlookInteractions", outlookInteractions },
                        { "LoopInteractions", loopInteractions },
                        { "TeamsInteractions", teamsInteractions },
                        { "CopilotChat", copilotChat },
                        { "DesignerInteractions", designerInteractions },
                        { "SharePointInteractions", sharePointInteractions },
                        { "AdminCenterInteractions", adminCenterInteractions },
                        { "WebPluginInteractions", webPluginInteractions },
                        { "CopilotAction", copilotAction },
                        { "CopilotStudioInteractions", copilotStudioInteractions }
                    };

                    // Add the new entity
                    await copilotInteractionDailyAggregationsTableClient.AddEntityAsync(tableEntity);
                }
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error retrieving copilot interaction daily aggregation event from table storage.");
                throw;
            }
        }

        public async Task AddListCreationRecordAsync(ListAuditObj listCreationEvent)
        {
            var tableClient = _serviceClient.GetTableClient(_listCreationTable);
            tableClient.CreateIfNotExists();

            // Ensure the creationTime is specified as UTC
            DateTime creationTime = DateTime.SpecifyKind(listCreationEvent.CreationTime, DateTimeKind.Utc);

            // Extract site URL from ObjectId
            var objectIdParts = listCreationEvent.ObjectId.Split('/');
            //var siteUrl = $"{objectIdParts[0]}//{objectIdParts[2]}/{objectIdParts[3]}/{objectIdParts[4]}";
            var siteUrl = $"{objectIdParts[4]}";

            var tableEntity = new TableEntity(siteUrl, listCreationEvent.AuditLogId)
            {
                { "ListUrl", listCreationEvent.ListUrl },
                { "ListName", listCreationEvent.ListName },
                { "ListBaseTemplateType", listCreationEvent.ListBaseTemplateType },
                { "ListBaseType", listCreationEvent.ListBaseType },
                { "CreationTime", creationTime }
            };

            try
            {
                await tableClient.AddEntityAsync(tableEntity);
                _logger.LogInformation($"Added list creation event for {listCreationEvent.ListName} at {listCreationEvent.ListUrl}");
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
            {
                // Merge the entity if it already exists
                await tableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error adding list creation event to table storage.");
                throw;
            }
        }

        public async Task LogWebhookTriggerAsync(LogEvent webhookEvent)
        {
            var tableClient = _serviceClient.GetTableClient(_webhookEventsTable);
            tableClient.CreateIfNotExists();

            // Ensure the creationTime is specified as UTC
            DateTime eventTime = DateTime.SpecifyKind(webhookEvent.EventTime, DateTimeKind.Utc);

            var tableEntity = new TableEntity(eventTime.ToString("yyyy-MM-dd"), webhookEvent.EventId)
            {
                { "EventId", webhookEvent.EventId},
                { "EventName", webhookEvent.EventName },
                { "EventMessage", webhookEvent.EventMessage },
                { "EventDetails", webhookEvent.EventDetails },
                { "EventCategory", webhookEvent.EventCategory },
                { "EventTime", eventTime }
            };

            try
            {
                await tableClient.AddEntityAsync(tableEntity);
                _logger.LogInformation($"Added webhook trigger event at {eventTime}");
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
            {
                // Merge the entity if it already exists
                await tableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error adding webhook trigger event to table storage.");
                throw;
            }
        }
    }
}