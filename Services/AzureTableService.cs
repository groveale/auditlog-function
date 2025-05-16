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
        Task AddSingleCopilotInteractionDailyAggregationForUserAsync(CopilotEventData entity, string userId);
        Task AddSpecificCopilotInteractionDailyAggregationForUserAsync(AppType appType, string userId, int count);
        Task AddAgentInteractionsDailyAggregationForUserAsync(string agentId, string userId, int count, string agentName);
        Task LogWebhookTriggerAsync(LogEvent webhookEvent);
    }

    public class AzureTableService : IAzureTableService
    {
        private readonly TableServiceClient _serviceClient;
        private readonly string _listCreationTable = "ListCreationEvents";
        private readonly string _copilotInteractionTable = "RAWCopilotInteractions";
        private readonly string _copilotInteractionDetailsTable = "CopilotInteractionDetails";
        private readonly string _copilotInteractionDailyAggregationForUserTable = "CopilotInteractionDailyAggregationByAppAndUser3";
        private readonly string _agentInteractionDailyAggregationForUserTable = "AgentInteractionDailyAggregationByUserAndAgentId";
        private readonly TableClient copilotInteractionDetailsTableClient;
        private readonly TableClient copilotInteractionDailyAggregationsTableClient;
        private readonly TableClient copilotAgentInteractionTableClient;
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

            copilotAgentInteractionTableClient = _serviceClient.GetTableClient(_agentInteractionDailyAggregationForUserTable);
            copilotAgentInteractionTableClient.CreateIfNotExists();
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

        public async Task AddCopilotInteractionDetailsAsync(AuditData copilotInteraction)
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
                { "AISystemPlugin", aiPlugin },
                { "AgentId", copilotInteraction.CopilotEventData?.AgentId },
                { "AgentName", copilotInteraction.CopilotEventData?.AgentName }
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

        /// <summary>
        /// Adds a copilot interaction daily aggregation for a user.
        /// NO longer used
        /// </summary>
        public async Task AddCopilotInteractionDailyAggregationForUserAsync(List<CopilotEventData> entity, string userId)
        {
            try
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

                // All agent interactions
                var agentInteractions = entity.Where(e => e?.AgentId != null).Count();



                var totalInteractions = entity.Count();


                // Ensure the creationTime is specified as UTC
                DateTime eventTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);


                // Attempt to retrieve existing entity

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
                    existingEntity["AgentInteractions"] = (int)existingEntity["AgentInteractions"] + agentInteractions;


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
                        { "CopilotStudioInteractions", copilotStudioInteractions },
                        { "AgentInteractions", agentInteractions }
                    };

                    // Add the new entity
                    await copilotInteractionDailyAggregationsTableClient.AddEntityAsync(tableEntity);
                }
            }
            catch (RequestFailedException ex)
            {
                // Handle the exception as needed
                _logger.LogError(ex, "Error retrieving copilot interaction daily aggregation event from table storage.");
                _logger.LogError(ex, "Message: {Message}", ex.Message);
                _logger.LogError(ex, "Status: {Status}", ex.StackTrace);

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

        public async Task AddSingleCopilotInteractionDailyAggregationForUserAsync(CopilotEventData entity, string userId)
        {

            var copilotUsage = new CopilotTimeFrameUsage
            {
                UPN = userId,
                TotalInteractionCount = 1
            };

            // We need to set the app type based on the entity.AppHost and sometimes other logic
            // For example, if the entity.AppHost is "Teams" and the context type is "TeamsMeeting", we set it to AppType.Teams
            switch (entity.AppHost)
            {
                case "Word":
                    // Handle Word interactions
                    copilotUsage.App = AppType.Word;
                    break;
                case "Excel":
                    // Handle Excel interactions
                    copilotUsage.App = AppType.Excel;
                    break;
                case "PowerPoint":
                    // Handle PowerPoint interactions
                    copilotUsage.App = AppType.PowerPoint;
                    break;
                case "OneNote":
                    // Handle OneNote interactions
                    copilotUsage.App = AppType.OneNote;
                    break;
                case "Outlook":
                    // Handle Outlook interactions
                    copilotUsage.App = AppType.Outlook;
                    break;
                case "Loop":
                    // Handle Loop interactions
                    copilotUsage.App = AppType.Loop;
                    break;
                case "Whiteboard":
                    // Handle Whiteboard interactions
                    copilotUsage.App = AppType.Whiteboard;
                    break;
                case "Teams":
                    // Handle Teams interactions
                    if (entity.Contexts != null && entity.Contexts.Any(c => c.Type.StartsWith("Teams")))
                    {
                        copilotUsage.App = AppType.Teams;
                    }
                    else
                    {
                        copilotUsage.App = AppType.CopilotChat;
                    }
                    break;
                case "Office":
                    // Handle Office interactions
                    if (entity.AgentId != null)
                    {
                        copilotUsage.App = AppType.CopilotChat;
                    }
                    else
                    {
                        copilotUsage.App = AppType.Agent;
                    }
                    break;
                case "Edge":
                    // Handle Edge interactions
                    copilotUsage.App = AppType.CopilotChat;
                    break;
                case "Designer":
                    // Handle Designer interactions
                    copilotUsage.App = AppType.Designer;
                    break;
                case "SharePoint":
                    // Handle SharePoint interactions
                    copilotUsage.App = AppType.SharePoint;
                    break;
                case "M365AdminCenter":
                    // Handle M365AdminCenter interactions
                    copilotUsage.App = AppType.MAC;
                    break;
                case "OAIAutomationAgent":
                    // Handle OAIAutomationAgent interactions
                    copilotUsage.App = AppType.CopilotAction;
                    break;
                case "Copilot Studio":
                    // Handle Copilot Studio interactions
                    copilotUsage.App = AppType.CopilotStudio;
                    break;
                default:
                    // Handle other cases or log an error
                    // We have a new appHost to handle
                    _logger.LogWarning($"Unhandled AppHost: {entity.AppHost}");
                    // TODO write to a table
                    break;
            }

            
            var copilotUsageEntity = copilotUsage.ToDailyTableEntity(entity.EventDateString);
            
            // Add the entity to the table
            await CreateOrUpdateCopilotUsageEntityAsync(copilotUsageEntity, copilotUsage.TotalInteractionCount);

        }

        public async Task AddSpecificCopilotInteractionDailyAggregationForUserAsync(AppType appType, string userId, int count)
        {
            var copilotUsage = new CopilotTimeFrameUsage
            {
                UPN = userId,
                TotalInteractionCount = count,
                App = appType
            };

            var copilotUsageEntity = copilotUsage.ToDailyTableEntity(DateTime.UtcNow.ToString("yyyy-MM-dd"));
            // Add the entity to the table
            await CreateOrUpdateCopilotUsageEntityAsync(copilotUsageEntity, copilotUsage.TotalInteractionCount);
        }

        private async Task CreateOrUpdateCopilotUsageEntityAsync(TableEntity copilotUsageEntity, int interactionCount)
        {

            try
            {
                var retrieveOperation = await copilotInteractionDailyAggregationsTableClient
                    .GetEntityIfExistsAsync<CopilotTimeFrameUsage>(
                        copilotUsageEntity.PartitionKey,
                        copilotUsageEntity.RowKey);

                if (retrieveOperation.HasValue)
                {
                    var existingEntity = retrieveOperation.Value;

                    existingEntity.TotalInteractionCount += interactionCount;

                    await copilotInteractionDailyAggregationsTableClient
                        .UpdateEntityAsync(existingEntity, existingEntity.ETag, TableUpdateMode.Merge);
                }
                else
                {
                    await copilotInteractionDailyAggregationsTableClient
                        .AddEntityAsync(copilotUsageEntity);
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                // Entity was added by someone else just after our check
                // Retry the update path
                var newlyUpdatedEntity = await copilotInteractionDailyAggregationsTableClient
                    .GetEntityAsync<CopilotTimeFrameUsage>(copilotUsageEntity.PartitionKey, copilotUsageEntity.RowKey);

                newlyUpdatedEntity.Value.TotalInteractionCount += interactionCount;

                await copilotInteractionDailyAggregationsTableClient
                    .UpdateEntityAsync(newlyUpdatedEntity.Value, newlyUpdatedEntity.Value.ETag, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError(ex, "Error retrieving or updating copilot interaction aggregation.");
                _logger.LogError("Message: {Message}", ex.Message);
                _logger.LogError("Status: {Status}", ex.Status);
                _logger.LogError("StackTrace: {StackTrace}", ex.StackTrace);
            }
        }

        public async Task AddAgentInteractionsDailyAggregationForUserAsync(string agentId, string userId, int count, string agentName)
        {
            var agentUsage = new AgentInteraction
            {
                UPN = userId,
                TotalInteractionCount = count,
                AgentId = agentId,
                AgentName = agentName
            };

            var agentUsageEntity = agentUsage.ToDailyTableEntity(DateTime.UtcNow.ToString("yyyy-MM-dd"));

            try
            {
                var retrieveOperation = await copilotAgentInteractionTableClient
                    .GetEntityIfExistsAsync<AgentInteraction>(
                        agentUsageEntity.PartitionKey,
                        agentUsageEntity.RowKey);

                if (retrieveOperation.HasValue)
                {
                    var existingEntity = retrieveOperation.Value;

                    existingEntity.TotalInteractionCount += count;

                    await copilotAgentInteractionTableClient
                        .UpdateEntityAsync(existingEntity, existingEntity.ETag, TableUpdateMode.Merge);
                }
                else
                {
                    await copilotAgentInteractionTableClient
                        .AddEntityAsync(agentUsageEntity);
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                // Entity was added by someone else just after our check
                // Retry the update path
                var newlyUpdatedEntity = await copilotAgentInteractionTableClient
                    .GetEntityAsync<CopilotTimeFrameUsage>(agentUsageEntity.PartitionKey, agentUsageEntity.RowKey);

                newlyUpdatedEntity.Value.TotalInteractionCount += count;

                await copilotAgentInteractionTableClient
                    .UpdateEntityAsync(newlyUpdatedEntity.Value, newlyUpdatedEntity.Value.ETag, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError(ex, "Error retrieving or updating agent interaction aggregation.");
                _logger.LogError("Message: {Message}", ex.Message);
                _logger.LogError("Status: {Status}", ex.Status);
                _logger.LogError("StackTrace: {StackTrace}", ex.StackTrace);
            }
        }

    }
}