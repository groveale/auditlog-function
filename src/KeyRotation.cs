using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groveale;

public class KeyRotation
{
    private readonly ILogger<KeyRotation> _logger;
    private readonly IAzureTableService _azureTableService;
    private readonly IKeyVaultService _keyVaultService;
    private readonly ISettingsService _settingsService;

    public KeyRotation(ILogger<KeyRotation> logger, IAzureTableService azureTableService, IKeyVaultService keyVaultService, ISettingsService settingsService)
    {
        _logger = logger;
        _azureTableService = azureTableService;
        _keyVaultService = keyVaultService;
        _settingsService = settingsService;
    }

    [Function("KeyRotation")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Admin, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");

        // Two modes, prepare and confirm
        var mode = req.Query["mode"].ToString().ToLowerInvariant();
        if (mode == "prepare")
        {
            // Logic for preparing key rotation
            _logger.LogInformation("Preparing key rotation.");

            // Pause the webhook
            await _azureTableService.SetWebhookStateAsync(isPaused: true);

            // We need two one further parameter newKeyVaultEncryptionKeySecretName
            var newKeyVaultEncryptionKeySecretName = req.Query["newKeyVaultEncryptionKeySecretName"].ToString();
            if (string.IsNullOrEmpty(newKeyVaultEncryptionKeySecretName))
            {
                _logger.LogError("New Key Vault encryption key secret name is not provided.");
                return new BadRequestObjectResult("newKeyVaultEncryptionKeySecretName is required. This is the name of the new key in Key Vault that will be used for encryption.");
            }

            // Get the current and new encryption services
            var encryptionService = await DeterministicEncryptionService.CreateAsync(_settingsService, _keyVaultService);
            var newEncryptionService = await DeterministicEncryptionService.CreateAsyncForKeyRotation(_keyVaultService, newKeyVaultEncryptionKeySecretName);

            // perform the key rotation logic
            _logger.LogInformation("Performing key rotation logic.");
            await _azureTableService.RotateKeyRecentDailyTableValues(_azureTableService.GetCopilotAgentInteractionTableClient(), encryptionService, newEncryptionService);
            await _azureTableService.RotateKeyRecentDailyTableValues(_azureTableService.GetCopilotInteractionDailyAggregationsTableClient(), encryptionService, newEncryptionService);

            // Todo once added, the weekly, monthly and alltime aggegations should also be rotated

            return new OkObjectResult("Key rotation prepared.");
        }
        else if (mode == "confirm")
        {
            // Logic for confirming key rotation
            _logger.LogInformation("Confirming key rotation.");

            // Resume the webhook
            await _azureTableService.SetWebhookStateAsync(isPaused: false);

            return new OkObjectResult("Key rotation confirmed.");
        }

        return new BadRequestObjectResult("Invalid mode specified. Use 'prepare' or 'confirm'.");
    }
}