using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using groveale.Services;

namespace groveale
{
    public interface IKeyVaultService
    {
        Task<string> GetSecretAsync(string secretName);
    }

    public class KeyVaultService : IKeyVaultService
    {
        private readonly ISettingsService _settings;
        private readonly SecretClient _secretClient;

        public KeyVaultService(ISettingsService settings)
        {
            _settings = settings;
            
            var keyVaultUrl = _settings.KeyVaultUrl;

            // Initialize Key Vault client
            
            var credential = new DefaultAzureCredential();
            _secretClient = new SecretClient(new Uri(keyVaultUrl), credential);
            
        }

        public async Task<string> GetSecretAsync(string secretName)
        {
            var secret = await _secretClient.GetSecretAsync(secretName);
            return secret.Value.Value;
        }
    }
}
