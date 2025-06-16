namespace groveale.Services
{
    public interface ISettingsService
    {
        string TenantId { get; }
        string AuthGuid { get; }
        string StorageAccountUri { get; }
        string StorageAccountName { get; }
        string StorageAccountKey { get; }
        string KeyVaultUrl { get; }
        string KeyVaultEncryptionKeySecretName { get; }
        string SPOSiteId { get; }
        string SPOFieldName { get; }
        string SPOListId { get; }
        
    }

    public class SettingsService : ISettingsService
    {
        public string TenantId => Environment.GetEnvironmentVariable("TenantId");
        public string AuthGuid => Environment.GetEnvironmentVariable("AuthGuid");

        public string StorageAccountUri => Environment.GetEnvironmentVariable("StorageAccountUri");
        public string StorageAccountName => Environment.GetEnvironmentVariable("StorageAccountName");
        public string StorageAccountKey => Environment.GetEnvironmentVariable("StorageAccountKey");
        public string KeyVaultUrl => Environment.GetEnvironmentVariable("KeyVault:Url");
        public string KeyVaultEncryptionKeySecretName => Environment.GetEnvironmentVariable("KeyVault:EncryptionKeySecretName");
        public string SPOSiteId => Environment.GetEnvironmentVariable("SPO:SiteId");
        public string SPOFieldName => Environment.GetEnvironmentVariable("SPO:FieldName");
        public string SPOListId => Environment.GetEnvironmentVariable("SPO:ListId");

    }
}