using System;
using System.Security.Cryptography;
using System.Text;

namespace groveale.Services
{
    public interface IDeterministicEncryptionService 
    {
        string Encrypt(string plainText);
        string Decrypt(string base64Encrypted);
    }

    public class DeterministicEncryptionService : IDeterministicEncryptionService
    {
        private readonly byte[] _key;
        private readonly byte[] _iv = Encoding.UTF8.GetBytes("16bytes-fixed-iv"); // 16 bytes (DON'T CHANGE IF DETERMINISTIC)

        private DeterministicEncryptionService(byte[] key)
        {
            _key = key;
        }

        public static async Task<DeterministicEncryptionService> CreateAsync(ISettingsService settings, IKeyVaultService kvService)
        {
            var secret = await kvService.GetSecretAsync(settings.KeyVaultEncryptionKeySecretName);
            var key = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(secret));
            return new DeterministicEncryptionService(key);
        }

        public string Encrypt(string plainText)
        {
            using var aes = Aes.Create();
            aes.Key = _key;
            aes.IV = _iv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;

            using var encryptor = aes.CreateEncryptor();
            byte[] plainBytes = Encoding.UTF8.GetBytes(plainText);
            byte[] encryptedBytes = encryptor.TransformFinalBlock(plainBytes, 0, plainBytes.Length);

            return Convert.ToBase64String(encryptedBytes);
        }

        public string Decrypt(string base64Encrypted)
        {
            using var aes = Aes.Create();
            aes.Key = _key;
            aes.IV = _iv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;

            using var decryptor = aes.CreateDecryptor();
            byte[] encryptedBytes = Convert.FromBase64String(base64Encrypted);
            byte[] decryptedBytes = decryptor.TransformFinalBlock(encryptedBytes, 0, encryptedBytes.Length);

            return Encoding.UTF8.GetString(decryptedBytes);
        }
    }
}