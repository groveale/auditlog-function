using Azure;
using Azure.Data.Tables;

namespace groveale.Models
{
    public abstract class BaseTableEntity : ITableEntity
    {
        public string PartitionKey { get; set; } = default!;
        public string RowKey { get; set; } = default!;
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}