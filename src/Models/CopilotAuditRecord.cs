using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace groveale
{
    public class AuditRecord
    {
        public Guid RecordID { get; set; }
        public DateTime CreationDate { get; set; }
        public int RecordType { get; set; }
        public string Operation { get; set; }
        public string UserID { get; set; }
        public AuditData AuditData { get; set; }
    }

    public class AuditData
    {
        public DateTime CreationTime { get; set; }
        public Guid Id { get; set; }
        public string Operation { get; set; }
        public Guid OrganizationId { get; set; }
        public int RecordType { get; set; }
        public string UserKey { get; set; }
        public int UserType { get; set; }
        public int Version { get; set; }
        public string Workload { get; set; }
        public string ClientIP { get; set; }
        public string ClientRegion { get; set; }
        public string UserId { get; set; }
        public CopilotEventData CopilotEventData { get; set; }
        
    }

    public class CopilotEventData
    {
        public List<AISystemPlugin> AISystemPlugin { get; set; } = new List<AISystemPlugin>();
        public List<AccessedResource> AccessedResources { get; set; } = new List<AccessedResource>();
        public string AppHost { get; set; }
        public List<Context> Contexts { get; set; } = new List<Context>();
        public List<string> MessageIds { get; set; } = new List<string>();
        public List<Message> Messages { get; set; } = new List<Message>();
        public List<ModelTransparencyDetail> ModelTransparencyDetails { get; set; } = new List<ModelTransparencyDetail>();
        public string ThreadId { get; set; }

        // These are not truly on this object
        public string? AgentId { get; set; }
        public string? AgentName { get; set; }
        public string? EventDateString { get; set; }
    }

    public class AccessedResource
    {
        public string Id { get; set; }
        public string Type { get; set; }
    }

    public class Context
    {
        public string Id { get; set; }
        public string Type { get; set; }
    }

    public class Message
    {
        public string Id { get; set; }
        public bool isPrompt { get; set; }
    }

    public class ModelTransparencyDetail
    {
        public string Provider { get; set; }
        public string ModelName { get; set; }
    }
    public class AISystemPlugin
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    // Helper class for deserializing from JSON
    public class AuditRecordParser
    {
        public static AuditRecord ParseFromString(string recordId, string creationDate, string recordType, 
                                                string operation, string userId, string auditDataJson)
        {
            var record = new AuditRecord
            {
                RecordID = Guid.Parse(recordId),
                CreationDate = DateTime.Parse(creationDate),
                RecordType = int.Parse(recordType),
                Operation = operation,
                UserID = userId,
                AuditData = JsonConvert.DeserializeObject<AuditData>(auditDataJson)
            };
            
            return record;
        }
    }
}