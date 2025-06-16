using Azure.Data.Tables;
using groveale.Models;

public class CopilotTimeFrameUsage : BaseTableEntity
{
    public const string AllTimePartitionKeyPrefix = "allTime";
    public string? UPN { get; set; }
    // CopilotAllUp / CopilotChat / Teams / Outlook / Word / Excel / PowerPoint / OneNote / Loop
    public AppType? App { get; set; }
    // Copilot all up
    public int TotalDailyActivityCount { get; set; }
    public int? BestDailyStreak { get; set; }
    public int? CurrentDailyStreak { get; set; }
    public int TotalInteractionCount { get; set; }

    public TableEntity ToAllTimeTableEntity()
    {
        PartitionKey = AllTimePartitionKeyPrefix + App.ToString();
        RowKey = UPN;

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(TotalDailyActivityCount), TotalDailyActivityCount },
            { nameof(CurrentDailyStreak), CurrentDailyStreak },
            { nameof(BestDailyStreak), BestDailyStreak },
            { nameof(TotalInteractionCount), TotalInteractionCount }
        };
    }

    public TableEntity ToTimeFrameTableEntity(string stringStartDate)
    {
        PartitionKey = stringStartDate + App.ToString();
        RowKey = UPN;

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(TotalDailyActivityCount), TotalDailyActivityCount },
            { nameof(CurrentDailyStreak), CurrentDailyStreak },
            { nameof(BestDailyStreak), BestDailyStreak },
            { nameof(TotalInteractionCount), TotalInteractionCount }
        };
    }

    public TableEntity ToDailyTableEntity(string stringDate)
    {
        PartitionKey = $"{stringDate}-{App.ToString()}";
        RowKey = UPN;

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(TotalDailyActivityCount), 1 },
            { nameof(TotalInteractionCount), TotalInteractionCount }
        };
    }

    public TableEntity ToDailyAggregationTableEntity(string stringDate)
    {
        PartitionKey = $"{stringDate}-{UPN}";
        RowKey = App.ToString();

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(TotalDailyActivityCount), 1 },
            { nameof(TotalInteractionCount), TotalInteractionCount }
        };
    }

    public string GetAppString()
    {
        return App.ToString();
    }
    
}

public enum AppType
{
    All,
    CopilotChat,
    Teams,
    Outlook,
    Word,
    Excel,
    PowerPoint,
    OneNote,
    Loop,
    MAC,
    Designer,
    SharePoint,
    Planner,
    Whiteboard,
    Stream,
    Forms,
    CopilotAction,
    WebPlugin,
    Agent,
    CopilotStudio
}

