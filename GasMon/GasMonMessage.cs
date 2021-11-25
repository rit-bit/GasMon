using System;

namespace GasMon
{
    public class GasMonMessage
    {
        public string locationId { get; set; }
        public string eventId { get; set; }
        public decimal value { get; set; }
        public long timestamp { get; set; }
        public DateTime DateTimeStamp => DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime;

        public override string ToString()
        {
            return $"Loc:{locationId}, event:{eventId} recorded {value} at {DateTimeStamp}";
        }
    }
}