using System;

namespace GasMon
{
    public class GasMonEvent
    {
        public string Type { get; set; }
        public string MessageId { get; set; }
        public string TopicArn { get; set; }
        public GasMonMessage Message { get; set; }
        public DateTime Timestamp { get; set; }
        public string SignatureVersion { get; set; }
        public string Signature { get; set; }
        public string SigningCertURL { get; set; }
        public string UnsubscribeURL { get; set; }

        public GasMonEvent(GasMonBody msg, GasMonMessage body)
        {
            Type = msg.Type;
            MessageId = msg.MessageId;
            TopicArn = msg.TopicArn;
            Message = body;
            Timestamp = msg.Timestamp;
            SignatureVersion = msg.SignatureVersion;
            Signature = msg.Signature;
            SigningCertURL = msg.SigningCertURL;
            UnsubscribeURL = msg.UnsubscribeURL;
        }

        public override string ToString()
        {
            return $"{Type} ID:{MessageId} received {Timestamp}:\n{Message}";
        }
    }
}