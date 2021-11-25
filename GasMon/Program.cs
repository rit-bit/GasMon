#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

namespace GasMon
{
    class Program
    {
        private static readonly NLog.Logger Log = NLog.LogManager.GetCurrentClassLogger();

        private const string BucketName = "eventprocessing-swapprentices20-locationss3bucket-qu0txg2hhzj2";

        private const string Arn =
            "arn:aws:sns:eu-west-1:552908040772:EventProcessing-SWApprentices2021-snsTopicSensorDataPart1-DF8ZTFFN636Z";

        private const string QueueName = "Queue";
        private const int MaxMessages = 1;
        private const int WaitTime = 2;

        private static IAmazonS3 _s3Client = new AmazonS3Client(); // Bucket storage
        private static IAmazonSQS _sqsClient = new AmazonSQSClient(); // Queue

        private static IAmazonSimpleNotificationService
            _snsClient = new AmazonSimpleNotificationServiceClient(); // Notification service

        private static IList<Location> _trustedLocations;
        private static List<GasMonEvent> _trustedEvents = new();

        public static void Main()
        {
            LoggingConfig.Init();
            
            _trustedLocations = GetLocations();
            
            var queueUrl = "";
            try
            {
                queueUrl = CreateQueue();
            }
            catch (AggregateException)
            {
                var queueUrlTask = _sqsClient.GetQueueUrlAsync(QueueName);
                queueUrlTask.Wait();
                queueUrl = queueUrlTask.Result.QueueUrl;
            }

            _snsClient.SubscribeQueueAsync(Arn, _sqsClient, queueUrl);

            do
            {
                var msgTask = GetMessage(_sqsClient, queueUrl, WaitTime);
                msgTask.Wait();
                var msg = msgTask.Result;

                if (msg.Messages.Count != 0)
                {
                    ProcessMessage(msg.Messages[0]);
                    var task = DeleteMessage(_sqsClient, msg.Messages[0], queueUrl);
                    task.Wait();
                }
            } while (!Console.KeyAvailable);

            _sqsClient.DeleteQueueAsync(queueUrl);
            Console.WriteLine($"Queue: \"{QueueName}\" was deleted.");
            Console.WriteLine($"Events list contained {_trustedEvents.Count} messages.");
        }

        private static IList<Location> GetLocations()
        {
            Log.Trace("Preparing to start retrieval from AWS bucket");
            try
            {
                var request = new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = "locations.json"
                };

                Log.Trace("Making request");
                var responseTask = _s3Client.GetObjectAsync(request);
                responseTask.Wait();
                var response = responseTask.Result;

                Log.Trace($"Response: {response}");
                using var reader = new StreamReader(response.ResponseStream);
                var jsonTask = reader.ReadToEndAsync();
                jsonTask.Wait();
                var json = jsonTask.Result;

                Log.Trace("Deserializing JSON");
                var locations = JsonConvert.DeserializeObject<List<Location>>(json);
                Log.Trace($"{locations.Count} locations parsed");
                foreach (var location in locations)
                {
                    Log.Trace($"Location id: {location.id}");
                }

                return locations;
            }
            catch (AmazonS3Exception e)
            {
                Log.Error($"Error encountered on server. Message: '{e.Message}' when writing an object");
                throw;
            }
            catch (Exception e)
            {
                Log.Error($"Unknown encountered on server. Message: '{e.Message}' when writing an object");
                throw;
            }
        }

        private static string CreateQueue()
        {
            var attributes = new Dictionary<string, string>
            {
                {QueueAttributeName.ReceiveMessageWaitTimeSeconds, "5"}
            };
            var responseTask = _sqsClient.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = QueueName,
                Attributes = attributes
            });
            responseTask.Wait();
            var response = responseTask.Result;
            return response.QueueUrl;
        }

        private static async Task<ReceiveMessageResponse> GetMessage(
            IAmazonSQS sqsClient, string qUrl, int waitTime = 0)
        {
            return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = qUrl,
                MaxNumberOfMessages = MaxMessages,
                WaitTimeSeconds = waitTime
                // (Could also request attributes, set visibility timeout, etc.)
            });
        }

        private static void ProcessMessage(Message message)
        {
            Log.Trace($"Deserializing message body from JSON: {message.Body}");
            var body = JsonConvert.DeserializeObject<GasMonBody>(message.Body);
            if (body != null)
            {
                var msg = JsonConvert.DeserializeObject<GasMonMessage>(body.Message);
                if (msg != null && IsTrustedLocation(msg.locationId))
                {
                    var gasMonEvent = new GasMonEvent(body, msg);
                    _trustedEvents.Add(gasMonEvent);
                    Log.Debug($"GasMonEvent recorded:\n{gasMonEvent}");
                }
            }
        }

        private static bool IsTrustedLocation(string locationId)
        {
            return _trustedLocations.Any(trustedLocation => trustedLocation.id.Equals(locationId));
        }


        // Method to delete a message from a queue
        private static async Task DeleteMessage(
            IAmazonSQS sqsClient, Message message, string qUrl)
        {
            Console.WriteLine($"\nDeleting message {message.MessageId} from queue...");
            await sqsClient.DeleteMessageAsync(qUrl, message.ReceiptHandle);
        }
    }
}