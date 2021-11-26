#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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

        private const string Part1Arn =
            "arn:aws:sns:eu-west-1:552908040772:EventProcessing-SWApprentices2021-snsTopicSensorDataPart1-DF8ZTFFN636Z";
        private const string Part2Arn =
            "arn:aws:sns:eu-west-1:552908040772:EventProcessing-SWApprentices2021-snsTopicSensorDataPart2-H16WHVKMI6NT";

        private const string Part1Locations = "locations.json";
        private const string Part2Locations = "locations-part2.json";
        private const string QueueName = "Queue1";
        private const int MaxMessages = 1;
        private const int WaitTime = 2;

        private static IAmazonS3 _s3Client = new AmazonS3Client(); // Bucket storage
        private static IAmazonSQS _sqsClient = new AmazonSQSClient(); // Queue

        private static IAmazonSimpleNotificationService
            _snsClient = new AmazonSimpleNotificationServiceClient(); // Notification service

        private static IList<Location> _trustedLocations = new List<Location>();
        private static IList<GasMonEvent> _trustedEvents = new List<GasMonEvent>();
        private static int _duplicatesDetected = 0;
        private static int _errorCount = 0;

        public static void Main()
        {
            LoggingConfig.Init();

            _trustedLocations = GetTrustedLocations(Part2Locations);
            var queueUrl = GetOrCreateQueue();

            _snsClient.SubscribeQueueAsync(Part2Arn, _sqsClient, queueUrl);
            Log.Debug($"Queue subscribed to topic.");

            var loopCount = 0;
            do
            {
                ProcessQueue(queueUrl);
                if (loopCount++ % 5 == 0)
                {
                    PrintQueueSize(queueUrl);
                }
            } while (!Console.KeyAvailable);

            _sqsClient.DeleteQueueAsync(queueUrl);
            Console.WriteLine($"Queue: \"{QueueName}\" was deleted.");

            OutputToFile()
                .Wait();

            Console.WriteLine($"Events list contained {_trustedEvents.Count} messages.");
            Console.WriteLine($"Eliminated {_duplicatesDetected} duplicates.");
            Console.WriteLine($"Encountered {_errorCount} errors.");
        }

        private static IList<Location> GetTrustedLocations(string locationsFilename)
        {
            Log.Trace("Preparing to start retrieval from AWS bucket");
            try
            {
                var request = new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = locationsFilename
                };

                Log.Trace("Making request");
                var responseTask = _s3Client.GetObjectAsync(request);
                var response = responseTask.Result;

                Log.Trace($"Response: {response}");
                using var reader = new StreamReader(response.ResponseStream);
                var jsonTask = reader.ReadToEndAsync();
                var json = jsonTask.Result;

                Log.Trace("Deserializing JSON");
                var locations = JsonConvert.DeserializeObject<List<Location>>(json);

                if (locations == null)
                {
                    Log.Error("No trusted locations could be parsed.");
                }
                else
                {
                    Log.Trace($"{locations.Count} trusted locations parsed");
                    foreach (var location in locations)
                    {
                        Log.Trace($"Trusted location id: {location.id}");
                    }
                }

                return locations ?? new List<Location>();
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

        private static string GetOrCreateQueue()
        {
            try
            {
                return CreateQueue();
            }
            catch (AggregateException)
            {
                var queueUrlTask = _sqsClient.GetQueueUrlAsync(QueueName);
                Log.Debug($"Using queue {QueueName} found in the cloud.");
                return queueUrlTask.Result.QueueUrl;
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
            var response = responseTask.Result;
            Log.Debug($"Queue {QueueName} created.");
            return response.QueueUrl;
        }
        
        private static async Task PrintQueueSize(string queueUrl)
        {
            var attributes = new List<string>{ QueueAttributeName.ApproximateNumberOfMessages };
            GetQueueAttributesResponse responseGetAtt =
                await _sqsClient.GetQueueAttributesAsync(queueUrl, attributes);
            Log.Debug($"Current queue size: {responseGetAtt.ApproximateNumberOfMessages}");
        }

        private static void ProcessQueue(string queueUrl)
        {
            var msgTask = GetMessage(queueUrl, WaitTime);
            var msg = msgTask.Result;

            if (msg.Messages.Count != 0)
            {
                ProcessMessage(msg.Messages[0]);
                DeleteMessageFromQueue(msg.Messages[0], queueUrl)
                    .Wait();
            }
        }

        private static async Task<ReceiveMessageResponse> GetMessage(string qUrl, int waitTime = 0)
        {
            return await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = qUrl,
                MaxNumberOfMessages = MaxMessages,
                WaitTimeSeconds = waitTime
                // (Could also request attributes, set visibility timeout, etc.)
            });
        }

        private static void ProcessMessage(Message message)
        {
            try
            {
                Log.Trace($"Deserializing message body from JSON: {message.Body}");
                var body = JsonConvert.DeserializeObject<GasMonBody>(message.Body);
                if (body != null)
                {
                    var msg = JsonConvert.DeserializeObject<GasMonMessage>(body.Message);
                    if (msg != null && IsTrustedLocation(msg.locationId))
                    {
                        var gasMonEvent = new GasMonEvent(body, msg);
                        if (_trustedEvents.Contains(gasMonEvent))
                        {
                            Log.Debug($"Duplicate message detected:\n{gasMonEvent}");
                            _duplicatesDetected++;
                        }
                        else
                        {
                            _trustedEvents.Add(gasMonEvent);
                            Log.Debug($"GasMonEvent recorded:\n{gasMonEvent}");
                        }
                    }
                }
            }
            catch (JsonReaderException)
            {
                Log.Error($"Unable to deserialize message body: {message.Body}");
                _errorCount++;
            }
        }

        private static bool IsTrustedLocation(string locationId)
        {
            return _trustedLocations.Any(trustedLocation => trustedLocation.id.Equals(locationId));
        }

        private static async Task DeleteMessageFromQueue(Message message, string queueUrl)
        {
            Log.Trace($"Deleting message {message.MessageId} from queue...");
            await _sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle);
        }

        private static List<TimeReadingPair> GetTimesAndAveragesForLocation(string locationId)
        {
            var sortedEvents = _trustedEvents
                .Where(ev => ev.Message.locationId == locationId)
                .GroupBy(ev => ev.Message.DateTimeStamp.ToString("g"));
            var lines = new List<TimeReadingPair>();
            foreach (var groups in sortedEvents.OrderBy(x => x.Key))
            {
                var average = groups.Average(g => g.Message.value);
                var kv = new TimeReadingPair (groups.Key, average);
                lines.Add(kv);
            }

            return lines;
        }

        private static async Task OutputToFile()
        {
            var locationsDictionary = new Dictionary<string, List<TimeReadingPair>>();
            foreach (var trustedLocation in _trustedLocations)
            {
                var locationId = trustedLocation.id;
                locationsDictionary[locationId] = GetTimesAndAveragesForLocation(locationId);
            }

            var timesSet = new List<string>();
            foreach (var locationId in locationsDictionary.Keys)
            {
                var readings = locationsDictionary[locationId];
                foreach (var timeReading in readings)
                {
                    var time = timeReading.Time;
                    if (!timesSet.Contains(time))
                    {
                        timesSet.Add(time);
                    }
                }
            }
            
            timesSet.Sort();
            var header = "Date,"; 
            header += string.Join(',', _trustedLocations.Select(l => l.id));
            var lines = new List<string> {header};
            foreach (var time in timesSet)
            {
                var line = new List<string> {$"{time}"};
                foreach (var trustedLocation in _trustedLocations)
                {
                    var timeAndReading = locationsDictionary[trustedLocation.id].SingleOrDefault(kv => kv.Time == time); // TODO Fix this
                    var reading = timeAndReading == null ? "," : $"{timeAndReading.Reading},";
                    line.Add($"{reading}"); // TODO Check for missing values
                }
                lines.Add(string.Join(',', line));
            }

            await File.WriteAllLinesAsync("MinuteAverages.csv", lines);
        }
    }
}