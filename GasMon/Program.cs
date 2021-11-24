#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
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
        
        private static IAmazonS3 _client;

        public static void Main()
        {
            LoggingConfig.Init();
            
            using (_client = new AmazonS3Client())
            {
                var locations = GetLocations();
                CreateQueue();
            }
        }

        private static async Task<List<Location>?> GetLocations()
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
                var response = await _client.GetObjectAsync(request);
                Log.Debug($"Response: {response}");
                using var reader = new StreamReader(response.ResponseStream);
                var json = await reader.ReadToEndAsync();
                Log.Trace("Deserializing JSON");
                var locations = JsonConvert.DeserializeObject<List<Location>>(json);
                Log.Trace($"{locations.Count} locations parsed");
                foreach (var location in locations)
                {
                    Console.WriteLine($"Location id: {location.id}");
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

        private static void CreateQueue()
        {
            
        }
    }
}