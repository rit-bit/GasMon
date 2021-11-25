using Amazon.S3.Model;
using NLog;

namespace GasMon
{
    public class LoggingConfig
    {
        public static void Init()
        {
            var config = new NLog.Config.LoggingConfiguration();

            // Targets where to log to: File and Console
            var logfile = new NLog.Targets.FileTarget("logfile") {FileName = "NLog-log.txt"};
            var logconsole = new NLog.Targets.ConsoleTarget
            {
                Name = "logconsole",
                Layout = "${level:uppercase=true}:: ${message}",
            };

            // Rules for mapping loggers to targets            
            config.AddRule(LogLevel.Info, LogLevel.Fatal, logfile);
            config.AddRule(LogLevel.Debug, LogLevel.Fatal, logconsole);

            // Apply config           
            LogManager.Configuration = config;
        }
    }
}