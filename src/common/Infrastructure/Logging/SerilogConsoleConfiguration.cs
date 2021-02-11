using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EventStore.Examples.Infrastructure.Logging
{
    public static class SerilogConsoleConfiguration
    {
        public static void Configure()
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();
        }
    }
}