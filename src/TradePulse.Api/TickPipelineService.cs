using System.Reactive.Subjects;
using Microsoft.Extensions.Logging;
using TradePulse.Application.Ports;
using TradePulse.Domain.MarketData;
using TradePulse.Infrastructure.Ingestion;

/// <summary>
/// Hosted service that orchestrates the full Phase 1 + Phase 2 pipeline:
///
///   TCP server (Pipelines + Channel)
///     → TickSimulatorClient sends random ticks
///     → Channel drain → Subject&lt;Tick&gt; (bridge: async world → Rx world)
///       ├── throughput logger (every 500 ticks)
///       └── TickStreamProcessor → VWAP windows logged per 10-second window
/// </summary>
internal sealed partial class TickPipelineService : BackgroundService
{
    private readonly ITickIngestionServer _server;
    private readonly TickSimulatorClient _simulator;
    private readonly ITickStreamProcessor _streamProcessor;
    private readonly ILogger<TickPipelineService> _logger;

    private static readonly TimeSpan VwapWindowSize = TimeSpan.FromSeconds(10);

    public TickPipelineService(
        ITickIngestionServer server,
        TickSimulatorClient simulator,
        ITickStreamProcessor streamProcessor,
        ILogger<TickPipelineService> logger)
    {
        _server          = server;
        _simulator       = simulator;
        _streamProcessor = streamProcessor;
        _logger          = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _server.StartAsync(stoppingToken);

        // Give the server a moment to bind before the simulator connects
        await Task.Delay(200, stoppingToken);

        var simulatorTask = _simulator.RunAsync(stoppingToken);
        var pipelineTask  = RunPipelineAsync(stoppingToken);

        await Task.WhenAll(simulatorTask, pipelineTask);
    }

    private async Task RunPipelineAsync(CancellationToken ct)
    {
        // Subject is the bridge from the async channel world into the Rx world.
        // Multiple Rx subscribers can attach to the same subject without touching the channel.
        using var tickSubject = new Subject<Tick>();

        // Subscribe the VWAP processor before we start pushing ticks
        using var vwapSubscription = _streamProcessor
            .ComputeVwap(tickSubject, VwapWindowSize)
            .Subscribe(
                window => LogVwapWindow(
                    window.Symbol, window.Vwap, window.TotalVolume,
                    window.TickCount, window.WindowStart, window.WindowEnd),
                ex => LogStreamError(ex));

        long count = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await foreach (var tick in _server.ConsumeAsync(ct))
        {
            count++;
            tickSubject.OnNext(tick); // fan-out to all Rx subscribers

            if (count % 500 == 0)
                LogThroughput(tick.Symbol, tick.Bid, tick.Ask, count, sw.Elapsed.TotalSeconds);
        }

        tickSubject.OnCompleted();
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _server.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    // ── High-performance logging (zero allocation when log level is disabled) ─

    [LoggerMessage(Level = LogLevel.Information,
        Message = "[{Symbol}] Bid={Bid} Ask={Ask} | {Count} ticks in {ElapsedSeconds:F1}s")]
    private partial void LogThroughput(
        string symbol, decimal bid, decimal ask, long count, double elapsedSeconds);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "VWAP [{Symbol}] = {Vwap:F4} | Volume={TotalVolume:N0} ticks={TickCount} " +
                  "window={WindowStart:HH:mm:ss}–{WindowEnd:HH:mm:ss}")]
    private partial void LogVwapWindow(
        string symbol, decimal vwap, decimal totalVolume,
        int tickCount, DateTimeOffset windowStart, DateTimeOffset windowEnd);

    [LoggerMessage(Level = LogLevel.Error, Message = "Stream processing error")]
    private partial void LogStreamError(Exception ex);
}
