using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TradePulse.Domain.MarketData;

namespace TradePulse.Infrastructure.Ingestion;

/// <summary>
/// Simulates a market data feed by generating random ticks for configured symbols
/// and streaming them to the ingestion server over TCP using length-prefixed framing.
/// </summary>
public sealed partial class TickSimulatorClient : IAsyncDisposable
{
    private static readonly string[] Symbols = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"];

    private static readonly Dictionary<string, decimal> BasePrices = new()
    {
        ["AAPL"]  = 185.00m,
        ["MSFT"]  = 415.00m,
        ["TSLA"]  = 175.00m,
        ["GOOGL"] = 175.00m,
        ["AMZN"]  = 195.00m
    };

    private readonly ILogger<TickSimulatorClient> _logger;
    private readonly string _host;
    private readonly int _port;
    private readonly int _ticksPerSecond;
    private TcpClient? _tcpClient;

    public TickSimulatorClient(
        ILogger<TickSimulatorClient> logger,
        string host = "localhost",
        int port = 7001,
        int ticksPerSecond = 100)
    {
        _logger = logger;
        _host = host;
        _port = port;
        _ticksPerSecond = ticksPerSecond;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(_host, _port, cancellationToken);
        LogConnected(_host, _port, _ticksPerSecond);

        var stream = _tcpClient.GetStream();
        var interval = TimeSpan.FromMilliseconds(1000.0 / _ticksPerSecond);
        var random = new Random();
        var prices = new Dictionary<string, decimal>(BasePrices);

        // Pre-allocate a reusable buffer for the 4-byte length header
        var header = new byte[TickFraming.HeaderSize];

        while (!cancellationToken.IsCancellationRequested)
        {
            var symbol = Symbols[random.Next(Symbols.Length)];
            var tick = GenerateTick(symbol, prices, random);

            await WriteTickAsync(stream, tick, header, cancellationToken);

            await Task.Delay(interval, cancellationToken);
        }
    }

    private static Tick GenerateTick(
        string symbol,
        Dictionary<string, decimal> prices,
        Random random)
    {
        // Random walk: ±0.05% per tick
        var delta = prices[symbol] * (decimal)(random.NextDouble() * 0.001 - 0.0005);
        prices[symbol] = Math.Max(1m, prices[symbol] + delta);

        var mid = prices[symbol];
        var halfSpread = mid * 0.0002m; // 2 bps spread

        return new Tick(
            Symbol: symbol,
            Bid: Math.Round(mid - halfSpread, 2),
            Ask: Math.Round(mid + halfSpread, 2),
            Volume: random.Next(100, 10_000),
            Timestamp: DateTimeOffset.UtcNow);
    }

    private static async Task WriteTickAsync(
        NetworkStream stream,
        Tick tick,
        byte[] header,
        CancellationToken ct)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(tick);

        // Write length-prefixed frame: [4-byte big-endian length][payload]
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
        await stream.WriteAsync(header, ct);
        await stream.WriteAsync(payload, ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_tcpClient is not null)
        {
            _tcpClient.Close();
            await Task.CompletedTask;
        }
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Simulator connected to {Host}:{Port}, emitting {TicksPerSecond} ticks/sec")]
    private partial void LogConnected(string host, int port, int ticksPerSecond);
}
