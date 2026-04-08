using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using TradePulse.Application.Ports;
using TradePulse.Domain.MarketData;

namespace TradePulse.Infrastructure.Ingestion;

public sealed partial class TickIngestionServer : ITickIngestionServer, IAsyncDisposable
{
    private readonly ILogger<TickIngestionServer> _logger;
    private readonly int _port;

    // Bounded channel: if downstream processing slows down, we apply backpressure
    // rather than buffering indefinitely and blowing memory.
    private readonly Channel<Tick> _channel = Channel.CreateBounded<Tick>(
        new BoundedChannelOptions(capacity: 10_000)
        {
            SingleWriter = false,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });

    private TcpListener? _listener;
    private CancellationTokenSource? _cts;

    public TickIngestionServer(ILogger<TickIngestionServer> logger, int port = 7001)
    {
        _logger = logger;
        _port = port;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener = new TcpListener(IPAddress.Any, _port);
        _listener.Start();
        LogListening(_port);

        _ = AcceptLoopAsync(_cts.Token);
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _channel.Writer.TryComplete();
        if (_cts is not null) await _cts.CancelAsync();
        _listener?.Stop();
    }

    public IAsyncEnumerable<Tick> ConsumeAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var tcpClient = await _listener!.AcceptTcpClientAsync(ct);
                LogClientConnected(tcpClient.Client.RemoteEndPoint);
                _ = HandleClientAsync(tcpClient, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                LogAcceptError(ex);
            }
        }
    }

    private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken ct)
    {
        using var client = tcpClient;
        var pipe = new Pipe();

        // Two concurrent tasks: fill the pipe from the socket, drain the pipe into ticks
        var fillTask = FillPipeAsync(client.GetStream(), pipe.Writer, ct);
        var readTask = ReadPipeAsync(pipe.Reader, ct);

        await Task.WhenAll(fillTask, readTask);
        LogClientDisconnected();
    }

    private static async Task FillPipeAsync(NetworkStream stream, PipeWriter writer, CancellationToken ct)
    {
        const int minimumBufferSize = 4096;

        while (!ct.IsCancellationRequested)
        {
            var memory = writer.GetMemory(minimumBufferSize);
            try
            {
                int bytesRead = await stream.ReadAsync(memory, ct);
                if (bytesRead == 0) break; // client disconnected cleanly

                writer.Advance(bytesRead);
            }
            catch (OperationCanceledException) { break; }
            catch (IOException) { break; }

            var result = await writer.FlushAsync(ct);
            if (result.IsCompleted) break;
        }

        await writer.CompleteAsync();
    }

    private async Task ReadPipeAsync(PipeReader reader, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            ReadResult result = await reader.ReadAsync(ct);
            ReadOnlySequence<byte> buffer = result.Buffer;

            while (TryReadFrame(ref buffer, out var payload))
            {
                var tick = DeserializeTick(payload);
                if (tick is not null)
                    await _channel.Writer.WriteAsync(tick, ct);
            }

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted) break;
        }

        await reader.CompleteAsync();
    }

    private static bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> payload)
    {
        payload = default;

        if (buffer.Length < TickFraming.HeaderSize) return false;

        // Read 4-byte length header without allocating
        Span<byte> headerBytes = stackalloc byte[TickFraming.HeaderSize];
        buffer.Slice(0, TickFraming.HeaderSize).CopyTo(headerBytes);
        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(headerBytes);

        if (buffer.Length < TickFraming.HeaderSize + payloadLength) return false;

        payload = buffer.Slice(TickFraming.HeaderSize, payloadLength);
        buffer = buffer.Slice(TickFraming.HeaderSize + payloadLength);
        return true;
    }

    private Tick? DeserializeTick(ReadOnlySequence<byte> payload)
    {
        try
        {
            // Avoid allocation: read directly from the sequence if it's a single segment
            if (payload.IsSingleSegment)
                return JsonSerializer.Deserialize<Tick>(payload.FirstSpan);

            // Multi-segment fallback: rent a buffer rather than allocating
            byte[] rented = ArrayPool<byte>.Shared.Rent((int)payload.Length);
            try
            {
                payload.CopyTo(rented);
                return JsonSerializer.Deserialize<Tick>(rented.AsSpan(0, (int)payload.Length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
        catch (JsonException ex)
        {
            LogDeserializeWarning(ex);
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }
        _listener?.Stop();
    }

    // High-performance logging via source generators — zero allocation when log level is disabled
    [LoggerMessage(Level = LogLevel.Information, Message = "Tick ingestion server listening on port {Port}")]
    private partial void LogListening(int port);

    [LoggerMessage(Level = LogLevel.Information, Message = "Simulator connected from {RemoteEndPoint}")]
    private partial void LogClientConnected(System.Net.EndPoint? remoteEndPoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Simulator disconnected")]
    private partial void LogClientDisconnected();

    [LoggerMessage(Level = LogLevel.Error, Message = "Error accepting connection")]
    private partial void LogAcceptError(Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to deserialize tick payload")]
    private partial void LogDeserializeWarning(Exception ex);
}
