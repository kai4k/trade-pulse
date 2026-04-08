using TradePulse.Domain.MarketData;

namespace TradePulse.Application.Ports;

public interface ITickIngestionServer
{
    IAsyncEnumerable<Tick> ConsumeAsync(CancellationToken cancellationToken);
    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
