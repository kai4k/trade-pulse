using TradePulse.Domain.Orders;

namespace TradePulse.Application.Ports;

public interface IOrderRouter
{
    Task RouteAsync(Order order, CancellationToken cancellationToken);
}
