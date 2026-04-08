namespace TradePulse.Domain.Orders;

public sealed record Order(
    string OrderId,
    string Symbol,
    OrderSide Side,
    OrderType Type,
    decimal Quantity,
    decimal? LimitPrice,
    DateTimeOffset CreatedAt)
{
    public OrderStatus Status { get; init; } = OrderStatus.New;
}

public enum OrderSide { Buy, Sell }

public enum OrderType { Market, Limit }

public enum OrderStatus
{
    New,
    PendingNew,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected
}
