using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Ordering.Api.IntegrationTests;

public sealed class PlaceOrderTests(OrderingApiFixture api) : IClassFixture<OrderingApiFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Order_is_stored_with_status_and_exactly_one_outbox_message()
    {
        var response = await api.PostOrderAsync(NewOrder(), NewKey());

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var accepted = await response.Content.ReadFromJsonAsync<Accepted>(Ct);
        Assert.StartsWith("ORD-", accepted!.OrderReference);

        var details = await api.Client.GetFromJsonAsync<JsonElement>($"/orders/{accepted.OrderId}", Ct);
        Assert.Equal("Received", details.GetProperty("status").GetString());
        Assert.Equal(2, details.GetProperty("items").GetArrayLength());
        Assert.Equal(1, await api.CountOutboxMessagesAsync(accepted.OrderId));
    }

    [Fact]
    public async Task Concurrent_retries_with_the_same_key_create_one_order_and_one_outbox_message()
    {
        var body = NewOrder();
        var key = NewKey();

        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => api.PostOrderAsync(body, key)));

        Assert.Single(responses, r => r.StatusCode == HttpStatusCode.Created);
        Assert.All(responses, r => Assert.True(r.StatusCode is HttpStatusCode.Created or HttpStatusCode.OK));
        var accepted = await Task.WhenAll(responses.Select(r => r.Content.ReadFromJsonAsync<Accepted>(Ct)));
        var orderId = Assert.Single(accepted.Select(a => a!.OrderId).Distinct());
        Assert.Equal(1, await api.CountOutboxMessagesAsync(orderId));
    }

    [Fact]
    public async Task Reusing_a_key_with_a_different_body_is_rejected()
    {
        var key = NewKey();
        var body = NewOrder();
        (await api.PostOrderAsync(body, key)).EnsureSuccessStatusCode();

        var response = await api.PostOrderAsync(body with { Currency = "EUR" }, key);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
    }

    public static TheoryData<string, object?, string?> InvalidRequests => new()
    {
        { "missing Idempotency-Key", NewOrder(), null },
        { "no items", NewOrder() with { Items = [] }, NewKey() },
        { "duplicate product", NewOrder() with { Items = [new("A", "x", 1, 1m), new("A", "y", 1, 2m)] }, NewKey() },
        { "quantity out of range", NewOrder() with { Items = [new("A", "x", 0, 1m)] }, NewKey() },
        { "price with 3 decimals", NewOrder() with { Items = [new("A", "x", 1, 1.234m)] }, NewKey() },
        { "lowercase currency", NewOrder() with { Currency = "try" }, NewKey() },
        { "number sent as string", new { customerId = Guid.NewGuid(), customerEmail = "a@b.co", currency = "TRY", items = new[] { new { productId = "A", productName = "x", quantity = "2", unitPrice = 1m } } }, NewKey() },
    };

    [Theory]
    [MemberData(nameof(InvalidRequests))]
    public async Task Invalid_requests_are_rejected_with_problem_details(string _, object? body, string? key)
    {
        var response = await api.PostOrderAsync(body!, key);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }

    private static string NewKey() => Guid.NewGuid().ToString();

    private static OrderRequest NewOrder() => new(
        Guid.NewGuid(),
        "ali@example.com",
        "TRY",
        [new("SKU-1", "Kablosuz Kulaklık", 2, 1499.90m), new("SKU-2", "USB-C Kablo", 1, 149.50m)]);

    public sealed record OrderRequest(Guid CustomerId, string CustomerEmail, string Currency, OrderItem[] Items);

    public sealed record OrderItem(string ProductId, string ProductName, int Quantity, decimal UnitPrice);

    private sealed record Accepted(Guid OrderId, string OrderReference, string Status);
}
