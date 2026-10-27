using System.Text.Json;
using System.Text.Json.Serialization;
using Ordering.Api.Orders;

namespace Ordering.Api.Infrastructure;

/// <summary>Source-generated (de)serialization for the API's own types: no reflection on the hot path.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, NumberHandling = JsonNumberHandling.Strict, UseStringEnumConverter = true)]
[JsonSerializable(typeof(PlaceOrderRequest))]
[JsonSerializable(typeof(OrderAccepted))]
[JsonSerializable(typeof(OrderDetails))]
internal sealed partial class ApiJsonContext : JsonSerializerContext;
