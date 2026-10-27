using System.Text.Json;
using System.Text.Json.Serialization;

namespace Contracts;

/// <summary>Source-generated serializer for every contract: no reflection, one shared thread-safe instance.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(InvoiceGenerated))]
[JsonSerializable(typeof(InvoiceFailed))]
public sealed partial class ContractsJsonContext : JsonSerializerContext;
