using System.ComponentModel.DataAnnotations;

namespace Ordering.Api.Orders;

/// <summary>
/// Members are nullable so that a missing field is reported as a validation error instead of silently
/// binding to a default (e.g. <see cref="Guid.Empty"/> or 0).
/// </summary>
public sealed record PlaceOrderRequest : IValidatableObject
{
    public const int MaxItems = 100;

    [Required]
    public Guid? CustomerId { get; init; }

    [Required, EmailAddress, StringLength(254)]
    public string? CustomerEmail { get; init; }

    /// <summary>ISO 4217 code; one currency per order.</summary>
    [Required, RegularExpression("^[A-Z]{3}$", ErrorMessage = "Currency must be a 3-letter ISO 4217 code, e.g. TRY.")]
    public string? Currency { get; init; }

    [Required, MinLength(1), MaxLength(MaxItems)]
    public IReadOnlyList<PlaceOrderItem>? Items { get; init; }

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (Items is null)
        {
            yield break;
        }

        // One line per product keeps invoice lines unambiguous.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var productId in Items.Select(i => i?.ProductId).OfType<string>())
        {
            if (!seen.Add(productId))
            {
                yield return new ValidationResult($"Product '{productId}' appears more than once.", [nameof(Items)]);
            }
        }
    }
}

public sealed record PlaceOrderItem : IValidatableObject
{
    public const decimal MaxUnitPrice = 1_000_000m;

    [Required, StringLength(64)]
    public string? ProductId { get; init; }

    [Required, StringLength(200)]
    public string? ProductName { get; init; }

    [Required, Range(1, 1000)]
    public int? Quantity { get; init; }

    [Required]
    public decimal? UnitPrice { get; init; }

    // Checked in code rather than with [Range]: RangeAttribute parses decimal limits with the current culture.
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (UnitPrice is not { } price)
        {
            yield break;
        }

        if (price <= 0 || price > MaxUnitPrice)
        {
            yield return new ValidationResult($"Unit price must be greater than 0 and at most {MaxUnitPrice}.", [nameof(UnitPrice)]);
        }
        else if (decimal.Round(price, 2) != price)
        {
            yield return new ValidationResult("Unit price can have at most 2 decimal places.", [nameof(UnitPrice)]);
        }
    }
}
