from decimal import Decimal


def normalize_list_filter(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None

    normalized_values = []
    for value in values:
        for item in value.split(","):
            stripped_item = item.strip()
            if stripped_item:
                normalized_values.append(stripped_item)

    return normalized_values or None


def decimal_ratio(numerator: Decimal, denominator: int) -> float:
    if denominator == 0:
        return 0.0

    return float(numerator / Decimal(denominator))
