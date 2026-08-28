"""One purchasable size of a product."""

from dataclasses import dataclass


@dataclass
class Size:
    """A single size variant.

    Availability and price are per-size: a dress can be sold out in 34 and in
    stock in 38, and some sites price sizes differently. Kept raw - `price` is
    the string the site sent.
    """

    size: str | None
    available: bool | None
    price: str | None
    sku: str | None
