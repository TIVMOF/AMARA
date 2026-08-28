"""A brand on the allowlist, and the index used to match vendor strings to it.

A brand is classified on three independent axes rather than one label, because
they genuinely vary independently:

    Yohji Yamamoto   segment=Designer     tier=Luxury      styles=[Avant-Garde]
    Nike             segment=Sportswear   tier=Premium     styles=[Performance]
    Uma Wang         segment=Contemporary tier=Luxury      styles=[Avant-Garde,
                              Designer                              Minimalist]

`styles` is a list because a brand routinely sits in more than one: Song for
the Mute is Avant-Garde and Street at once, and forcing a single label would
throw away half of that.

What a brand *makes* is deliberately not one of them - that a brand sells shoes
is a fact about the product, and belongs in the product's `category`.
"""

from dataclasses import dataclass, field
import re
import unicodedata


# Characters Unicode decomposition does not strip on its own.
_FOLD = str.maketrans({"ø": "o", "Ø": "o", "æ": "ae", "Æ": "ae",
                       "œ": "oe", "Œ": "oe", "ß": "ss", "đ": "d", "ł": "l"})


def fold(value: str) -> str:
    """Reduce a brand string to a comparison key.

    Case, accents and punctuation vary wildly between retailers - the same
    label appears as "ACNE STUDIOS", "Acne Studios", "Alaïa" and "ALAIA" - so
    matching happens on the folded form while the original string is kept
    verbatim on the product record.
    """
    value = value.translate(_FOLD)
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = value.replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "", value.lower())


@dataclass
class Brand:
    """One allowlisted brand and its position on the three axes."""

    name: str
    segment: str
    tier: str
    styles: list[str]
    aliases: list[str] = field(default_factory=list)

    @property
    def keys(self) -> set[str]:
        return {fold(self.name)} | {fold(a) for a in self.aliases}


@dataclass
class BrandIndex:
    """Folded lookup over every allowlisted brand.

    Matching is exact on the folded form. Anything unmatched is counted rather
    than guessed at, so a crawl can report which vendors it dropped - that
    report is how the allowlist grows.
    """

    brands: list[Brand]
    _by_key: dict[str, Brand] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        for brand in self.brands:
            for key in brand.keys:
                self._by_key.setdefault(key, brand)

    def match(self, vendor: str | None) -> Brand | None:
        if not vendor:
            return None
        return self._by_key.get(fold(vendor))

    def __len__(self) -> int:
        return len(self.brands)
