"""Works out which adapter a domain needs, so sites/ can be written from evidence.

Run this against any store before adding it. It answers two questions: is the
platform's JSON open, and if so is this a multi-brand retailer worth crawling
or a single-brand store whose `vendor` field is unreliable.
"""

from __future__ import annotations

from .fetch import Fetcher, FetchError


def probe(domain: str, fetcher: Fetcher | None = None) -> dict:
    """Classify one domain.

    Returns a dict with `adapter` set to the module that can handle it, or
    None when nothing here can. `vendors` is the distinct vendor list from the
    first page - the signal for whether the store is worth adding, since a
    multi-brand retailer carries many allowlisted labels while a single-brand
    store carries one.
    """
    fetcher = fetcher or Fetcher()
    domain = domain.replace("https://", "").replace("http://", "").strip("/")
    last = ""

    for base in (f"https://www.{domain}", f"https://{domain}"):
        try:
            payload = fetcher.get_json(f"{base}/products.json?limit=250", allow_404=True)
        except FetchError as exc:
            last = str(exc)
            continue
        if not isinstance(payload, dict) or "products" not in payload:
            last = f"{base}/products.json did not return a product list"
            continue

        products = payload["products"]
        vendors = sorted({p["vendor"] for p in products if p.get("vendor")})
        meta = fetcher.get_json(f"{base}/meta.json", allow_404=True) or {}
        return {
            "domain": domain,
            "adapter": "shopify",
            "base_url": base,
            "currency": meta.get("currency"),
            "country": meta.get("country"),
            "first_page_products": len(products),
            "vendor_count": len(vendors),
            "vendors": vendors,
            "kind": "multi-brand" if len(vendors) > 3 else "single-brand",
        }

    return {"domain": domain, "adapter": None, "error": last or "no open products.json"}


def suggest_yaml(result: dict) -> str:
    """Render a probe result as a sites/*.yaml starting point."""
    if not result.get("adapter"):
        return f"# {result['domain']}: {result.get('error')}"

    name = result["domain"].split(".")[0].replace("-", "")
    lines = [
        f"name: {name}",
        f"adapter: {result['adapter']}",
        f"base_url: {result['base_url']}",
        f"currency: {result['currency']}" if result.get("currency") else "",
        f"country: {result['country']}" if result.get("country") else "",
        "rate_limit_rps: 0.5",
    ]
    if result["kind"] == "single-brand":
        lines.append(
            "# single-brand store: check the vendor strings above before trusting\n"
            "# them, and set brand_override if they are seasons or fabrics\n"
            f"# brand_override: {result['vendors'][0] if result['vendors'] else '?'}"
        )
    lines.append(f"notes: \"{result['kind']}, {result['vendor_count']} vendors on page 1\"")
    return "\n".join(line for line in lines if line)
