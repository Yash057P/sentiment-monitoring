"""Company catalog + tweet attribution for the multi-company platform.

The raw Sentiment140 dataset contains no company information, so we add our own
layer on top of it:

  * ``COMPANIES`` - six demo companies, each with a product/service keyword
    profile, brand colours and a logo used by the web dashboard.
  * ``attribution()`` - attributes any tweet to the best-matching company based
    on keyword hits (tie = "General").
  * ``ENRICHMENT_TWEETS`` - clearly branded positive/negative tweets that the
    producer injects into the live stream, so every company dashboard fills up
    quickly even when the underlying dataset barely mentions the sector.

This module is imported by the producer, the Flask backend and the demo
scheduler, so the company list stays in one place.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Company:
    handle: str                 # login username + stable id (e.g. "technova")
    name: str                   # display name
    sector: str                 # industry/sector
    tagline: str                # short marketing line
    description: str            # shown on the company dashboard
    logo: str                   # emoji logo (works offline everywhere)
    color: str                  # brand accent colour (hex) for the UI
    password: str               # seeded login credential for the company
    keywords: tuple[str, ...] = field(default_factory=tuple)  # attribution keywords


COMPANIES: tuple[Company, ...] = (
    Company(
        handle="technova",
        name="TechNova",
        sector="Consumer Electronics",
        tagline="Smartphones, laptops & everyday tech",
        description="TechNova designs smartphones, laptops and accessories people "
                    "use every day. The dashboard tracks what customers feel about "
                    "our products, updates and support.",
        logo="📱",
        color="#2563EB",
        password="technova123",
        keywords=(
            "phone", "iphone", "android", "samsung", "nokia", "smartphone",
            "laptop", "notebook", "charger", "battery", "screen", "display",
            "wifi", "bluetooth", "speaker", "headphone", "webcam", "pixel",
            "gadget", "app", "software", "update", "download", "ipod", "ipad",
            "mac", "computer", "pc", "tech", "device", "camera",
        ),
    ),
    Company(
        handle="urbaneats",
        name="UrbanEats",
        sector="Food Delivery & Restaurants",
        tagline="Great food, delivered fast",
        description="UrbanEats runs a food-delivery network and quick-service "
                    "restaurants. Here you see live reactions to our food, "
                    "delivery speed and overall taste experience.",
        logo="🍕",
        color="#EA580C",
        password="urbaneats123",
        keywords=(
            "food", "pizza", "burger", "sandwich", "taco", "sushi", "pasta",
            "restaurant", "delivery", "order", "meal", "lunch", "dinner",
            "breakfast", "hungry", "menu", "chef", "eat", "ate", "eating",
            "snack", "fries", "noodle", "kitchen", "stomach", "delicious",
            "taste", "tasty", "cuisine",
        ),
    ),
    Company(
        handle="skyride",
        name="SkyRide",
        sector="Ride-Hailing & Transport",
        tagline="Cabs, rides and city transport",
        description="SkyRide connects riders with drivers across the city. The "
                    "dashboard tracks how riders feel about trips, drivers, "
                    "pricing and punctuality.",
        logo="🚕",
        color="#16A34A",
        password="skyride123",
        keywords=(
            "cab", "taxi", "uber", "ride", "driver", "driving", "drive",
            "commute", "traffic", "fare", "pickup", "pick", "drop", "car",
            "passenger", "meter", "highway", "vehicle", "ridehailing",
            "punctual", "waiting", "journey", "auto", "rickshaw",
        ),
    ),
    Company(
        handle="novapay",
        name="NovaPay",
        sector="Digital Payments & Banking",
        tagline="Payments, wallets & banking made simple",
        description="NovaPay powers digital wallets, card payments and banking. "
                    "We watch feedback on transactions, fees, security and "
                    "customer support.",
        logo="💳",
        color="#7C3AED",
        password="novapay123",
        keywords=(
            "payment", "pay", "paying", "paid", "wallet", "credit", "debit",
            "card", "cash", "transaction", "bank", "banking", "transfer",
            "refund", "money", "bill", "fees", "atm", "interest", "loan",
            "savings", "invest", "paypal", "upi", "account", "withdraw",
        ),
    ),
    Company(
        handle="pacificair",
        name="PacificAir",
        sector="Airlines & Travel",
        tagline="Fly farther, arrive relaxed",
        description="PacificAir is an airline focused on comfortable, on-time "
                    "travel. The dashboard follows what travellers say about "
                    "flights, airports, staff and service.",
        logo="✈️",
        color="#0369A1",
        password="pacificair123",
        keywords=(
            "flight", "airline", "airport", "boarding", "baggage", "luggage",
            "pilot", "plane", "seat", "seats", "landing", "takeoff", "delay",
            "air", "terminal", "travel", "tour", "destination", "aboard",
            "runway", "ticket", "vacation", "holiday", "airfare",
        ),
    ),
    Company(
        handle="zenwear",
        name="ZenWear",
        sector="Fashion & Shopping",
        tagline="Everyday style that feels good",
        description="ZenWear sells fashion and footwear online and in stores. We "
                    "track opinions on fits, quality, style and our shopping "
                    "experience.",
        logo="👗",
        color="#DB2777",
        password="zenwear123",
        keywords=(
            "shoes", "sneaker", "boot", "dress", "jeans", "shirt", "jacket",
            "hoodie", "clothes", "clothing", "fashion", "outfit", "style",
            "wear", "wearing", "shopping", "store", "mall", "size", "fit",
            "fabric", "quality", "return", "fashionable", "stylish", "footwear",
            "wardrobe",
        ),
    ),
)

COMPANIES_BY_HANDLE: dict[str, Company] = {c.handle: c for c in COMPANIES}
GENERAL_LABEL = "General"

_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Threshold: how many keyword hits a tweet needs to be attributed at all.
MIN_KEYWORD_HITS = 1


def tokens(text: str) -> list[str]:
    """Lowercased word tokens from free text (URLs/punctuation already removed)."""
    return _TOKEN_RE.findall((text or "").lower())


def keyword_hits(text: str, company: Company) -> int:
    """Number of distinct product/service keywords a tweet matches."""
    words = tokens(text)
    hits = 0
    for kw in company.keywords:
        if any(w == kw or (len(kw) >= 4 and w.startswith(kw)) for w in words):
            hits += 1
    return hits


def attribution(text: str) -> str | None:
    """Return the best-matching company handle for a tweet, or None (General)."""
    best, best_score = None, 0
    for company in COMPANIES:
        score = keyword_hits(text, company)
        if score > best_score:
            best, best_score = company.handle, score
        elif score == best_score and score > 0:
            best = None  # tie between two companies -> General
    return best if best_score >= MIN_KEYWORD_HITS else None


def company_json(company: Company) -> dict:
    """Branding + identity payload for the admin list / dashboards."""
    data = asdict(company)
    data.pop("keywords", None)      # credentials never leave the backend
    data.pop("password", None)
    return data


# ---------------------------------------------------------------------------
# Enrichment: clearly branded tweets mixed into the live stream so company
# dashboards stay lively even where the raw dataset is sparse.
# ---------------------------------------------------------------------------
def _branded(handle: str, label: int, text: str) -> dict:
    return {"company": handle, "label": label, "text": text}


ENRICHMENT_TWEETS: tuple[dict, ...] = (
    # --- TechNova ---
    _branded("technova", 4, "my new technova phone battery lasts all day it is amazing"),
    _branded("technova", 4, "finally got my technova laptop superfast and slim love it"),
    _branded("technova", 4, "technova app update is smooth and the new camera is incredible"),
    _branded("technova", 4, "technova customer support fixed my screen within a day kudos"),
    _branded("technova", 0, "technova battery died in 2 hours totally unacceptable"),
    _branded("technova", 0, "tired of technova app crashing on every single update"),
    _branded("technova", 0, "technova laptop overheats badly while charging ridiculous"),
    _branded("technova", 0, "my technova phone screen cracked so easy build quality is bad"),
    # --- UrbanEats ---
    _branded("urbaneats", 4, "urbaneats pizza arrived hot in 20 minutes simply delicious"),
    _branded("urbaneats", 4, "the new urbaneats burger is the best lunch I had this week"),
    _branded("urbaneats", 4, "urbaneats delivery man was polite and food was fresh and tasty"),
    _branded("urbaneats", 4, "love urbaneats menu such variety and the pasta is amazing"),
    _branded("urbaneats", 0, "urbaneats order arrived cold and one hour late unacceptable"),
    _branded("urbaneats", 0, "the urbaneats burger was dry and tasteless never again"),
    _branded("urbaneats", 0, "urbaneats forgot my drink and refused to refund complete waste"),
    _branded("urbaneats", 0, "my last urbaneats meal gave me food poisoning so sad"),
    # --- SkyRide ---
    _branded("skyride", 4, "skyride cab came in two minutes and the driver was very polite"),
    _branded("skyride", 4, "skyride fares are fair and the new booking app is so easy"),
    _branded("skyride", 4, "great skyride driver today smooth trip and clean vehicle"),
    _branded("skyride", 4, "skyride dropped me exactly on time for my train excellent"),
    _branded("skyride", 0, "skyride driver took a long route and charged me double greedy"),
    _branded("skyride", 0, "waited twenty minutes for a skyride cab and then it got cancelled"),
    _branded("skyride", 0, "skyride surge pricing tonight is a complete money grab"),
    _branded("skyride", 0, "my skyride car smelt awful and the driver drove dangerously"),
    # --- NovaPay ---
    _branded("novapay", 4, "novapay transfer went through instantly great wallet app"),
    _branded("novapay", 4, "novapay customer care returned my refund the very same day"),
    _branded("novapay", 4, "love novapay cashback offers the app feels very secure"),
    _branded("novapay", 4, "novapay bill pay is so convenient it saves me every month"),
    _branded("novapay", 0, "novapay blocked my account without any reason terrible support"),
    _branded("novapay", 0, "novapay charged me double and took a week to fix it"),
    _branded("novapay", 0, "novapay hidden fees made my transaction expensive unnotified"),
    _branded("novapay", 0, "novapay app keeps logging me out during every payment so painful"),
    # --- PacificAir ---
    _branded("pacificair", 4, "pacificair flight landed exactly on time and the staff were lovely"),
    _branded("pacificair", 4, "best pacificair boarding experience ever so organized"),
    _branded("pacificair", 4, "pacificair seat was super comfy and the inflight meal tasty"),
    _branded("pacificair", 4, "great pacificair pilot smooth landing i felt completely safe"),
    _branded("pacificair", 0, "pacificair delay of five hours with zero explanation disgraceful"),
    _branded("pacificair", 0, "pacificair lost my suitcase and still no compensation after a week"),
    _branded("pacificair", 0, "pacificair cancelled my flight and offered nothing but a refund"),
    _branded("pacificair", 0, "the pacificair seat was broken and legroom too small horrible"),
    # --- ZenWear ---
    _branded("zenwear", 4, "zenwear sneakers fit perfectly and are super comfortable"),
    _branded("zenwear", 4, "got my zenwear order in two days and the fabric quality is great"),
    _branded("zenwear", 4, "the zenwear dress is gorgeous and stylish everyone asked about it"),
    _branded("zenwear", 4, "zenwear return policy made my size exchange effortless"),
    _branded("zenwear", 0, "zenwear jeans shrunk after first wash such poor quality"),
    _branded("zenwear", 0, "zenwear order arrived with the wrong size and wrong colour"),
    _branded("zenwear", 0, "zenwear shoes fell apart in two weeks waste of money"),
    _branded("zenwear", 0, "zenwear customer service refused my refund very disappointing"),
)


def enrichment_tweets() -> tuple[dict, ...]:
    """Branded tweets for the live stream (label 4 = positive, 0 = negative)."""
    return ENRICHMENT_TWEETS