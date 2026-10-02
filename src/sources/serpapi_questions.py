"""SerpAPI questions and declarative request specifications.

Add an entity to a spec's variables with an explicit, permanent id. Add a new API
to QUESTION_SPECS with retrieval configuration, variables, and question metadata.
Templates retain the two forecast dates.
Each entry defines question_template, question_background and resolution_criteria;
variables and {url} in the background and criteria are formatted at update.
The optional observation_date_offset controls the stored date independently of
the requested date_offset; by default they are the same.

IDs are <api>__<entity>, plus __gte<percentage>pct for growth questions.
They never depend on list order, display names or collection dates. Keep old
entries while issued forecasts need collection. Change the entity id when changing
measurement semantics, filters, units or date policy. A different percentage
automatically produces a new ID.
Finance stores raw observations and fills missing dates when resolving. Snapshot
resolution uses the earliest successful measurement within 7 days on or after each
target date; flight dates must match exactly.
"""

from .serpapi_helpers import (
    SAVED_MEASUREMENT_RESOLUTION_CRITERIA,
    google_finance_window,
    parse_amazon_seller_price,
    parse_google_finance_price,
    parse_google_flight_departure_delay,
    parse_walmart_price,
    parse_youtube_max_views,
)

# not needed for google_finance_ticker_price and flight_departure_delay
COMMON_RESOLUTION_CRITERIA_FOR_SNAPSHOT_DATA = (
    "\n\nCollection is scheduled daily around 00:00 UTC; actual collection times can vary. "
    "Dates are UTC calendar dates, not exact midnight measurements. We retain the first "
    "successful snapshot saved for each day. For each comparison date, we use that day's "
    "snapshot or, if missing, the earliest successful snapshot saved within the next 7 days; "
    "a replacement for the forecast due date must be dated before the resolution date and "
    "is selected separately for each horizon. If either required snapshot is unavailable "
    "after its allowed fallback window, the comparison remains unresolved for that horizon "
    "and is excluded from scoring."
)

QUESTION_SPECS = {
    # Keep the category key for compatibility; new IDs isolate Amazon.com seller history.
    "amazon_minimum_product_price": {
        "question_template": (
            "Will the listed one-time price in USD for a new '{product}', sold by Amazon.com, "
            "be higher on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "This question tracks the new-condition offer sold by Amazon.com for {product} "
            "(ASIN: {asin}), in English and USD, for delivery to ZIP code 10001 (New York, NY). "
            "Use the listed item price for a one-time purchase, excluding shipping, taxes, "
            "coupons, subscription discounts and member-only prices. The seller must be "
            "Amazon.com; 'Ships from Amazon' alone is insufficient. Third-party sellers, "
            "Amazon Resale, used or refurbished items, other variants, unit prices and "
            "crossed-out prices do not qualify. Titles may change, but the ASIN must match. "
            "A missing or ambiguous qualifying offer is a missing measurement. If a required "
            "measurement remains unavailable after the fallback window in the resolution "
            "criteria, that horizon remains unresolved and is excluded from scoring. "
            "See: {url}\nSet the delivery ZIP code to 10001 while signed out, with English "
            "and USD selected; the URL does not set the delivery location. Keep the specified "
            "ASIN and variant selected. Open 'Other sellers on Amazon' or 'See All Buying "
            "Options' when needed, filter for New, and find 'Sold by Amazon.com'. Read that "
            "offer's item price. Prices and availability can change between visits."
        ),
        "resolution_criteria": SAVED_MEASUREMENT_RESOLUTION_CRITERIA
        + (
            "Resolves Yes if the eligible Amazon.com new-condition one-time item price is "
            "strictly higher on the resolution date than on the forecast due date; equal or "
            "lower resolves No. SerpApi must return the exact ASIN for amazon.com, English "
            "and delivery ZIP 10001. Its matching HTML capture must identify New condition, "
            "Amazon.com as seller, the exact ASIN and an ordinary Add to Cart USD price in "
            "the same offer. That seller and price must also appear in the JSON purchase "
            "options or other-seller results. A Buy New caption is not required. An omitted "
            "JSON currency means USD on this US listing. Missing evidence, conflicting "
            "qualifying prices or an unavailable HTML capture yield a missing measurement; "
            "we never substitute another seller's price. Returned offers can be incomplete. "
            "We do not separately verify checkout availability."
        )
        + COMMON_RESOLUTION_CRITERIA_FOR_SNAPSHOT_DATA,
        "engine": "amazon_product",
        "date_offset": 0,
        "url": "https://www.amazon.com/dp/{asin}?language=en_US",
        "variables": [
            # Amazon.com New offers matched SerpAPI JSON/HTML and signed-out browser prices
            # for ZIP 10001 on 2026-10-02. Future availability is not guaranteed.
            # Do not reuse the former minimum-price or featured Buy New history IDs.
            {
                "id": "b00063rwwa-amazon-com-new",
                "product": "Lodge L8DSK3 Seasoned Cast Iron Deep Skillet 10.25 Inch 3 Quart",
                "asin": "B00063RWWA",
            },
            {
                "id": "b00004ocip-amazon-com-new",
                "product": "OXO Good Grips Swivel Peeler 20081 Black 1 Pack",
                "asin": "B00004OCIP",
            },
            {
                "id": "b00flywnyq-amazon-com-new",
                "product": "Instant Pot Duo 6 Qt 7-in-1 Electric Pressure Cooker Stainless Steel",
                "asin": "B00FLYWNYQ",
            },
            {
                "id": "b00006ifhe-amazon-com-new",
                "product": "Sharpie Permanent Markers Fine Tip Red 12 Count",
                "asin": "B00006IFHE",
            },
            {
                "id": "b0055qavg2-amazon-com-new",
                "product": "Penn Championship Extra Duty Tennis Balls 12 Cans 36 Balls",
                "asin": "B0055QAVG2",
            },
            {
                "id": "b00004ocns-amazon-com-new",
                "product": "OXO Good Grips 11 Inch Balloon Whisk Black",
                "asin": "B00004OCNS",
            },
            {
                "id": "b00005al1c-amazon-com-new",
                "product": "All-Clad Copper Core 8 Inch Fry Pan Stainless Steel",
                "asin": "B00005AL1C",
            },
            {
                "id": "b00004s7v8-amazon-com-new",
                "product": "Microplane Classic 40020 Zester Grater Black",
                "asin": "B00004S7V8",
            },
            {
                "id": "b00005ibxj-amazon-com-new",
                "product": "Presto Pizzazz Plus 03430 Rotating Oven Black",
                "asin": "B00005IBXJ",
            },
            {
                "id": "b003vahyqy-amazon-com-new",
                "product": "Logitech F310 Wired Gamepad Blue and Black",
                "asin": "B003VAHYQY",
            },
            {
                "id": "b006jh8t3s-amazon-com-new",
                "product": "Logitech C920 HD Pro Webcam",
                "asin": "B006JH8T3S",
            },
            {
                "id": "b00004ockr-amazon-com-new",
                "product": "OXO Good Grips Salad Spinner 6.22 Qt White Large",
                "asin": "B00004OCKR",
            },
            {
                "id": "b0006huq9m-amazon-com-new",
                "product": "Swingline 747 Classic Stapler 74701 Black",
                "asin": "B0006HUQ9M",
            },
            {
                "id": "b001r1rxug-amazon-com-new",
                "product": "Honeywell TurboForce HT900 Fan Small Black",
                "asin": "B001R1RXUG",
            },
            {
                "id": "b0025qkue8-amazon-com-new",
                "product": "Vornado 660 Large Air Circulator Black 4 Speeds",
                "asin": "B0025QKUE8",
            },
            {
                "id": "b00080dpnq-amazon-com-new",
                "product": "Klein Tools 11055EP Wire Stripper and Cutter",
                "asin": "B00080DPNQ",
            },
            {
                "id": "b00006ieeu-amazon-com-new",
                "product": "Prismacolor Premier Soft Core Colored Pencils 24 Count Assorted Colors",
                "asin": "B00006IEEU",
            },
            {
                "id": "b00a128s24-amazon-com-new",
                "product": "TP-Link TL-SG105 5 Port Gigabit Ethernet Switch No PoE",
                "asin": "B00A128S24",
            },
            {
                "id": "b006lxojc0-amazon-com-new",
                "product": "BLACK+DECKER Dustbuster AdvancedClean CHV1410L 16V Handheld Vacuum",
                "asin": "B006LXOJC0",
            },
            {
                "id": "b00007j5u7-amazon-com-new",
                "product": "Zojirushi Neuro Fuzzy NS-ZCC10 Rice Cooker 5.5 Cups Uncooked Premium White",
                "asin": "B00007J5U7",
            },
        ],
        "params": lambda variables, requested_date: {
            "asin": variables["asin"],
            "amazon_domain": "amazon.com",
            "language": "en_US",
            "delivery_zip": "10001",
            "other_sellers": "true",
            "no_cache": "true",
        },
        "parse_response": parse_amazon_seller_price,
    },
    "walmart_food_drink_price": {
        "question_template": (
            "Will the listed price in USD of '{brand} {product}' on Walmart.com at "
            "{store_name} (store {store_id}) be higher "
            "on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "SerpApi retrieves the walmart.com price of '{brand} {product}' (item ID: {us_item_id}) "
            "at {store_name}, store {store_id}, at {store_address}. We use the positive listed "
            "USD price, excluding taxes, shipping, unit and previous prices, and other sellers "
            "or variants. The returned item and store IDs must match, and the item must be "
            "confirmed in stock. An omitted currency means USD; other currencies are rejected. "
            "No valid price means a missing measurement. If a required measurement remains "
            "unavailable after the fallback window specified in the resolution criteria, that "
            "horizon remains unresolved and is excluded from scoring. "
            "See: {url}\nOpen this product URL in your browser and select this store manually "
            "using Walmart's location selector; the URL does not automatically select this store."
        ),
        "resolution_criteria": SAVED_MEASUREMENT_RESOLUTION_CRITERIA
        + (
            "Resolves Yes if the eligible price defined in the background is strictly higher "
            "on the resolution date than on the forecast due date; equal or lower resolves No."
        )
        + COMMON_RESOLUTION_CRITERIA_FOR_SNAPSHOT_DATA,
        "engine": "walmart_product",
        "date_offset": 0,
        "url": "https://www.walmart.com/ip/{us_item_id}",
        "variables": [
            # Product listings and item IDs verified on Walmart.com, 2026-09-20.
            # Supercenter names, IDs and addresses verified in Walmart's store directory, 2026-09-26.
            # IDs are <us_item_id>-store-<store_id>, so a different store gives a new question.
            {
                "id": "12166733-store-3081",
                "brand": "Coca-Cola",
                "product": "Original Taste Cola Soda 12 fl oz Cans 12 Pack",
                "us_item_id": "12166733",
                "store_id": "3081",
                "store_name": "Sacramento Gerber Rd Supercenter",
                "store_address": "8915 Gerber Road, Sacramento, CA 95829",
            },
            {
                "id": "10321636-store-2152",
                "brand": "Campbell's",
                "product": "Condensed Tomato Soup 10.75 oz Can 1 Count",
                "us_item_id": "10321636",
                "store_id": "2152",
                "store_name": "Albany Supercenter",
                "store_address": "141 Washington Ave Extension, Albany, NY 12205",
            },
            {
                "id": "15529427-store-2141",
                "brand": "Heinz",
                "product": "Tomato Ketchup 32 oz Inverted Squeeze Bottle 1 Count",
                "us_item_id": "15529427",
                "store_id": "2141",
                "store_name": "Philadelphia S Christopher Columbus Blvd Supercenter",
                "store_address": "1675 S Christopher Columbus Blvd, Philadelphia, PA 19148",
            },
            {
                "id": "1830120353-store-2178",
                "brand": "OREO",
                "product": "Chocolate Sandwich Cookies Family Size 18.12 oz Pack",
                "us_item_id": "1830120353",
                "store_id": "2178",
                "store_name": "Calais Supercenter",
                "store_address": "379 South St, Calais, ME 04619",
            },
            {
                "id": "33282303-store-2682",
                "brand": "Lay's",
                "product": "Classic Potato Chips 8 oz Bag",
                "us_item_id": "33282303",
                "store_id": "2682",
                "store_name": "Berlin Supercenter",
                "store_address": "282 Berlin Mall Rd, Berlin, VT 05602",
            },
            {
                "id": "363183524-store-5402",
                "brand": "Cheerios",
                "product": "Original Gluten Free Breakfast Cereal Family Size 18 oz Box",
                "us_item_id": "363183524",
                "store_id": "5402",
                "store_name": "Chicago W N Ave Supercenter",
                "store_address": "4650 W North Ave, Chicago, IL 60639",
            },
            {
                "id": "10312439-store-5185",
                "brand": "Quaker",
                "product": "Old Fashioned Whole Grain Oats 42 oz Canister",
                "us_item_id": "10312439",
                "store_id": "5185",
                "store_name": "Columbus Georgesville Rd Supercenter",
                "store_address": "1221 Georgesville Rd, Columbus, OH 43228",
            },
            {
                "id": "777839120-store-3233",
                "brand": "Jif",
                "product": "Creamy Peanut Butter 16 oz Jar",
                "us_item_id": "777839120",
                "store_id": "3233",
                "store_name": "Bemidji Supercenter",
                "store_address": "2025 Paul Bunyan Dr NW, Bemidji, MN 56601",
            },
            {
                "id": "10321567-store-4352",
                "brand": "Smucker's",
                "product": "Concord Grape Jelly 18 oz Jar",
                "us_item_id": "10321567",
                "store_id": "4352",
                "store_name": "Fargo 55th Ave S Supercenter",
                "store_address": "3757 55th Ave S, Fargo, ND 58104",
            },
            {
                "id": "10309153-store-867",
                "brand": "Barilla",
                "product": "Classic Spaghetti 16 oz Box",
                "us_item_id": "10309153",
                "store_id": "867",
                "store_name": "Scottsbluff Supercenter",
                "store_address": "3322 Avenue I, Scottsbluff, NE 69361",
            },
            {
                "id": "131735446-store-4303",
                "brand": "Ben's Original",
                "product": "Ready Rice Jasmine Rice 8.5 oz Pouch",
                "us_item_id": "131735446",
                "store_id": "4303",
                "store_name": "Miami NW 79th St Supercenter",
                "store_address": "3200 NW 79th St, Miami, FL 33147",
            },
            {
                "id": "10295756-store-3709",
                "brand": "Kraft",
                "product": "Original Macaroni and Cheese Dinner 7.25 oz Box",
                "us_item_id": "10295756",
                "store_id": "3709",
                "store_name": "Atlanta Gresham Rd S E Supercenter",
                "store_address": "2427 Gresham Rd SE, Atlanta, GA 30316",
            },
            {
                "id": "10306771-store-3371",
                "brand": "Bush's",
                "product": "Original Baked Beans 28 oz Can",
                "us_item_id": "10306771",
                "store_id": "3371",
                "store_name": "Charlotte Wilkinson Blvd Supercenter",
                "store_address": "3240 Wilkinson Blvd, Charlotte, NC 28208",
            },
            {
                "id": "13398002-store-3500",
                "brand": "StarKist",
                "product": "Chunk Light Tuna in Water 5 oz Can",
                "us_item_id": "13398002",
                "store_id": "3500",
                "store_name": "Houston E Sam Houston Pkwy Wallisville Rd Supercenter",
                "store_address": "5655 E Sam Houston Pkwy N, Houston, TX 77015",
            },
            {
                "id": "10311311-store-964",
                "brand": "Gold Medal",
                "product": "All Purpose Flour 5 lb Bag",
                "us_item_id": "10311311",
                "store_id": "964",
                "store_name": "El Paso Alameda Avenue Supercenter",
                "store_address": "9441 Alameda Ave, El Paso, TX 79907",
            },
            {
                "id": "19500189-store-1315",
                "brand": "Domino",
                "product": "Pure Cane Granulated Sugar 4 lb Bag",
                "us_item_id": "19500189",
                "store_id": "1315",
                "store_name": "Cheyenne Dell Range Blvd Supercenter",
                "store_address": "2032 Dell Range Blvd, Cheyenne, WY 82009",
            },
            {
                "id": "10450650-store-1872",
                "brand": "Gatorade",
                "product": "Thirst Quencher Fruit Punch Sports Drink 20 fl oz Bottles 8 Count",
                "us_item_id": "10450650",
                "store_id": "1872",
                "store_name": "Helena Supercenter",
                "store_address": "2750 Prospect Ave, Helena, MT 59601",
            },
            {
                "id": "5512318310-store-2479",
                "brand": "Tropicana",
                "product": "Pure Premium Original 100% Orange Juice No Pulp 46 fl oz Bottle",
                "us_item_id": "5512318310",
                "store_id": "2479",
                "store_name": "San Diego College Ave Supercenter",
                "store_address": "3412 College Ave, San Diego, CA 92115",
            },
            {
                "id": "578550306-store-2596",
                "brand": "Folgers",
                "product": "Classic Roast Ground Coffee Medium Roast 25.9 oz Canister",
                "us_item_id": "578550306",
                "store_id": "2596",
                "store_name": "Mount Vernon Supercenter",
                "store_address": "2301 Freeway Dr, Mount Vernon, WA 98273",
            },
            {
                "id": "15177516255-store-2722",
                "brand": "Lipton",
                "product": "Black Tea Bags 100 Count Box",
                "us_item_id": "15177516255",
                "store_id": "2722",
                "store_name": "Fairbanks Supercenter",
                "store_address": "537 Johansen Expy, Fairbanks, AK 99701",
            },
        ],
        "params": lambda variables, requested_date: {
            "product_id": variables["us_item_id"],
            "store_id": variables["store_id"],
            "no_cache": "true",
        },
        "parse_response": parse_walmart_price,
    },
    "youtube_channel_max_video_views": {
        "question_template": (
            "Will the highest displayed view count in the first page of {channel_name}'s "
            "YouTube Popular Videos results (excluding Shorts and streams) "
            "be at least {percentage_threshold}% higher on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "SerpApi queries {channel_name}'s Videos tab (channel ID: {channel_id}), sorted by "
            "Popular. We use the highest displayed view count on the first page of results, "
            "excluding shorts, live streams and archived streams. Coverage may be incomplete, "
            "and the leading video may change between dates. Counts use the displayed rounding "
            "(e.g., 1.2M means 1,200,000), not exact video-page counts. The returned channel ID "
            "must match, Popular must be selected, and the first returned video must have a "
            "readable view count at least as high as every other readable count on the returned "
            "page; otherwise the measurement is missing. "
            "If a required measurement remains unavailable after the fallback window specified "
            "in the resolution criteria, that horizon remains unresolved and is excluded "
            "from scoring. "
            "See: {url}\nSelect Popular in the channel's Videos tab."
        ),
        "resolution_criteria": SAVED_MEASUREMENT_RESOLUTION_CRITERIA
        + (
            "Resolves Yes if the view count defined in the background is at least "
            "{percentage_threshold}% higher on the resolution date than on the forecast due "
            "date; otherwise resolves No. A baseline of zero or less leaves the question unresolved."
        )
        + COMMON_RESOLUTION_CRITERIA_FOR_SNAPSHOT_DATA,
        "engine": "youtube_channel",
        "date_offset": 0,
        "url": "https://www.youtube.com/channel/{channel_id}/videos",
        "variables": [
            # Query by handle; validate returned identity against the permanent channel ID.
            {
                "id": "markrober",
                "channel_name": "Mark Rober",
                "channel_id": "UCY1kMZp36IQSyNx_9h4mpCg",
                "channel_handle": "MarkRober",
                "percentage_threshold": 2,
            },
            {
                "id": "veritasium",
                "channel_name": "Veritasium",
                "channel_id": "UCHnyfMqiRRG1u-2MsSQLbXA",
                "channel_handle": "veritasium",
                "percentage_threshold": 5,
            },
            {
                "id": "mrbeast",
                "channel_name": "MrBeast",
                "channel_id": "UCX6OQ3DkcsbYNE6H8uQQuVA",
                "channel_handle": "MrBeast",
                "percentage_threshold": 10,
            },
            # Sports: https://www.youtube.com/@NBA/videos
            {
                "id": "nba",
                "channel_name": "NBA",
                "channel_id": "UCWJ2lWNubArHWmf3FIHbfcQ",
                "channel_handle": "NBA",
                "percentage_threshold": 2,
            },
            # Sports: https://www.youtube.com/@NFL/videos
            {
                "id": "nfl",
                "channel_name": "NFL",
                "channel_id": "UCDVYQ4Zhbm3S2dlz7P1GBDg",
                "channel_handle": "NFL",
                "percentage_threshold": 5,
            },
            # Sports: https://www.youtube.com/@FIFA/videos
            {
                "id": "fifa",
                "channel_name": "FIFA",
                "channel_id": "UCpcTrCXblq78GZrTUTLWeBw",
                "channel_handle": "FIFA",
                "percentage_threshold": 10,
            },
            # Sports and entertainment: https://www.youtube.com/@DudePerfect/videos
            {
                "id": "dudeperfect",
                "channel_name": "Dude Perfect",
                "channel_id": "UCRijo3ddMTht_IHyNSNXpNQ",
                "channel_handle": "DudePerfect",
                "percentage_threshold": 2,
            },
            # Education: https://www.youtube.com/@TED/videos
            {
                "id": "ted",
                "channel_name": "TED",
                "channel_id": "UCAuUUnT6oDeKwE6v1NGQxug",
                "channel_handle": "TED",
                "percentage_threshold": 5,
            },
            # Nature and exploration: https://www.youtube.com/@NatGeo/videos
            {
                "id": "natgeo",
                "channel_name": "National Geographic",
                "channel_id": "UCpVm7bg6pXKo1Pr6k5kxG9A",
                "channel_handle": "NatGeo",
                "percentage_threshold": 10,
            },
            # Science education: https://www.youtube.com/@kurzgesagt/videos
            {
                "id": "kurzgesagt",
                "channel_name": "Kurzgesagt – In a Nutshell",
                "channel_id": "UCsXVk37bltHxD1rDPwtNM8Q",
                "channel_handle": "kurzgesagt",
                "percentage_threshold": 2,
            },
            # Technology: https://www.youtube.com/@mkbhd/videos
            {
                "id": "mkbhd",
                "channel_name": "Marques Brownlee",
                "channel_id": "UCBJycsmduvYEL83R_U4JriQ",
                "channel_handle": "mkbhd",
                "percentage_threshold": 5,
            },
            # Technology: https://www.youtube.com/@Mrwhosetheboss/videos
            {
                "id": "mrwhosetheboss",
                "channel_name": "Mrwhosetheboss",
                "channel_id": "UCMiJRAwDNSNzuYeN2uWa0pA",
                "channel_handle": "Mrwhosetheboss",
                "percentage_threshold": 10,
            },
            # Cooking: https://www.youtube.com/@JamieOliver/videos
            {
                "id": "jamieoliver",
                "channel_name": "Jamie Oliver",
                "channel_id": "UCpSgg_ECBj25s9moCDfSTsA",
                "channel_handle": "JamieOliver",
                "percentage_threshold": 2,
            },
            # Nature: https://www.youtube.com/@BBCEarth/videos
            {
                "id": "bbcearth",
                "channel_name": "BBC Earth",
                "channel_id": "UCwmZiChSryoWQCZMIQezgTg",
                "channel_handle": "BBCEarth",
                "percentage_threshold": 5,
            },
            # Gaming: https://www.youtube.com/@IGN/videos
            {
                "id": "ign",
                "channel_name": "IGN",
                "channel_id": "UCKy1dAqELo0zrOtPkf0eTMw",
                "channel_handle": "IGN",
                "percentage_threshold": 10,
            },
            # News: https://www.youtube.com/@BBCNews/videos
            {
                "id": "bbcnews",
                "channel_name": "BBC News",
                "channel_id": "UC16niRr50-MSBwiO3YDb3RA",
                "channel_handle": "BBCNews",
                "percentage_threshold": 2,
            },
            # Music: https://www.youtube.com/@TaylorSwift/videos
            {
                "id": "taylorswift",
                "channel_name": "Taylor Swift",
                "channel_id": "UCqECaJ8Gagnn7YCbPEzWH6g",
                "channel_handle": "TaylorSwift",
                "percentage_threshold": 5,
            },
            # Music: https://www.youtube.com/@BLACKPINK/videos
            {
                "id": "blackpink",
                "channel_name": "BLACKPINK",
                "channel_id": "UCOmHUn--16B90oW2L6FRR3A",
                "channel_handle": "BLACKPINK",
                "percentage_threshold": 10,
            },
            # Comedy: https://www.youtube.com/@SaturdayNightLive/videos
            {
                "id": "saturdaynightlive",
                "channel_name": "Saturday Night Live",
                "channel_id": "UCqFzWxSCi39LnW1JKFR3efg",
                "channel_handle": "SaturdayNightLive",
                "percentage_threshold": 5,
            },
            # Comedy and current affairs: https://www.youtube.com/@TheDailyShow/videos
            {
                "id": "thedailyshow",
                "channel_name": "The Daily Show",
                "channel_id": "UCwWhs_6x42TyRM4Wstoq8HA",
                "channel_handle": "TheDailyShow",
                "percentage_threshold": 5,
            },
        ],
        "params": lambda variables, requested_date: {
            "channel_id": variables["channel_handle"],
            "tab": "videos",
            "sort": "popular",
            "gl": "us",
            "hl": "en",
            "no_cache": "true",
        },
        "parse_response": parse_youtube_max_views,
    },
    "google_finance_ticker_price": {
        "question_template": (
            "Will the value of {name} ({symbol}), measured in {units}, on Google Finance be "
            "higher on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "SerpApi retrieves Google Finance graph values for {name} ({symbol}), measured in "
            "{units}. We use the last valid value on each graph timestamp's local calendar date; "
            "these are not necessarily official closing prices. Values are used as returned by "
            "Google Finance, including any provider adjustments. We make no additional adjustments "
            "for splits or cash distributions. Collection is scheduled daily "
            "around 00:00 UTC, refreshing available observations from the past month through the "
            "previous UTC day and retaining older observations. Missing observations can be "
            "recovered while within this window and still returned by the API. If either required "
            "measurement is unavailable under the fallback rules in the resolution criteria, that "
            "horizon remains unresolved and is excluded from scoring until both measurements are "
            "available. See: {url}"
        ),
        "resolution_criteria": SAVED_MEASUREMENT_RESOLUTION_CRITERIA
        + (
            "Resolves Yes if the value defined in the background is strictly higher on the "
            "resolution date than on the forecast due date; equal or lower resolves No. For "
            "either date without an observation, use the most recent preceding observation, "
            "at most 14 calendar days old. Recovered observations replace fallback values; "
            "later valid responses may revise saved observations. If either required measurement "
            "is unavailable, the comparison remains unresolved and is excluded from scoring "
            "until both measurements are available."
        ),
        "engine": "google_finance",
        "date_offset": -1,
        "fill_missing_dates": True,
        "url": "https://www.google.com/finance/quote/{symbol}?hl=en",
        "variables": [
            # Country-market indexes, measured in index points.
            # Names are as displayed on Google Finance, checked 2026-09-27.
            # Germany
            {
                "id": "dax",
                "name": "DAX Performance Index",
                "symbol": "DAX:INDEXDB",
                "units": "index points",
            },
            # United Kingdom
            {
                "id": "ftse100",
                "name": "FTSE 100 Index",
                "symbol": "UKX:INDEXFTSE",
                "units": "index points",
            },
            # France
            {
                "id": "cac40",
                "name": "CAC 40",
                "symbol": "PX1:INDEXEURO",
                "units": "index points",
            },
            # Italy
            {
                "id": "ftsemib",
                "name": "FTSE MIB",
                "symbol": "FTSEMIB:INDEXBIT",
                "units": "index points",
            },
            # Spain
            {
                "id": "ibex35",
                "name": "IBEX 35",
                "symbol": "I:INDEXBME",
                "units": "index points",
            },
            # India
            {
                "id": "nifty50",
                "name": "NIFTY 50",
                "symbol": "NIFTY_50:INDEXNSE",
                "units": "index points",
            },
            # China
            {
                "id": "csi300",
                "name": "CSI 300 Index",
                "symbol": "000300:SHA",
                "units": "index points",
            },
            # Japan
            {
                "id": "topix",
                "name": "TOPIX",
                "symbol": "TOPIX:INDEXTOPIX",
                "units": "index points",
            },
            # Canada
            {
                "id": "tsx-composite",
                "name": "S&P/TSX Composite Index",
                "symbol": "OSPTX:INDEXTSI",
                "units": "index points",
            },
            # Brazil
            {
                "id": "ibovespa",
                "name": "IBOVESPA",
                "symbol": "IBOV:INDEXBVMF",
                "units": "index points",
            },
            # Chile
            {
                "id": "ftse-chile",
                "name": "FTSE Chile Index",
                "symbol": "WICHL:INDEXFTSE",
                "units": "index points",
            },
            # Israel
            {
                "id": "ta125",
                "name": "TA-125 Index",
                "symbol": "137:TLV",
                "units": "index points",
            },
            # Turkey
            {
                "id": "bist100",
                "name": "BIST 100",
                "symbol": "XU100:INDEXIST",
                "units": "index points",
            },
            # UAE
            {
                "id": "ftse-nasdaq-dubai-uae20-aed",
                "name": "FTSE NASDAQ Dubai UAE 20 Index AED",
                "symbol": "DUAED:INDEXFTSE",
                "units": "index points",
            },
            # Saudi Arabia
            {
                "id": "tadawul-all-share",
                "name": "Tadawul All-Share Index",
                "symbol": "TASI:TADAWUL",
                "units": "index points",
            },
            # Australia
            {
                "id": "asx200",
                "name": "S&P/ASX 200",
                "symbol": "XJO:INDEXASX",
                "units": "index points",
            },
            # New Zealand
            {
                "id": "nzx50",
                "name": "S&P/NZX 50 Index",
                "symbol": "NZ50G:NZE",
                "units": "index points",
            },
            # Malaysia
            {
                "id": "ftse-malaysia",
                "name": "FTSE Malaysia Index",
                "symbol": "WIMAL:INDEXFTSE",
                "units": "index points",
            },
            # South Korea
            {
                "id": "kospi",
                "name": "KOSPI",
                "symbol": "KOSPI:KRX",
                "units": "index points",
            },
            # Indonesia
            {
                "id": "idx-composite",
                "name": "IDX Composite",
                "symbol": "COMPOSITE:IDX",
                "units": "index points",
            },
            # U.S.-listed sector ETFs and commodity funds, quoted in USD.
            # Semiconductors
            {
                "id": "smh",
                "name": "VanEck Semiconductor ETF",
                "symbol": "SMH:NASDAQ",
                "units": "USD per share",
            },
            # Artificial intelligence
            {
                "id": "aiq",
                "name": "Global X Artificial Intelligence & Technology ETF",
                "symbol": "AIQ:NASDAQ",
                "units": "USD per share",
            },
            # Robotics and automation
            {
                "id": "botz",
                "name": "Global X Robotics and Artificial Intelligence ETF",
                "symbol": "BOTZ:NASDAQ",
                "units": "USD per share",
            },
            # Energy
            {
                "id": "xle",
                "name": "State Street Energy Select Sector SPDR ETF",
                "symbol": "XLE:NYSEARCA",
                "units": "USD per share",
            },
            # Electricity and utilities
            {
                "id": "xlu",
                "name": "State Street Utilities Select Sector SPDR ETF",
                "symbol": "XLU:NYSEARCA",
                "units": "USD per share",
            },
            # Solar energy
            {
                "id": "tan",
                "name": "Invesco Solar ETF",
                "symbol": "TAN:NYSEARCA",
                "units": "USD per share",
            },
            # Uranium and nuclear energy
            {
                "id": "ura",
                "name": "Global X Uranium ETF",
                "symbol": "URA:NYSEARCA",
                "units": "USD per share",
            },
            # Physical gold
            {
                "id": "gld",
                "name": "SPDR Gold Trust",
                "symbol": "GLD:NYSEARCA",
                "units": "USD per share",
            },
            # Physical silver
            {
                "id": "slv",
                "name": "iShares Silver Trust",
                "symbol": "SLV:NYSEARCA",
                "units": "USD per share",
            },
            # Copper miners
            {
                "id": "copx",
                "name": "Global X Copper Miners ETF",
                "symbol": "COPX:NYSEARCA",
                "units": "USD per share",
            },
            # Agricultural commodity futures
            {
                "id": "dba",
                "name": "Invesco DB Agriculture Fund",
                "symbol": "DBA:NYSEARCA",
                "units": "USD per share",
            },
            # Agribusiness
            {
                "id": "moo",
                "name": "VanEck Agribusiness ETF",
                "symbol": "MOO:NYSEARCA",
                "units": "USD per share",
            },
            # Manufacturing and industrials
            {
                "id": "xli",
                "name": "State Street Industrial Select Sector SPDR ETF",
                "symbol": "XLI:NYSEARCA",
                "units": "USD per share",
            },
            # Finance
            {
                "id": "xlf",
                "name": "State Street Financial Select Sector SPDR ETF",
                "symbol": "XLF:NYSEARCA",
                "units": "USD per share",
            },
            # Telecommunications
            {
                "id": "iyz",
                "name": "iShares US Telecommunications ETF",
                "symbol": "IYZ:BATS",
                "units": "USD per share",
            },
            # Infrastructure
            {
                "id": "pave",
                "name": "Global X US Infrastructure Development ETF",
                "symbol": "PAVE:BATS",
                "units": "USD per share",
            },
            # Housing construction
            {
                "id": "itb",
                "name": "iShares US Home Construction ETF",
                "symbol": "ITB:BATS",
                "units": "USD per share",
            },
            # Food and household essentials
            {
                "id": "xlp",
                "name": "State Street Consumer Staples Select Sector SPDR ETF",
                "symbol": "XLP:NYSEARCA",
                "units": "USD per share",
            },
            # Materials and resources
            {
                "id": "xlb",
                "name": "State Street Materials Select Sector SPDR ETF",
                "symbol": "XLB:NYSEARCA",
                "units": "USD per share",
            },
            # Water infrastructure
            {
                "id": "pho",
                "name": "Invesco Water Resources ETF",
                "symbol": "PHO:NASDAQ",
                "units": "USD per share",
            },
        ],
        "params": lambda variables, requested_date: {
            "q": variables["symbol"],
            "window": google_finance_window(requested_date),
            "hl": "en",
        },
        "parse_response": parse_google_finance_price,
    },
    "flight_departure_delay": {
        "question_template": (
            "Will the departure delay in minutes of flight {flight_id} ({origin} to "
            "{destination}) scheduled for local departure date {resolution_date} be greater "
            "than for local departure date {forecast_due_date}, counting early and on-time "
            "departures as zero delay?"
        ),
        "question_background": (
            "SerpApi retrieves Google's reported departure delay in minutes for flight {flight_id} "
            "({origin} to {destination}), matching the flight number, route and scheduled local "
            "departure date. Only flights marked departed, landed or arrived qualify. Departure "
            "delay measures minutes late: early and on-time departures count as zero delay, "
            "and positive reported delays are used unchanged. Collection is scheduled daily "
            "around 00:00 UTC, using queries without a date and recording eligible flights "
            "through the previous UTC day under their "
            "scheduled local departure dates. Missing delays can be recovered only while Google "
            "still returns that flight date. If either required flight-date measurement is "
            "unavailable, that horizon remains unresolved and is excluded from scoring until both "
            "measurements are available. See: {url}"
        ),
        "resolution_criteria": SAVED_MEASUREMENT_RESOLUTION_CRITERIA
        + (
            "Resolves Yes if the departure delay defined in the background is strictly greater "
            "on the resolution date than on the forecast due date; equal or lower resolves No. "
            "For each date, use max(0, reported departure delay in minutes), so early and on-time "
            "departures both count as zero delay. "
            "Each date requires a unique eligible flight matching that exact scheduled local "
            "departure date; never substitute another date or carry forward a delay. Missing, "
            "canceled or not-yet-departed flights have no measurement, not zero. Later valid "
            "responses may revise saved departure delays; comparisons use the latest saved valid "
            "measurements. If either required measurement is unavailable, the comparison remains "
            "unresolved and is excluded from scoring until both measurements are available."
        ),
        "engine": "google",
        "date_offset": -1,
        "url": "https://www.google.com/search?q={flight_id}+flight+status&hl=en&gl=us",
        "variables": [
            {"id": "ba117-lhr-jfk", "flight_id": "BA117", "origin": "LHR", "destination": "JFK"},
            {"id": "ek203-dxb-jfk", "flight_id": "EK203", "origin": "DXB", "destination": "JFK"},
            {"id": "sq308-sin-lhr", "flight_id": "SQ308", "origin": "SIN", "destination": "LHR"},
            {"id": "lh400-fra-jfk", "flight_id": "LH400", "origin": "FRA", "destination": "JFK"},
            {"id": "af6-cdg-jfk", "flight_id": "AF6", "origin": "CDG", "destination": "JFK"},
            {"id": "kl601-ams-lax", "flight_id": "KL601", "origin": "AMS", "destination": "LAX"},
            {"id": "qf35-mel-sin", "flight_id": "QF35", "origin": "MEL", "destination": "SIN"},
            {"id": "nz6-akl-lax", "flight_id": "NZ6", "origin": "AKL", "destination": "LAX"},
            {"id": "jl6-hnd-jfk", "flight_id": "JL6", "origin": "HND", "destination": "JFK"},
            {"id": "qr701-doh-jfk", "flight_id": "QR701", "origin": "DOH", "destination": "JFK"},
            {"id": "qf11-syd-lax", "flight_id": "QF11", "origin": "SYD", "destination": "LAX"},
            {"id": "ey1-auh-jfk", "flight_id": "EY1", "origin": "AUH", "destination": "JFK"},
            {"id": "ac7-yvr-hkg", "flight_id": "AC7", "origin": "YVR", "destination": "HKG"},
            {"id": "ua1-sfo-sin", "flight_id": "UA1", "origin": "SFO", "destination": "SIN"},
            {"id": "dl30-atl-lhr", "flight_id": "DL30", "origin": "ATL", "destination": "LHR"},
            {"id": "aa100-jfk-lhr", "flight_id": "AA100", "origin": "JFK", "destination": "LHR"},
            {"id": "kq100-nbo-lhr", "flight_id": "KQ100", "origin": "NBO", "destination": "LHR"},
            {"id": "et602-add-dxb", "flight_id": "ET602", "origin": "ADD", "destination": "DXB"},
            {"id": "cx255-hkg-lhr", "flight_id": "CX255", "origin": "HKG", "destination": "LHR"},
            {"id": "la2478-lim-lax", "flight_id": "LA2478", "origin": "LIM", "destination": "LAX"},
        ],
        "params": lambda variables, requested_date: {
            # Including a date can suppress Google's flight-status result; validate returned dates.
            "q": f"{variables['flight_id']} flight status",
            "google_domain": "google.com",
            "hl": "en",
            "gl": "us",
        },
        "parse_response": parse_google_flight_departure_delay,
    },
}
