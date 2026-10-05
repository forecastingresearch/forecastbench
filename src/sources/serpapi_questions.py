"""SerpAPI questions and declarative request specifications.

Add an entity to a spec's variables with an explicit, permanent id. Add a new API
to QUESTION_SPECS with retrieval configuration, variables, and question metadata.
Templates retain placeholders for the forecast dates they use.
Each entry defines question_template, question_background and resolution_criteria;
variables and {url} in the background and criteria are formatted at update.
The optional observation_date_offset controls the stored date independently of
the requested date_offset; by default they are the same.

IDs are <api>__<entity>.
They never depend on list order, display names or collection dates. Keep old
entries while issued forecasts need collection. Change the entity id when changing
measurement semantics, filters, units or date policy.
Snapshot resolution uses the earliest successful measurement within 7 days on or after each
target date; flight dates must match exactly.
"""

from .serpapi_helpers import (
    parse_amazon_seller_price,
    parse_google_flight_departure_delay,
    parse_walmart_price,
)

# Flight departure delays use exact dates instead of snapshot fallback.
COMMON_SNAPSHOT_BACKGROUND = (
    "\n\nData collection runs around 00:00 UTC; timing varies. Keep each UTC day's first valid "
    "snapshot. For either comparison date, replace missing data with the earliest valid "
    "snapshot within the next 7 calendar days, inclusive. Choose the due-date replacement "
    "per horizon, strictly before the resolution date. Without both snapshots, the horizon "
    "remains unresolved and unscored until qualifying data arrive. Freeze values use the "
    "latest saved date without replacement (N/A if missing)."
)

QUESTION_SPECS = {
    "amazon_minimum_product_price": {
        "question_template": (
            "Will the listed price in USD for a new '{product}', sold by Amazon.com, "
            "be higher on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "The listed price in USD for {product} (ASIN {asin}), in new condition and sold "
            "directly by Amazon.com, collected in English for delivery to "
            "ZIP 10001. Other sellers, product variants, Amazon Resale, and used or "
            "refurbished items do not qualify. Shipping, taxes, coupons, subscriptions, "
            "member-only prices, per-unit prices and crossed-out prices are excluded. "
            "Measurements require confirmation of the product, new condition, Amazon.com "
            "seller and listed price; missing or conflicting evidence makes the measurement "
            "unavailable. Amazon fulfillment alone does not establish the seller. Product "
            "titles may change; the ASIN identifies the product. Returned offers may be "
            "incomplete, and checkout prices are not verified. See: {url}. "
            "When checking Amazon manually, stay signed out and select ZIP 10001; "
            "the URL does not set it."
        )
        + COMMON_SNAPSHOT_BACKGROUND,
        "resolution_criteria": (
            "Uses only ForecastBench's saved SerpAPI amazon_product engine measurements "
            "as ground truth, following the background's rules. "
            "Resolves Yes if the resolution-date price is strictly higher than the "
            "forecast-due-date price; equal or lower resolves No."
        ),
        "engine": "amazon_product",
        "date_offset": 0,
        "url": "https://www.amazon.com/dp/{asin}?language=en_US",
        "variables": [
            {
                "id": "b00063rwwa-amazon-com-new",
                "product": "Lodge L8DSK3 Seasoned Cast Iron Deep Skillet 10.25 Inch 3 Quart",
                "asin": "B00063RWWA",
            },
            {
                "id": "b00mnv8e0c-amazon-com-new",
                "product": "Amazon Basics AA Alkaline Batteries 1.5 Volt 48 Pack",
                "asin": "B00MNV8E0C",
            },
            {
                "id": "b00fxnaaw2-amazon-com-new",
                "product": "Amazon Basics Slim Velvet Non-Slip Suit Clothes Hangers Black 50 Pack",
                "asin": "B00FXNAAW2",
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
            {
                "id": "b00004ocnj-amazon-com-new",
                "product": "OXO Good Grips Multi-Purpose Scraper and Chopper 73281",
                "asin": "B00004OCNJ",
            },
            {
                "id": "b001kvztsg-amazon-com-new",
                "product": "Fiskars Power-Lever Extendable Hedge Shears 25-33 Inch",
                "asin": "B001KVZTSG",
            },
            {
                "id": "b0030mihau-amazon-com-new",
                "product": "Fiskars Deluxe Stand-Up Weed Puller 4 Claw",
                "asin": "B0030MIHAU",
            },
            {
                "id": "b003nredc8-amazon-com-new",
                "product": "Logitech MK120 Wired Keyboard and Mouse Combo Black",
                "asin": "B003NREDC8",
            },
            {
                "id": "b09hswbtn4-amazon-com-new",
                "product": "Cuisinart TOA-70 Air Fryer Toaster Oven with Grill Stainless Steel",
                "asin": "B09HSWBTN4",
            },
            {
                "id": "b0000a1zn1-amazon-com-new",
                "product": "Cuisinart CPT-180P1 4-Slice Toaster Stainless Steel",
                "asin": "B0000A1ZN1",
            },
            {
                "id": "b003kyslnq-amazon-com-new",
                "product": "Cuisinart PerfecTemp CPK-17P1 Electric Kettle 1.7 Liter Stainless Steel",
                "asin": "B003KYSLNQ",
            },
            {
                "id": "b001ch0zle-amazon-com-new",
                "product": "Hamilton Beach 62682G 6-Speed Electric Hand Mixer White",
                "asin": "B001CH0ZLE",
            },
            {
                "id": "b006fan49s-amazon-com-new",
                "product": "DEWALT DWHT70485 Compound Action Pliers Set 3 Piece",
                "asin": "B006FAN49S",
            },
            {
                "id": "b007tuqf9o-amazon-com-new",
                "product": "Cuisinart CTG-00-3MS Stainless Steel Mesh Strainers 3 Piece",
                "asin": "B007TUQF9O",
            },
            {
                "id": "b00ei7dpi0-amazon-com-new",
                "product": "Hamilton Beach Power Elite 58148AG Blender 40 oz Glass Jar Black",
                "asin": "B00EI7DPI0",
            },
            {
                "id": "b00ij0alys-amazon-com-new",
                "product": "DEWALT DCK240C2 20V MAX Drill and Impact Driver Kit with 2 Batteries",
                "asin": "B00IJ0ALYS",
            },
            {
                "id": "b00klvy3tw-amazon-com-new",
                "product": "Hamilton Beach 25361MN Indoor Searing Grill with Viewing Window",
                "asin": "B00KLVY3TW",
            },
            {
                "id": "b00n3l2dmg-amazon-com-new",
                "product": "Hamilton Beach 25490A Dual Breakfast Sandwich Maker Silver",
                "asin": "B00N3L2DMG",
            },
            {
                "id": "b00t4rh8e6-amazon-com-new",
                "product": "Hamilton Beach Smooth Touch 76606AG Electric Can Opener Black and Chrome",
                "asin": "B00T4RH8E6",
            },
            {
                "id": "b09hn1c1yj-amazon-com-new",
                "product": "Coleman Triton 2-Burner Propane Camping Stove Matte Black",
                "asin": "B09HN1C1YJ",
            },
            {
                "id": "b0c1lxsvsh-amazon-com-new",
                "product": "DEWALT Atomic Compact Series DWHT38130S Tape Measure 30 ft",
                "asin": "B0C1LXSVSH",
            },
            {
                "id": "b00006iuwa-amazon-com-new",
                "product": "Presto PopLite 04820 Hot Air Popcorn Popper Yellow",
                "asin": "B00006IUWA",
            },
            {
                "id": "b0000z6jjg-amazon-com-new",
                "product": "Presto Professional SaladShooter 02970 Electric Slicer and Shredder Black",
                "asin": "B0000Z6JJG",
            },
            {
                "id": "b0007xrtdg-amazon-com-new",
                "product": "Presto 06852 Electric Skillet 16 Inch with Glass Cover Black",
                "asin": "B0007XRTDG",
            },
            {
                "id": "b000tybwig-amazon-com-new",
                "product": "Presto FlipSide 03510 Belgian Waffle Maker Black and Gray",
                "asin": "B000TYBWIG",
            },
            {
                "id": "b005fyf3oy-amazon-com-new",
                "product": "Presto 07061 Electric Ceramic Griddle 22 Inch Black",
                "asin": "B005FYF3OY",
            },
            {
                "id": "b00ei7dppi-amazon-com-new",
                "product": "Hamilton Beach 49980R 2-Way Coffee Maker 12 Cup Black and Stainless Steel",
                "asin": "B00EI7DPPI",
            },
            {
                "id": "b0007gawrs-amazon-com-new",
                "product": "Escali Primo Digital Kitchen Scale Chrome",
                "asin": "B0007GAWRS",
            },
            {
                "id": "b00006jnu2-amazon-com-new",
                "product": "Bostitch Impulse 02210 Electric Stapler 30 Sheet Black",
                "asin": "B00006JNU2",
            },
            {
                "id": "b00nj2m33i-amazon-com-new",
                "product": "Sony MDR-ZX110 Wired On-Ear Headphones Black No Microphone",
                "asin": "B00NJ2M33I",
            },
            {
                "id": "b08yj7v76j-amazon-com-new",
                "product": "Fellowes 14C10 Cross-Cut Paper Shredder 14 Sheet Black",
                "asin": "B08YJ7V76J",
            },
            {
                "id": "b0012yvgow-amazon-com-new",
                "product": "BIC Round Stic Xtra Life Ballpoint Pens Black Ink 60 Count",
                "asin": "B0012YVGOW",
            },
            {
                "id": "b001rjtq8u-amazon-com-new",
                "product": "BLACK+DECKER Classic F67E-T Steam Iron Black",
                "asin": "B001RJTQ8U",
            },
            {
                "id": "b008h2oely-amazon-com-new",
                "product": "Presto Dehydro 06300 Electric Food Dehydrator White",
                "asin": "B008H2OELY",
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
            "Will the listed price in USD of '{brand} {product}', sold by Walmart.com at "
            "{store_name} (store {store_id}), be higher "
            "on {resolution_date} than on {forecast_due_date}?"
        ),
        "question_background": (
            "The listed price in USD for '{brand} {product}' (item {us_item_id}), sold "
            "directly by Walmart.com at {store_name} (store {store_id}), {store_address}. "
            "Only in-stock items with a positive price in the primary product result qualify. "
            "Other sellers, product variants and alternative offers do not qualify. Shipping, "
            "taxes, per-unit prices and previous prices are excluded. Measurements require "
            "confirmation of the item, selected store, availability, Walmart.com seller and "
            "listed price; missing or conflicting evidence makes the measurement unavailable. "
            "Walmart fulfillment does not establish the seller. See: {url}. "
            "When checking Walmart, select the store manually; the URL does not set it."
        )
        + COMMON_SNAPSHOT_BACKGROUND,
        "resolution_criteria": (
            "Uses only ForecastBench's saved SerpAPI walmart_product engine measurements "
            "as ground truth, following the background's rules. "
            "Resolves Yes if the resolution-date price is strictly higher than the "
            "forecast-due-date price; equal or lower resolves No."
        ),
        "engine": "walmart_product",
        "date_offset": 0,
        "url": "https://www.walmart.com/ip/{us_item_id}",
        "variables": [
            # IDs are <us_item_id>-store-<store_id>-walmart-com.
            {
                "id": "12166733-store-3081-walmart-com",
                "brand": "Coca-Cola",
                "product": "Original Taste Cola Soda 12 fl oz Cans 12 Pack",
                "us_item_id": "12166733",
                "store_id": "3081",
                "store_name": "Sacramento Gerber Rd Supercenter",
                "store_address": "8915 Gerber Road, Sacramento, CA 95829",
            },
            {
                "id": "10321636-store-2152-walmart-com",
                "brand": "Campbell's",
                "product": "Condensed Tomato Soup 10.75 oz Can 1 Count",
                "us_item_id": "10321636",
                "store_id": "2152",
                "store_name": "Albany Supercenter",
                "store_address": "141 Washington Ave Extension, Albany, NY 12205",
            },
            {
                "id": "15529427-store-2141-walmart-com",
                "brand": "Heinz",
                "product": "Tomato Ketchup 32 oz Inverted Squeeze Bottle 1 Count",
                "us_item_id": "15529427",
                "store_id": "2141",
                "store_name": "Philadelphia S Christopher Columbus Blvd Supercenter",
                "store_address": "1675 S Christopher Columbus Blvd, Philadelphia, PA 19148",
            },
            {
                "id": "1830120353-store-2178-walmart-com",
                "brand": "OREO",
                "product": "Chocolate Sandwich Cookies Family Size 18.12 oz Pack",
                "us_item_id": "1830120353",
                "store_id": "2178",
                "store_name": "Calais Supercenter",
                "store_address": "379 South St, Calais, ME 04619",
            },
            {
                "id": "33282303-store-2682-walmart-com",
                "brand": "Lay's",
                "product": "Classic Potato Chips 8 oz Bag",
                "us_item_id": "33282303",
                "store_id": "2682",
                "store_name": "Berlin Supercenter",
                "store_address": "282 Berlin Mall Rd, Berlin, VT 05602",
            },
            {
                "id": "363183524-store-5402-walmart-com",
                "brand": "Cheerios",
                "product": "Original Gluten Free Breakfast Cereal Family Size 18 oz Box",
                "us_item_id": "363183524",
                "store_id": "5402",
                "store_name": "Chicago W N Ave Supercenter",
                "store_address": "4650 W North Ave, Chicago, IL 60639",
            },
            {
                "id": "10312439-store-5185-walmart-com",
                "brand": "Quaker",
                "product": "Old Fashioned Whole Grain Oats 42 oz Canister",
                "us_item_id": "10312439",
                "store_id": "5185",
                "store_name": "Columbus Georgesville Rd Supercenter",
                "store_address": "1221 Georgesville Rd, Columbus, OH 43228",
            },
            {
                "id": "777839120-store-3233-walmart-com",
                "brand": "Jif",
                "product": "Creamy Peanut Butter 16 oz Jar",
                "us_item_id": "777839120",
                "store_id": "3233",
                "store_name": "Bemidji Supercenter",
                "store_address": "2025 Paul Bunyan Dr NW, Bemidji, MN 56601",
            },
            {
                "id": "10321567-store-4352-walmart-com",
                "brand": "Smucker's",
                "product": "Concord Grape Jelly 18 oz Jar",
                "us_item_id": "10321567",
                "store_id": "4352",
                "store_name": "Fargo 55th Ave S Supercenter",
                "store_address": "3757 55th Ave S, Fargo, ND 58104",
            },
            {
                "id": "10309153-store-867-walmart-com",
                "brand": "Barilla",
                "product": "Classic Spaghetti 16 oz Box",
                "us_item_id": "10309153",
                "store_id": "867",
                "store_name": "Scottsbluff Supercenter",
                "store_address": "3322 Avenue I, Scottsbluff, NE 69361",
            },
            {
                "id": "131735446-store-4303-walmart-com",
                "brand": "Ben's Original",
                "product": "Ready Rice Jasmine Rice 8.5 oz Pouch",
                "us_item_id": "131735446",
                "store_id": "4303",
                "store_name": "Miami NW 79th St Supercenter",
                "store_address": "3200 NW 79th St, Miami, FL 33147",
            },
            {
                "id": "10295756-store-3709-walmart-com",
                "brand": "Kraft",
                "product": "Original Macaroni and Cheese Dinner 7.25 oz Box",
                "us_item_id": "10295756",
                "store_id": "3709",
                "store_name": "Atlanta Gresham Rd S E Supercenter",
                "store_address": "2427 Gresham Rd SE, Atlanta, GA 30316",
            },
            {
                "id": "10306771-store-3371-walmart-com",
                "brand": "Bush's",
                "product": "Original Baked Beans 28 oz Can",
                "us_item_id": "10306771",
                "store_id": "3371",
                "store_name": "Charlotte Wilkinson Blvd Supercenter",
                "store_address": "3240 Wilkinson Blvd, Charlotte, NC 28208",
            },
            {
                "id": "13398002-store-3500-walmart-com",
                "brand": "StarKist",
                "product": "Chunk Light Tuna in Water 5 oz Can",
                "us_item_id": "13398002",
                "store_id": "3500",
                "store_name": "Houston E Sam Houston Pkwy Wallisville Rd Supercenter",
                "store_address": "5655 E Sam Houston Pkwy N, Houston, TX 77015",
            },
            {
                "id": "10311311-store-964-walmart-com",
                "brand": "Gold Medal",
                "product": "All Purpose Flour 5 lb Bag",
                "us_item_id": "10311311",
                "store_id": "964",
                "store_name": "El Paso Alameda Avenue Supercenter",
                "store_address": "9441 Alameda Ave, El Paso, TX 79907",
            },
            {
                "id": "19500189-store-1315-walmart-com",
                "brand": "Domino",
                "product": "Pure Cane Granulated Sugar 4 lb Bag",
                "us_item_id": "19500189",
                "store_id": "1315",
                "store_name": "Cheyenne Dell Range Blvd Supercenter",
                "store_address": "2032 Dell Range Blvd, Cheyenne, WY 82009",
            },
            {
                "id": "10450650-store-1872-walmart-com",
                "brand": "Gatorade",
                "product": "Thirst Quencher Fruit Punch Sports Drink 20 fl oz Bottles 8 Count",
                "us_item_id": "10450650",
                "store_id": "1872",
                "store_name": "Helena Supercenter",
                "store_address": "2750 Prospect Ave, Helena, MT 59601",
            },
            {
                "id": "5512318310-store-2479-walmart-com",
                "brand": "Tropicana",
                "product": "Pure Premium Original 100% Orange Juice No Pulp 46 fl oz Bottle",
                "us_item_id": "5512318310",
                "store_id": "2479",
                "store_name": "San Diego College Ave Supercenter",
                "store_address": "3412 College Ave, San Diego, CA 92115",
            },
            {
                "id": "578550306-store-2596-walmart-com",
                "brand": "Folgers",
                "product": "Classic Roast Ground Coffee Medium Roast 25.9 oz Canister",
                "us_item_id": "578550306",
                "store_id": "2596",
                "store_name": "Mount Vernon Supercenter",
                "store_address": "2301 Freeway Dr, Mount Vernon, WA 98273",
            },
            {
                "id": "15177516255-store-2722-walmart-com",
                "brand": "Lipton",
                "product": "Black Tea Bags 100 Count Box",
                "us_item_id": "15177516255",
                "store_id": "2722",
                "store_name": "Fairbanks Supercenter",
                "store_address": "537 Johansen Expy, Fairbanks, AK 99701",
            },
            {
                "id": "14089343-store-3081-walmart-com",
                "brand": "French's",
                "product": "Classic Yellow Mustard 20 oz Bottle",
                "us_item_id": "14089343",
                "store_id": "3081",
                "store_name": "Sacramento Gerber Rd Supercenter",
                "store_address": "8915 Gerber Road, Sacramento, CA 95829",
            },
            {
                "id": "10312980-store-2152-walmart-com",
                "brand": "Hellmann's",
                "product": "Real Mayonnaise 30 fl oz Jar",
                "us_item_id": "10312980",
                "store_id": "2152",
                "store_name": "Albany Supercenter",
                "store_address": "141 Washington Ave Extension, Albany, NY 12205",
            },
            {
                "id": "10413990-store-2152-walmart-com",
                "brand": "Hidden Valley",
                "product": "Original Ranch Dressing 36 fl oz Bottle",
                "us_item_id": "10413990",
                "store_id": "2152",
                "store_name": "Albany Supercenter",
                "store_address": "141 Washington Ave Extension, Albany, NY 12205",
            },
            {
                "id": "10317856-store-2141-walmart-com",
                "brand": "Tabasco",
                "product": "Original Red Pepper Sauce 2 fl oz Bottle",
                "us_item_id": "10317856",
                "store_id": "2141",
                "store_name": "Philadelphia S Christopher Columbus Blvd Supercenter",
                "store_address": "1675 S Christopher Columbus Blvd, Philadelphia, PA 19148",
            },
            {
                "id": "10307412-store-2141-walmart-com",
                "brand": "Kikkoman",
                "product": "Soy Sauce 10 fl oz Bottle",
                "us_item_id": "10307412",
                "store_id": "2141",
                "store_name": "Philadelphia S Christopher Columbus Blvd Supercenter",
                "store_address": "1675 S Christopher Columbus Blvd, Philadelphia, PA 19148",
            },
            {
                "id": "10308233-store-5402-walmart-com",
                "brand": "Prego",
                "product": "Traditional Pasta Sauce 24 oz Jar",
                "us_item_id": "10308233",
                "store_id": "5402",
                "store_name": "Chicago W N Ave Supercenter",
                "store_address": "4650 W North Ave, Chicago, IL 60639",
            },
            {
                "id": "10295217-store-5402-walmart-com",
                "brand": "Del Monte",
                "product": "Golden Sweet Whole Kernel Corn 15.25 oz Can",
                "us_item_id": "10295217",
                "store_id": "5402",
                "store_name": "Chicago W N Ave Supercenter",
                "store_address": "4650 W North Ave, Chicago, IL 60639",
            },
            {
                "id": "10304322-store-5185-walmart-com",
                "brand": "Dole",
                "product": "Pineapple Slices in 100% Pineapple Juice 20 oz Can",
                "us_item_id": "10304322",
                "store_id": "5185",
                "store_name": "Columbus Georgesville Rd Supercenter",
                "store_address": "1221 Georgesville Rd, Columbus, OH 43228",
            },
            {
                "id": "10295073-store-5185-walmart-com",
                "brand": "Hunt's",
                "product": "Diced Tomatoes 14.5 oz Can",
                "us_item_id": "10295073",
                "store_id": "5185",
                "store_name": "Columbus Georgesville Rd Supercenter",
                "store_address": "1221 Georgesville Rd, Columbus, OH 43228",
            },
            {
                "id": "10290957-store-3233-walmart-com",
                "brand": "Hormel",
                "product": "Chili No Beans 15 oz Can",
                "us_item_id": "10290957",
                "store_id": "3233",
                "store_name": "Bemidji Supercenter",
                "store_address": "2025 Paul Bunyan Dr NW, Bemidji, MN 56601",
            },
            {
                "id": "10290926-store-3233-walmart-com",
                "brand": "SPAM",
                "product": "Classic 12 oz Can",
                "us_item_id": "10290926",
                "store_id": "3233",
                "store_name": "Bemidji Supercenter",
                "store_address": "2025 Paul Bunyan Dr NW, Bemidji, MN 56601",
            },
            {
                "id": "15754233-store-4352-walmart-com",
                "brand": "Maruchan",
                "product": "Chicken Flavor Instant Ramen Noodle Soup 3 oz Pack",
                "us_item_id": "15754233",
                "store_id": "4352",
                "store_name": "Fargo 55th Ave S Supercenter",
                "store_address": "3757 55th Ave S, Fargo, ND 58104",
            },
            {
                "id": "10448936-store-4352-walmart-com",
                "brand": "Morton",
                "product": "Iodized Table Salt 26 oz Canister",
                "us_item_id": "10448936",
                "store_id": "4352",
                "store_name": "Fargo 55th Ave S Supercenter",
                "store_address": "3757 55th Ave S, Fargo, ND 58104",
            },
            {
                "id": "10291025-store-867-walmart-com",
                "brand": "Arm & Hammer",
                "product": "Baking Soda 1 lb Box",
                "us_item_id": "10291025",
                "store_id": "867",
                "store_name": "Scottsbluff Supercenter",
                "store_address": "3322 Avenue I, Scottsbluff, NE 69361",
            },
            {
                "id": "495170161-store-867-walmart-com",
                "brand": "Crisco",
                "product": "Pure Vegetable Oil 40 fl oz Bottle",
                "us_item_id": "495170161",
                "store_id": "867",
                "store_name": "Scottsbluff Supercenter",
                "store_address": "3322 Avenue I, Scottsbluff, NE 69361",
            },
            {
                "id": "10294409-store-4303-walmart-com",
                "brand": "Karo",
                "product": "Light Corn Syrup 16 fl oz Bottle",
                "us_item_id": "10294409",
                "store_id": "4303",
                "store_name": "Miami NW 79th St Supercenter",
                "store_address": "3200 NW 79th St, Miami, FL 33147",
            },
            {
                "id": "21092535-store-4303-walmart-com",
                "brand": "Hershey's",
                "product": "Natural Unsweetened Cocoa Powder 8 oz Can",
                "us_item_id": "21092535",
                "store_id": "4303",
                "store_name": "Miami NW 79th St Supercenter",
                "store_address": "3200 NW 79th St, Miami, FL 33147",
            },
            {
                "id": "10818608-store-3709-walmart-com",
                "brand": "Kellogg's",
                "product": "Original Corn Flakes Breakfast Cereal Family Size 18 oz Box",
                "us_item_id": "10818608",
                "store_id": "3709",
                "store_name": "Atlanta Gresham Rd S E Supercenter",
                "store_address": "2427 Gresham Rd SE, Atlanta, GA 30316",
            },
            {
                "id": "10311527-store-3709-walmart-com",
                "brand": "Bisquick",
                "product": "Original Pancake and Baking Mix 40 oz Box",
                "us_item_id": "10311527",
                "store_id": "3709",
                "store_name": "Atlanta Gresham Rd S E Supercenter",
                "store_address": "2427 Gresham Rd SE, Atlanta, GA 30316",
            },
            {
                "id": "34632324-store-3371-walmart-com",
                "brand": "RITZ",
                "product": "Original Crackers 13.7 oz Box",
                "us_item_id": "34632324",
                "store_id": "3371",
                "store_name": "Charlotte Wilkinson Blvd Supercenter",
                "store_address": "3240 Wilkinson Blvd, Charlotte, NC 28208",
            },
            {
                "id": "10292621-store-3371-walmart-com",
                "brand": "Premium",
                "product": "Original Saltine Crackers 16 oz Box",
                "us_item_id": "10292621",
                "store_id": "3371",
                "store_name": "Charlotte Wilkinson Blvd Supercenter",
                "store_address": "3240 Wilkinson Blvd, Charlotte, NC 28208",
            },
            {
                "id": "10294707-store-3500-walmart-com",
                "brand": "Goldfish",
                "product": "Cheddar Baked Snack Crackers 6.6 oz Bag",
                "us_item_id": "10294707",
                "store_id": "3500",
                "store_name": "Houston E Sam Houston Pkwy Wallisville Rd Supercenter",
                "store_address": "5655 E Sam Houston Pkwy N, Houston, TX 77015",
            },
            {
                "id": "42124218-store-3500-walmart-com",
                "brand": "Cheez-It",
                "product": "Original Baked Snack Crackers 12.4 oz Box",
                "us_item_id": "42124218",
                "store_id": "3500",
                "store_name": "Houston E Sam Houston Pkwy Wallisville Rd Supercenter",
                "store_address": "5655 E Sam Houston Pkwy N, Houston, TX 77015",
            },
            {
                "id": "10291453-store-964-walmart-com",
                "brand": "Planters",
                "product": "Salted Dry Roasted Peanuts 16 oz Jar",
                "us_item_id": "10291453",
                "store_id": "964",
                "store_name": "El Paso Alameda Avenue Supercenter",
                "store_address": "9441 Alameda Ave, El Paso, TX 79907",
            },
            {
                "id": "19275995-store-964-walmart-com",
                "brand": "Pepsi",
                "product": "Cola Soda 12 fl oz Cans 15 Pack",
                "us_item_id": "19275995",
                "store_id": "964",
                "store_name": "El Paso Alameda Avenue Supercenter",
                "store_address": "9441 Alameda Ave, El Paso, TX 79907",
            },
            {
                "id": "10452492-store-1315-walmart-com",
                "brand": "Dr Pepper",
                "product": "Original Soda 12 fl oz Cans 12 Pack",
                "us_item_id": "10452492",
                "store_id": "1315",
                "store_name": "Cheyenne Dell Range Blvd Supercenter",
                "store_address": "2032 Dell Range Blvd, Cheyenne, WY 82009",
            },
            {
                "id": "10291611-store-1315-walmart-com",
                "brand": "Sprite",
                "product": "Lemon-Lime Soda 12 fl oz Cans 12 Pack",
                "us_item_id": "10291611",
                "store_id": "1315",
                "store_name": "Cheyenne Dell Range Blvd Supercenter",
                "store_address": "2032 Dell Range Blvd, Cheyenne, WY 82009",
            },
            {
                "id": "12166385-store-1872-walmart-com",
                "brand": "Ocean Spray",
                "product": "Cranberry Juice Cocktail 64 fl oz Bottle",
                "us_item_id": "12166385",
                "store_id": "1872",
                "store_name": "Helena Supercenter",
                "store_address": "2750 Prospect Ave, Helena, MT 59601",
            },
            {
                "id": "15570918-store-1872-walmart-com",
                "brand": "V8",
                "product": "Original 100% Vegetable Juice 64 fl oz Bottle",
                "us_item_id": "15570918",
                "store_id": "1872",
                "store_name": "Helena Supercenter",
                "store_address": "2750 Prospect Ave, Helena, MT 59601",
            },
            {
                "id": "597867208-store-2596-walmart-com",
                "brand": "Swiss Miss",
                "product": "Milk Chocolate Hot Cocoa Mix 1.38 oz Envelopes 8 Count",
                "us_item_id": "597867208",
                "store_id": "2596",
                "store_name": "Mount Vernon Supercenter",
                "store_address": "2301 Freeway Dr, Mount Vernon, WA 98273",
            },
        ],
        "params": lambda variables, requested_date: {
            "product_id": variables["us_item_id"],
            "store_id": variables["store_id"],
            "no_cache": "true",
        },
        "parse_response": parse_walmart_price,
    },
    "flight_departure_delay": {
        "question_template": (
            "Will the departure delay in minutes of flight {flight_id} ({origin} to "
            "{destination}), scheduled to depart on {resolution_date}, be greater "
            "than the median departure delay for the same flight and route over the 14 days "
            "ending on {forecast_due_date}, counting early and on-time departures as zero "
            "delay?"
        ),
        "question_background": (
            "The departure delay in minutes for flight {flight_id} ({origin} to {destination}), "
            "as reported by Google. Only flights that have departed or arrived qualify. "
            "Measurements require a single matching flight, route and scheduled local "
            "departure date; missing or ambiguous evidence makes the measurement unavailable. "
            "Early and on-time departures count as zero; positive delays are unchanged. "
            "Canceled flights and flights that have not yet departed do not qualify. "
            "Data collection runs around 00:00 UTC and collects available departed flights "
            "with scheduled local departure dates on or before the UTC date when each request "
            "starts. Each observation retains its scheduled local departure date. "
            "Missing data can be recovered only while Google makes "
            "them available. The latest valid measurements are used, including revisions. "
            "Each median uses all available observations in its 14-calendar-day window; "
            "one observation is sufficient. Missing days are omitted without replacement. "
            "For an even number of observations, the two middle values are averaged. "
            "The freeze median covers the 14 days before the UTC bank update, excluding that "
            "day; the resolution median covers the 14 days ending on the forecast due date, "
            "including it. Both use scheduled local departure dates. No separate "
            "forecast-due-date observation is required. Missing resolution-date data or an "
            "empty comparison "
            "window leaves the horizon unresolved and unscored until qualifying data arrive. "
            "See: {url}"
        ),
        "resolution_criteria": (
            "Uses only ForecastBench's saved SerpAPI google engine measurements "
            "as ground truth, following the background's rules. "
            "Resolves Yes if the exact resolution-date delay is strictly greater than the "
            "same flight/route's median over the 14 calendar days ending on the forecast due "
            "date; equal or lower resolves No. Dates are scheduled local departure dates."
        ),
        "engine": "google",
        # Record yesterday even when missing; also collect other available departed dates.
        "date_offset": -1,
        "url": "https://www.google.com/search?q={flight_id}+flight+status&hl=en&gl=us",
        # Daily winter/summer schedules checked 2026-10-03 at flight.info/{flight_id},
        # through at least 2027-08-31. Recheck future timetables as airlines revise them.
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
            {"id": "aa72-syd-lax", "flight_id": "AA72", "origin": "SYD", "destination": "LAX"},
            {"id": "ey1-auh-jfk", "flight_id": "EY1", "origin": "AUH", "destination": "JFK"},
            {"id": "nh102-hnd-iad", "flight_id": "NH102", "origin": "HND", "destination": "IAD"},
            {"id": "ua1-sfo-sin", "flight_id": "UA1", "origin": "SFO", "destination": "SIN"},
            {"id": "dl30-atl-lhr", "flight_id": "DL30", "origin": "ATL", "destination": "LHR"},
            {"id": "ba178-jfk-lhr", "flight_id": "BA178", "origin": "JFK", "destination": "LHR"},
            {"id": "kq100-nbo-lhr", "flight_id": "KQ100", "origin": "NBO", "destination": "LHR"},
            {"id": "et602-add-dxb", "flight_id": "ET602", "origin": "ADD", "destination": "DXB"},
            {"id": "cx253-hkg-lhr", "flight_id": "CX253", "origin": "HKG", "destination": "LHR"},
            {"id": "la2478-lim-lax", "flight_id": "LA2478", "origin": "LIM", "destination": "LAX"},
            {"id": "ba283-lhr-lax", "flight_id": "BA283", "origin": "LHR", "destination": "LAX"},
            {"id": "nh114-hnd-iah", "flight_id": "NH114", "origin": "HND", "destination": "IAH"},
            {"id": "ek1-dxb-lhr", "flight_id": "EK1", "origin": "DXB", "destination": "LHR"},
            {"id": "sq228-mel-sin", "flight_id": "SQ228", "origin": "MEL", "destination": "SIN"},
            {"id": "lh760-fra-del", "flight_id": "LH760", "origin": "FRA", "destination": "DEL"},
            {"id": "lh430-fra-ord", "flight_id": "LH430", "origin": "FRA", "destination": "ORD"},
            {"id": "lh454-fra-sfo", "flight_id": "LH454", "origin": "FRA", "destination": "SFO"},
            {"id": "lh456-fra-lax", "flight_id": "LH456", "origin": "FRA", "destination": "LAX"},
            {"id": "lx14-zrh-jfk", "flight_id": "LX14", "origin": "ZRH", "destination": "JFK"},
            {"id": "ek406-dxb-mel", "flight_id": "EK406", "origin": "DXB", "destination": "MEL"},
            {"id": "nh108-hnd-sfo", "flight_id": "NH108", "origin": "HND", "destination": "SFO"},
            {"id": "sq24-sin-jfk", "flight_id": "SQ24", "origin": "SIN", "destination": "JFK"},
            {"id": "sq322-sin-lhr", "flight_id": "SQ322", "origin": "SIN", "destination": "LHR"},
            {"id": "sq221-sin-syd", "flight_id": "SQ221", "origin": "SIN", "destination": "SYD"},
            {"id": "sq232-syd-sin", "flight_id": "SQ232", "origin": "SYD", "destination": "SIN"},
            {"id": "cx880-hkg-lax", "flight_id": "CX880", "origin": "HKG", "destination": "LAX"},
            {"id": "cx840-hkg-jfk", "flight_id": "CX840", "origin": "HKG", "destination": "JFK"},
            {"id": "nh110-hnd-jfk", "flight_id": "NH110", "origin": "HND", "destination": "JFK"},
            {"id": "nh106-hnd-lax", "flight_id": "NH106", "origin": "HND", "destination": "LAX"},
            {"id": "nh211-hnd-lhr", "flight_id": "NH211", "origin": "HND", "destination": "LHR"},
            {"id": "jl2-hnd-sfo", "flight_id": "JL2", "origin": "HND", "destination": "SFO"},
            {"id": "jl16-hnd-lax", "flight_id": "JL16", "origin": "HND", "destination": "LAX"},
            {"id": "jl10-hnd-ord", "flight_id": "JL10", "origin": "HND", "destination": "ORD"},
            {"id": "ke81-icn-jfk", "flight_id": "KE81", "origin": "ICN", "destination": "JFK"},
            {"id": "ke11-icn-lax", "flight_id": "KE11", "origin": "ICN", "destination": "LAX"},
            {"id": "la8084-gru-lhr", "flight_id": "LA8084", "origin": "GRU", "destination": "LHR"},
            {"id": "br32-tpe-jfk", "flight_id": "BR32", "origin": "TPE", "destination": "JFK"},
            {"id": "ci8-tpe-lax", "flight_id": "CI8", "origin": "TPE", "destination": "LAX"},
            {"id": "tk1-ist-jfk", "flight_id": "TK1", "origin": "IST", "destination": "JFK"},
            {"id": "ek215-dxb-lax", "flight_id": "EK215", "origin": "DXB", "destination": "LAX"},
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
