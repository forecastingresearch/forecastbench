"""DBnomics-specific variables."""

FETCH_COLUMN_DTYPE = {
    "id": str,
    "period": str,
    "value": str,
    "question_text": str,
    "value_at_freeze_datetime_explanation": str,
}
FETCH_COLUMNS = list(FETCH_COLUMN_DTYPE.keys())

SOURCE_INTRO = (
    "DBnomics collects data on topics such as population and living conditions, "
    "environment and energy, agriculture, finance, trade and others from publicly available "
    "resources, for example national and international statistical institutions, researchers and "
    "private companies. You're going to predict how questions based on this data will resolve."
)

RESOLUTION_CRITERIA = "Resolves to the value found at {url} once the data is published."

METEOFRANCE_STATIONS = [
    {"id": "07005", "station": "Abbeville"},
    {"id": "07015", "station": "Lille Airport"},
    {"id": "07020", "station": "Pointe De La Hague"},
    {"id": "07027", "station": "Caen – Carpiquet Airport"},
    {"id": "07037", "station": "Rouen Airport"},
    {"id": "07072", "station": "Reims – Prunay Aerodrome"},
    {"id": "07110", "station": "Brest Bretagne Airport"},
    {"id": "07117", "station": "Ploumanac'h"},
    {"id": "07130", "station": "Rennes–Saint-Jacques Airport"},
    {"id": "07139", "station": "Alençon"},
    {"id": "07149", "station": "Orly"},
    {"id": "07168", "station": "Troyes-Barberey Airport"},
    {"id": "07181", "station": "Nancy – Ochey Air Base"},
    {"id": "07190", "station": "Strasbourg Airport"},
    {"id": "07222", "station": "Nantes Atlantique Airport"},
    {"id": "07240", "station": "Tours"},
    {"id": "07255", "station": "Bourges"},
    {"id": "07280", "station": "Dijon-Bourgogne Airport"},
    {"id": "07299", "station": "EuroAirport Basel Mulhouse Freiburg"},
    {"id": "07335", "station": "Poitiers–Biard Airport"},
    {"id": "07434", "station": "Limoges – Bellegarde Airport"},
    {"id": "07460", "station": "Clermont-Ferrand Auvergne Airport"},
    {"id": "07471", "station": "Le Puy – Loudes Airport"},
    {"id": "07481", "station": "Lyon–Saint Exupéry Airport"},
    {"id": "07510", "station": "Bordeaux–Mérignac Airport"},
    {"id": "07535", "station": "Gourdon"},
    {"id": "07558", "station": "Millau"},
    {"id": "07577", "station": "Montélimar"},
    {"id": "07591", "station": "Embrun"},
    {"id": "07607", "station": "Mont-de-Marsan"},
    {"id": "07621", "station": "Tarbes–Lourdes–Pyrénées Airport"},
    {"id": "07627", "station": "Saint-Girons"},
    {"id": "07630", "station": "Toulouse–Blagnac Airport"},
    {"id": "07650", "station": "Marignane"},
    {"id": "07690", "station": "Nice"},
    {"id": "07747", "station": "Perpignan"},
    {"id": "07761", "station": "Ajaccio"},
    {"id": "61968", "station": "Glorioso Islands"},
    {"id": "61970", "station": "Juan de Nova Island"},
    {"id": "61972", "station": "Europa Island"},
    {"id": "61976", "station": "Tromelin Island"},
    {"id": "61980", "station": "Roland Garros Airport"},
    {"id": "61996", "station": "Amsterdam Island"},
    {"id": "61997", "station": "Île de la Possession"},
    {"id": "61998", "station": "Grande Terre"},
    {"id": "67005", "station": "Pamandzi"},
    {"id": "71805", "station": "Saint-Pierre"},
    {"id": "78890", "station": "La Désirade"},
    {"id": "78894", "station": "Saint Barthélemy"},
    {"id": "78897", "station": "Pointe-à-Pitre International Airport"},
    {"id": "78925", "station": "Martinique Aimé Césaire International Airport"},
    {"id": "81401", "station": "Saint-Laurent"},
    {"id": "81405", "station": "Cayenne – Félix Éboué Airport"},
]

QUESTION_TEMPLATES = {
    "meteofrance": (
        "What is the probability that the daily average temperature at the French weather station "
        "at {station} will be higher on {resolution_date} than on {forecast_due_date}?"
    )
}

VALUE_EXPLANATIONS = {
    "meteofrance": "The daily average temperature at the French weather station at {station}."
}


def create_meteofrance_constants(STATIONS):
    """Convert PRE-CONSTANTS data to format expected by fetch and update_questions functions."""
    constants = []
    for item in STATIONS:
        id = item["id"]
        station = item["station"]
        question_text = QUESTION_TEMPLATES["meteofrance"].replace("{station}", station)
        explanation = VALUE_EXPLANATIONS["meteofrance"].format(station=station)
        new_entry = {
            "id": f"meteofrance/TEMPERATURE/celsius.{id}.D",
            "question_text": question_text,
            "freeze_datetime_value_explanation": explanation,
        }
        constants.append(new_entry)
    return constants


CONSTANTS = create_meteofrance_constants(METEOFRANCE_STATIONS)
