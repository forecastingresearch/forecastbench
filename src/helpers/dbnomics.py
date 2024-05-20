"""DBnomics series."""

METEOFRANCE_STATIONS = [
    {"id": "07005", "station": "ABBEVILLE"},
    {"id": "07015", "station": "LILLE-LESQUIN"},
    {"id": "07020", "station": "PTE DE LA HAGUE"},
    {"id": "07027", "station": "CAEN-CARPIQUET"},
    {"id": "07037", "station": "ROUEN-BOOS"},
    {"id": "07072", "station": "REIMS-PRUNAY"},
    {"id": "07110", "station": "BREST-GUIPAVAS"},
    {"id": "07117", "station": "PLOUMANAC'H"},
    {"id": "07130", "station": "RENNES-ST JACQUES"},
    {"id": "07139", "station": "ALENCON"},
    {"id": "07149", "station": "ORLY"},
    {"id": "07168", "station": "TROYES-BARBEREY"},
    {"id": "07181", "station": "NANCY-OCHEY"},
    {"id": "07190", "station": "STRASBOURG-ENTZHEIM"},
    {"id": "07222", "station": "NANTES-BOUGUENAIS"},
    {"id": "07240", "station": "TOURS"},
    {"id": "07255", "station": "BOURGES"},
    {"id": "07280", "station": "DIJON-LONGVIC"},
    {"id": "07299", "station": "BALE-MULHOUSE"},
    {"id": "07335", "station": "POITIERS-BIARD"},
    {"id": "07434", "station": "LIMOGES-BELLEGARDE"},
    {"id": "07460", "station": "CLERMONT-FD"},
    {"id": "07471", "station": "LE PUY-LOUDES"},
    {"id": "07481", "station": "LYON-ST EXUPERY"},
    {"id": "07510", "station": "BORDEAUX-MERIGNAC"},
    {"id": "07535", "station": "GOURDON"},
    {"id": "07558", "station": "MILLAU"},
    {"id": "07577", "station": "MONTELIMAR"},
    {"id": "07591", "station": "EMBRUN"},
    {"id": "07607", "station": "MONT-DE-MARSAN"},
    {"id": "07621", "station": "TARBES-OSSUN"},
    {"id": "07627", "station": "ST GIRONS"},
    {"id": "07630", "station": "TOULOUSE-BLAGNAC"},
    {"id": "07650", "station": "MARIGNANE"},
    {"id": "07690", "station": "NICE"},
    {"id": "07747", "station": "PERPIGNAN"},
    {"id": "07761", "station": "AJACCIO"},
    {"id": "61968", "station": "GLORIEUSES"},
    {"id": "61970", "station": "JUAN DE NOVA"},
    {"id": "61972", "station": "EUROPA"},
    {"id": "61976", "station": "TROMELIN"},
    {"id": "61980", "station": "GILLOT-AEROPORT"},
    {"id": "61996", "station": "NOUVELLE AMSTERDAM"},
    {"id": "61997", "station": "CROZET"},
    {"id": "67005", "station": "PAMANDZI"},
    {"id": "71805", "station": "ST-PIERRE"},
    {"id": "78890", "station": "LA DESIRADE METEO"},
    {"id": "78894", "station": "ST-BARTHELEMY METEO"},
    {"id": "78897", "station": "LE RAIZET AERO"},
    {"id": "78925", "station": "LAMENTIN-AERO"},
    {"id": "81401", "station": "SAINT LAURENT"},
    {"id": "81405", "station": "CAYENNE-MATOURY"},
]

QUESTION_TEMPLATES = {
    "meteofrance": (
        "What is the probability that the daily average temperature at the French weather station "
        "at {station} will be higher on the resolution date listed than on the forecast due date?"
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
        question_text = QUESTION_TEMPLATES["meteofrance"].format(station=station)
        explanation = VALUE_EXPLANATIONS["meteofrance"].format(station=station)
        new_entry = {
            "id": f"meteofrance/TEMPERATURE/celsius.{id}.D",
            "question_text": question_text,
            "freeze_datetime_value_explanation": explanation,
        }
        constants.append(new_entry)
    return constants


CONSTANTS = create_meteofrance_constants(METEOFRANCE_STATIONS)
