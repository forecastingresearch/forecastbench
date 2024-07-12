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
    {"id": "61970", "station": "Juan de Nova Island"},
    {"id": "61972", "station": "Europa Island"},
    {"id": "61976", "station": "Tromelin Island"},
    {"id": "61980", "station": "Roland Garros Airport"},
    {"id": "61996", "station": "Amsterdam Island"},
    {"id": "61997", "station": "Île de la Possession"},
    {"id": "67005", "station": "Pamandzi"},
    {"id": "71805", "station": "Saint-Pierre"},
    {"id": "78890", "station": "La Désirade"},
    {"id": "78894", "station": "Saint Barthélemy"},
    {"id": "78897", "station": "Pointe-à-Pitre International Airport"},
    {"id": "78925", "station": "Martinique Aimé Césaire International Airport"},
    {"id": "81401", "station": "Saint-Laurent"},
    {"id": "81405", "station": "Cayenne – Félix Éboué Airport"},
]

ENTSO_E_GENERATION_SOURCES = [
    {"country_source_id": "AT.B01.D", "energy_source": "biomass", "country_name": "Austria"},
    {
        "country_source_id": "AT.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Austria",
    },
    {
        "country_source_id": "AT.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Austria",
    },
    {
        "country_source_id": "AT.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Austria",
    },
    {"country_source_id": "AT.B16.D", "energy_source": "solar", "country_name": "Austria"},
    {"country_source_id": "AT.B17.D", "energy_source": "waste", "country_name": "Austria"},
    {
        "country_source_id": "AT.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Austria",
    },
    {"country_source_id": "AT.B20.D", "energy_source": "other", "country_name": "Austria"},
    {"country_source_id": "BE.B01.D", "energy_source": "biomass", "country_name": "Belgium"},
    {"country_source_id": "BE.B04.D", "energy_source": "fossil gas", "country_name": "Belgium"},
    {
        "country_source_id": "BE.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Belgium",
    },
    {
        "country_source_id": "BE.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Belgium",
    },
    {"country_source_id": "BE.B14.D", "energy_source": "nuclear", "country_name": "Belgium"},
    {"country_source_id": "BE.B16.D", "energy_source": "solar", "country_name": "Belgium"},
    {"country_source_id": "BE.B17.D", "energy_source": "waste", "country_name": "Belgium"},
    {
        "country_source_id": "BE.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Belgium",
    },
    {
        "country_source_id": "BE.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Belgium",
    },
    {"country_source_id": "BE.B20.D", "energy_source": "other", "country_name": "Belgium"},
    {"country_source_id": "BG.B01.D", "energy_source": "biomass", "country_name": "Bulgaria"},
    {
        "country_source_id": "BG.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Bulgaria",
    },
    {
        "country_source_id": "BG.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Bulgaria",
    },
    {
        "country_source_id": "BG.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Bulgaria",
    },
    {
        "country_source_id": "BG.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Bulgaria",
    },
    {
        "country_source_id": "BG.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Bulgaria",
    },
    {"country_source_id": "BG.B14.D", "energy_source": "nuclear", "country_name": "Bulgaria"},
    {"country_source_id": "BG.B16.D", "energy_source": "solar", "country_name": "Bulgaria"},
    {"country_source_id": "BG.B17.D", "energy_source": "waste", "country_name": "Bulgaria"},
    {
        "country_source_id": "BG.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Bulgaria",
    },
    {
        "country_source_id": "CH.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Switzerland",
    },
    {
        "country_source_id": "CH.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Switzerland",
    },
    {
        "country_source_id": "CH.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Switzerland",
    },
    {
        "country_source_id": "CH.B14.D",
        "energy_source": "nuclear",
        "country_name": "Switzerland",
    },
    {"country_source_id": "CH.B16.D", "energy_source": "solar", "country_name": "Switzerland"},
    {"country_source_id": "CZ.B01.D", "energy_source": "biomass", "country_name": "Czechia"},
    {
        "country_source_id": "CZ.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Czechia",
    },
    {"country_source_id": "CZ.B04.D", "energy_source": "fossil gas", "country_name": "Czechia"},
    {
        "country_source_id": "CZ.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Czechia",
    },
    {
        "country_source_id": "CZ.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Czechia",
    },
    {
        "country_source_id": "CZ.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Czechia",
    },
    {"country_source_id": "CZ.B14.D", "energy_source": "nuclear", "country_name": "Czechia"},
    {
        "country_source_id": "CZ.B15.D",
        "energy_source": "other renewable",
        "country_name": "Czechia",
    },
    {"country_source_id": "CZ.B16.D", "energy_source": "solar", "country_name": "Czechia"},
    {"country_source_id": "CZ.B17.D", "energy_source": "waste", "country_name": "Czechia"},
    {
        "country_source_id": "CZ.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Czechia",
    },
    {"country_source_id": "CZ.B20.D", "energy_source": "other", "country_name": "Czechia"},
    {"country_source_id": "DE.B01.D", "energy_source": "biomass", "country_name": "Germany"},
    {
        "country_source_id": "DE.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Germany",
    },
    {"country_source_id": "DE.B04.D", "energy_source": "fossil gas", "country_name": "Germany"},
    {
        "country_source_id": "DE.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Germany",
    },
    {"country_source_id": "DE.B06.D", "energy_source": "fossil oil", "country_name": "Germany"},
    {"country_source_id": "DE.B09.D", "energy_source": "geothermal", "country_name": "Germany"},
    {
        "country_source_id": "DE.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Germany",
    },
    {
        "country_source_id": "DE.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Germany",
    },
    {
        "country_source_id": "DE.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Germany",
    },
    {
        "country_source_id": "DE.B15.D",
        "energy_source": "other renewable",
        "country_name": "Germany",
    },
    {"country_source_id": "DE.B16.D", "energy_source": "solar", "country_name": "Germany"},
    {"country_source_id": "DE.B17.D", "energy_source": "waste", "country_name": "Germany"},
    {
        "country_source_id": "DE.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Germany",
    },
    {
        "country_source_id": "DE.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Germany",
    },
    {"country_source_id": "DE.B20.D", "energy_source": "other", "country_name": "Germany"},
    {"country_source_id": "DK.B01.D", "energy_source": "biomass", "country_name": "Denmark"},
    {"country_source_id": "DK.B04.D", "energy_source": "fossil gas", "country_name": "Denmark"},
    {
        "country_source_id": "DK.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Denmark",
    },
    {"country_source_id": "DK.B06.D", "energy_source": "fossil oil", "country_name": "Denmark"},
    {"country_source_id": "DK.B16.D", "energy_source": "solar", "country_name": "Denmark"},
    {"country_source_id": "DK.B17.D", "energy_source": "waste", "country_name": "Denmark"},
    {
        "country_source_id": "DK.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Denmark",
    },
    {
        "country_source_id": "DK.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Denmark",
    },
    {"country_source_id": "EE.B01.D", "energy_source": "biomass", "country_name": "Estonia"},
    {
        "country_source_id": "EE.B03.D",
        "energy_source": "fossil coal-derived gas",
        "country_name": "Estonia",
    },
    {"country_source_id": "EE.B04.D", "energy_source": "fossil gas", "country_name": "Estonia"},
    {
        "country_source_id": "EE.B07.D",
        "energy_source": "fossil oil shale",
        "country_name": "Estonia",
    },
    {"country_source_id": "EE.B16.D", "energy_source": "solar", "country_name": "Estonia"},
    {"country_source_id": "EE.B17.D", "energy_source": "waste", "country_name": "Estonia"},
    {
        "country_source_id": "EE.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Estonia",
    },
    {"country_source_id": "ES.B01.D", "energy_source": "biomass", "country_name": "Spain"},
    {"country_source_id": "ES.B04.D", "energy_source": "fossil gas", "country_name": "Spain"},
    {
        "country_source_id": "ES.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Spain",
    },
    {"country_source_id": "ES.B06.D", "energy_source": "fossil oil", "country_name": "Spain"},
    {"country_source_id": "ES.B08.D", "energy_source": "fossil peat", "country_name": "Spain"},
    {
        "country_source_id": "ES.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Spain",
    },
    {
        "country_source_id": "ES.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Spain",
    },
    {
        "country_source_id": "ES.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Spain",
    },
    {"country_source_id": "ES.B14.D", "energy_source": "nuclear", "country_name": "Spain"},
    {
        "country_source_id": "ES.B15.D",
        "energy_source": "other renewable",
        "country_name": "Spain",
    },
    {"country_source_id": "ES.B16.D", "energy_source": "solar", "country_name": "Spain"},
    {"country_source_id": "ES.B17.D", "energy_source": "waste", "country_name": "Spain"},
    {"country_source_id": "ES.B19.D", "energy_source": "wind onshore", "country_name": "Spain"},
    {"country_source_id": "ES.B20.D", "energy_source": "other", "country_name": "Spain"},
    {"country_source_id": "FI.B01.D", "energy_source": "biomass", "country_name": "Finland"},
    {"country_source_id": "FI.B04.D", "energy_source": "fossil gas", "country_name": "Finland"},
    {
        "country_source_id": "FI.B08.D",
        "energy_source": "fossil peat",
        "country_name": "Finland",
    },
    {
        "country_source_id": "FI.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Finland",
    },
    {"country_source_id": "FI.B14.D", "energy_source": "nuclear", "country_name": "Finland"},
    {"country_source_id": "FI.B16.D", "energy_source": "solar", "country_name": "Finland"},
    {"country_source_id": "FI.B17.D", "energy_source": "waste", "country_name": "Finland"},
    {
        "country_source_id": "FI.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Finland",
    },
    {"country_source_id": "FI.B20.D", "energy_source": "other", "country_name": "Finland"},
    {"country_source_id": "FR.B01.D", "energy_source": "biomass", "country_name": "France"},
    {"country_source_id": "FR.B04.D", "energy_source": "fossil gas", "country_name": "France"},
    {"country_source_id": "FR.B06.D", "energy_source": "fossil oil", "country_name": "France"},
    {
        "country_source_id": "FR.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "France",
    },
    {
        "country_source_id": "FR.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "France",
    },
    {
        "country_source_id": "FR.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "France",
    },
    {"country_source_id": "FR.B14.D", "energy_source": "nuclear", "country_name": "France"},
    {"country_source_id": "FR.B16.D", "energy_source": "solar", "country_name": "France"},
    {"country_source_id": "FR.B17.D", "energy_source": "waste", "country_name": "France"},
    {
        "country_source_id": "FR.B18.D",
        "energy_source": "wind offshore",
        "country_name": "France",
    },
    {
        "country_source_id": "FR.B19.D",
        "energy_source": "wind onshore",
        "country_name": "France",
    },
    {"country_source_id": "GR.B04.D", "energy_source": "fossil gas", "country_name": "Greece"},
    {
        "country_source_id": "GR.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Greece",
    },
    {
        "country_source_id": "GR.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Greece",
    },
    {"country_source_id": "GR.B16.D", "energy_source": "solar", "country_name": "Greece"},
    {
        "country_source_id": "GR.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Greece",
    },
    {"country_source_id": "HR.B01.D", "energy_source": "biomass", "country_name": "Croatia"},
    {"country_source_id": "HR.B04.D", "energy_source": "fossil gas", "country_name": "Croatia"},
    {
        "country_source_id": "HR.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Croatia",
    },
    {
        "country_source_id": "HR.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Croatia",
    },
    {
        "country_source_id": "HR.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Croatia",
    },
    {
        "country_source_id": "HR.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Croatia",
    },
    {
        "country_source_id": "HR.B15.D",
        "energy_source": "other renewable",
        "country_name": "Croatia",
    },
    {"country_source_id": "HR.B16.D", "energy_source": "solar", "country_name": "Croatia"},
    {"country_source_id": "HR.B17.D", "energy_source": "waste", "country_name": "Croatia"},
    {
        "country_source_id": "HR.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Croatia",
    },
    {"country_source_id": "HU.B01.D", "energy_source": "biomass", "country_name": "Hungary"},
    {
        "country_source_id": "HU.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Hungary",
    },
    {"country_source_id": "HU.B04.D", "energy_source": "fossil gas", "country_name": "Hungary"},
    {
        "country_source_id": "HU.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Hungary",
    },
    {
        "country_source_id": "HU.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Hungary",
    },
    {
        "country_source_id": "HU.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Hungary",
    },
    {"country_source_id": "HU.B14.D", "energy_source": "nuclear", "country_name": "Hungary"},
    {
        "country_source_id": "HU.B15.D",
        "energy_source": "other renewable",
        "country_name": "Hungary",
    },
    {"country_source_id": "HU.B16.D", "energy_source": "solar", "country_name": "Hungary"},
    {"country_source_id": "HU.B17.D", "energy_source": "waste", "country_name": "Hungary"},
    {
        "country_source_id": "HU.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Hungary",
    },
    {"country_source_id": "HU.B20.D", "energy_source": "other", "country_name": "Hungary"},
    {"country_source_id": "IE.B04.D", "energy_source": "fossil gas", "country_name": "Ireland"},
    {
        "country_source_id": "IE.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Ireland",
    },
    {"country_source_id": "IE.B06.D", "energy_source": "fossil oil", "country_name": "Ireland"},
    {
        "country_source_id": "IE.B08.D",
        "energy_source": "fossil peat",
        "country_name": "Ireland",
    },
    {
        "country_source_id": "IE.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Ireland",
    },
    {
        "country_source_id": "IE.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Ireland",
    },
    {
        "country_source_id": "IE.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Ireland",
    },
    {"country_source_id": "IE.B20.D", "energy_source": "other", "country_name": "Ireland"},
    {"country_source_id": "IT.B01.D", "energy_source": "biomass", "country_name": "Italy"},
    {
        "country_source_id": "IT.B03.D",
        "energy_source": "fossil coal-derived gas",
        "country_name": "Italy",
    },
    {"country_source_id": "IT.B04.D", "energy_source": "fossil gas", "country_name": "Italy"},
    {
        "country_source_id": "IT.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Italy",
    },
    {"country_source_id": "IT.B06.D", "energy_source": "fossil oil", "country_name": "Italy"},
    {"country_source_id": "IT.B09.D", "energy_source": "geothermal", "country_name": "Italy"},
    {
        "country_source_id": "IT.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Italy",
    },
    {
        "country_source_id": "IT.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Italy",
    },
    {
        "country_source_id": "IT.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Italy",
    },
    {"country_source_id": "IT.B16.D", "energy_source": "solar", "country_name": "Italy"},
    {"country_source_id": "IT.B17.D", "energy_source": "waste", "country_name": "Italy"},
    {
        "country_source_id": "IT.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Italy",
    },
    {"country_source_id": "IT.B19.D", "energy_source": "wind onshore", "country_name": "Italy"},
    {"country_source_id": "IT.B20.D", "energy_source": "other", "country_name": "Italy"},
    {"country_source_id": "LT.B01.D", "energy_source": "biomass", "country_name": "Lithuania"},
    {
        "country_source_id": "LT.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Lithuania",
    },
    {
        "country_source_id": "LT.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Lithuania",
    },
    {
        "country_source_id": "LT.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Lithuania",
    },
    {"country_source_id": "LT.B16.D", "energy_source": "solar", "country_name": "Lithuania"},
    {"country_source_id": "LT.B17.D", "energy_source": "waste", "country_name": "Lithuania"},
    {
        "country_source_id": "LT.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Lithuania",
    },
    {"country_source_id": "LT.B20.D", "energy_source": "other", "country_name": "Lithuania"},
    {"country_source_id": "LU.B01.D", "energy_source": "biomass", "country_name": "Luxembourg"},
    {
        "country_source_id": "LU.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Luxembourg",
    },
    {
        "country_source_id": "LU.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Luxembourg",
    },
    {
        "country_source_id": "LU.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Luxembourg",
    },
    {"country_source_id": "LU.B16.D", "energy_source": "solar", "country_name": "Luxembourg"},
    {"country_source_id": "LU.B17.D", "energy_source": "waste", "country_name": "Luxembourg"},
    {
        "country_source_id": "LU.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Luxembourg",
    },
    {"country_source_id": "LV.B01.D", "energy_source": "biomass", "country_name": "Latvia"},
    {"country_source_id": "LV.B04.D", "energy_source": "fossil gas", "country_name": "Latvia"},
    {
        "country_source_id": "LV.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Latvia",
    },
    {"country_source_id": "LV.B16.D", "energy_source": "solar", "country_name": "Latvia"},
    {
        "country_source_id": "LV.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Latvia",
    },
    {
        "country_source_id": "MD.B01.D",
        "energy_source": "biomass",
        "country_name": "Moldova, Republic of",
    },
    {
        "country_source_id": "MD.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Moldova, Republic of",
    },
    {
        "country_source_id": "MD.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Moldova, Republic of",
    },
    {
        "country_source_id": "MD.B16.D",
        "energy_source": "solar",
        "country_name": "Moldova, Republic of",
    },
    {
        "country_source_id": "MD.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Moldova, Republic of",
    },
    {
        "country_source_id": "ME.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Montenegro",
    },
    {
        "country_source_id": "ME.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Montenegro",
    },
    {
        "country_source_id": "MK.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "North Macedonia",
    },
    {
        "country_source_id": "MK.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "North Macedonia",
    },
    {
        "country_source_id": "MK.B19.D",
        "energy_source": "wind onshore",
        "country_name": "North Macedonia",
    },
    {
        "country_source_id": "NL.B01.D",
        "energy_source": "biomass",
        "country_name": "Netherlands",
    },
    {
        "country_source_id": "NL.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Netherlands",
    },
    {
        "country_source_id": "NL.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Netherlands",
    },
    {"country_source_id": "NL.B16.D", "energy_source": "solar", "country_name": "Netherlands"},
    {"country_source_id": "NL.B17.D", "energy_source": "waste", "country_name": "Netherlands"},
    {
        "country_source_id": "NL.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Netherlands",
    },
    {
        "country_source_id": "NL.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Netherlands",
    },
    {"country_source_id": "NL.B20.D", "energy_source": "other", "country_name": "Netherlands"},
    {"country_source_id": "NO.B04.D", "energy_source": "fossil gas", "country_name": "Norway"},
    {
        "country_source_id": "NO.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Norway",
    },
    {
        "country_source_id": "NO.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Norway",
    },
    {
        "country_source_id": "NO.B15.D",
        "energy_source": "other renewable",
        "country_name": "Norway",
    },
    {"country_source_id": "NO.B16.D", "energy_source": "solar", "country_name": "Norway"},
    {"country_source_id": "NO.B17.D", "energy_source": "waste", "country_name": "Norway"},
    {
        "country_source_id": "NO.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Norway",
    },
    {"country_source_id": "PL.B01.D", "energy_source": "biomass", "country_name": "Poland"},
    {
        "country_source_id": "PL.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Poland",
    },
    {
        "country_source_id": "PL.B03.D",
        "energy_source": "fossil coal-derived gas",
        "country_name": "Poland",
    },
    {"country_source_id": "PL.B04.D", "energy_source": "fossil gas", "country_name": "Poland"},
    {
        "country_source_id": "PL.B05.D",
        "energy_source": "fossil hard coal",
        "country_name": "Poland",
    },
    {"country_source_id": "PL.B06.D", "energy_source": "fossil oil", "country_name": "Poland"},
    {
        "country_source_id": "PL.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Poland",
    },
    {
        "country_source_id": "PL.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Poland",
    },
    {
        "country_source_id": "PL.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Poland",
    },
    {"country_source_id": "PL.B16.D", "energy_source": "solar", "country_name": "Poland"},
    {
        "country_source_id": "PL.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Poland",
    },
    {"country_source_id": "PL.B20.D", "energy_source": "other", "country_name": "Poland"},
    {"country_source_id": "PT.B01.D", "energy_source": "biomass", "country_name": "Portugal"},
    {
        "country_source_id": "PT.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Portugal",
    },
    {
        "country_source_id": "PT.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Portugal",
    },
    {
        "country_source_id": "PT.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Portugal",
    },
    {
        "country_source_id": "PT.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Portugal",
    },
    {"country_source_id": "PT.B16.D", "energy_source": "solar", "country_name": "Portugal"},
    {
        "country_source_id": "PT.B18.D",
        "energy_source": "wind offshore",
        "country_name": "Portugal",
    },
    {
        "country_source_id": "PT.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Portugal",
    },
    {"country_source_id": "PT.B20.D", "energy_source": "other", "country_name": "Portugal"},
    {"country_source_id": "RO.B01.D", "energy_source": "biomass", "country_name": "Romania"},
    {
        "country_source_id": "RO.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Romania",
    },
    {"country_source_id": "RO.B04.D", "energy_source": "fossil gas", "country_name": "Romania"},
    {
        "country_source_id": "RO.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Romania",
    },
    {
        "country_source_id": "RO.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Romania",
    },
    {"country_source_id": "RO.B14.D", "energy_source": "nuclear", "country_name": "Romania"},
    {"country_source_id": "RO.B16.D", "energy_source": "solar", "country_name": "Romania"},
    {
        "country_source_id": "RO.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Romania",
    },
    {"country_source_id": "RS.B01.D", "energy_source": "biomass", "country_name": "Serbia"},
    {
        "country_source_id": "RS.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Serbia",
    },
    {"country_source_id": "RS.B04.D", "energy_source": "fossil gas", "country_name": "Serbia"},
    {
        "country_source_id": "RS.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Serbia",
    },
    {
        "country_source_id": "RS.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Serbia",
    },
    {
        "country_source_id": "RS.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Serbia",
    },
    {"country_source_id": "RS.B20.D", "energy_source": "other", "country_name": "Serbia"},
    {"country_source_id": "SE.B04.D", "energy_source": "fossil gas", "country_name": "Sweden"},
    {
        "country_source_id": "SE.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Sweden",
    },
    {"country_source_id": "SE.B14.D", "energy_source": "nuclear", "country_name": "Sweden"},
    {"country_source_id": "SE.B16.D", "energy_source": "solar", "country_name": "Sweden"},
    {
        "country_source_id": "SE.B19.D",
        "energy_source": "wind onshore",
        "country_name": "Sweden",
    },
    {"country_source_id": "SE.B20.D", "energy_source": "other", "country_name": "Sweden"},
    {"country_source_id": "SI.B01.D", "energy_source": "biomass", "country_name": "Slovenia"},
    {
        "country_source_id": "SI.B02.D",
        "energy_source": "fossil brown coal/lignite",
        "country_name": "Slovenia",
    },
    {
        "country_source_id": "SI.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Slovenia",
    },
    {
        "country_source_id": "SI.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Slovenia",
    },
    {
        "country_source_id": "SI.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Slovenia",
    },
    {"country_source_id": "SI.B14.D", "energy_source": "nuclear", "country_name": "Slovenia"},
    {"country_source_id": "SI.B16.D", "energy_source": "solar", "country_name": "Slovenia"},
    {"country_source_id": "SI.B17.D", "energy_source": "waste", "country_name": "Slovenia"},
    {"country_source_id": "SK.B01.D", "energy_source": "biomass", "country_name": "Slovakia"},
    {
        "country_source_id": "SK.B04.D",
        "energy_source": "fossil gas",
        "country_name": "Slovakia",
    },
    {
        "country_source_id": "SK.B06.D",
        "energy_source": "fossil oil",
        "country_name": "Slovakia",
    },
    {
        "country_source_id": "SK.B10.D",
        "energy_source": "hydro pumped storage",
        "country_name": "Slovakia",
    },
    {
        "country_source_id": "SK.B11.D",
        "energy_source": "hydro run-of-river and poundage",
        "country_name": "Slovakia",
    },
    {
        "country_source_id": "SK.B12.D",
        "energy_source": "hydro water reservoir",
        "country_name": "Slovakia",
    },
    {"country_source_id": "SK.B14.D", "energy_source": "nuclear", "country_name": "Slovakia"},
    {
        "country_source_id": "SK.B15.D",
        "energy_source": "other renewable",
        "country_name": "Slovakia",
    },
    {"country_source_id": "SK.B16.D", "energy_source": "solar", "country_name": "Slovakia"},
    {"country_source_id": "SK.B20.D", "energy_source": "other", "country_name": "Slovakia"},
]

QUESTION_TEMPLATES = {
    "meteofrance": (
        "What is the probability that the daily average temperature at the French weather station "
        "at {station} will be higher on {resolution_date} than on {forecast_due_date}?"
    ),
    "ENTSO_E": (
        "What is the probability that the actual daily electricity generated from {energy_source} in "
        "{country_name} will be higher on the date of resolution listed below than on {forecast_due_date}?"
    ),
}

VALUE_EXPLANATIONS = {
    "meteofrance": "The daily average temperature at the French weather station at {station}.",
    "ENTSO_E": "The actual realised daily electricity generated from {energy_source} in {country_name}.",
}


def create_meteofrance_constants(STATIONS):
    """Convert PRE-CONSTANTS Meteo-France data to format expected by fetch and update_questions functions."""
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


def create_entso_e_constants(GENERATION_SOURCES):
    """Convert PRE-CONSTANTS ENTSOE data to format expected by fetch and update_questions functions."""
    constants = []
    for item in GENERATION_SOURCES:
        id = item["country_source_id"]
        energy_source = item["energy_source"]
        country_name = item["country_name"]
        question_text = (
            QUESTION_TEMPLATES["ENTSO_E"]
            .replace("{energy_source}", energy_source)
            .replace("{country_name}", country_name)
        )
        explanation = (
            VALUE_EXPLANATIONS["ENTSO_E"]
            .replace("{energy_source}", energy_source)
            .replace("{country_name}", country_name)
        )
        new_entry = {
            "id": f"ENTSOE/AGPT/{id}",
            "question_text": question_text,
            "freeze_datetime_value_explanation": explanation,
        }
        constants.append(new_entry)
    return constants


CONSTANTS = create_meteofrance_constants(METEOFRANCE_STATIONS)
CONSTANTS += create_entso_e_constants(ENTSO_E_GENERATION_SOURCES)
