"""DBnomics predefined series catalogues.

Only consumed by ``sources.dbnomics``; kept in a separate file purely for maintainability.
"""

# flake8: noqa: B950

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


ECB_QUESTIONS = [
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_FXN.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from foreign exchange "
        "market subindex",
    },
    {
        "id": "ECB/CISS/D.IE.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Ireland – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_BMN.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from bond market subindex",
    },
    {
        "id": "ECB/CISS/D.IT.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Italy – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.FI.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Finland – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.FI.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Finland – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_MMN.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from money market subindex",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – New Composite Indicator of Systemic "
        "Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.GB.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "United Kingdom – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.AT.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Austria – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.DE.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Germany – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.IT.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Italy – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.DE.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Germany – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.GR.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Greece – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.FR.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "France – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Correlation and real GDP-country weights "
        "average",
    },
    {
        "id": "ECB/CISS/D.AT.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Austria – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.BE.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Belgium – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_CON.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from cross-subindex "
        "correlations",
    },
    {
        "id": "ECB/CISS/D.BE.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Belgium – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.PT.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Portugal – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_EMN.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from equity market "
        "subindex",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SOV_EWN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Correlation and equal-country weights",
    },
    {
        "id": "ECB/CISS/D.NL.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Netherlands – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.US.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "United States – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.PT.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Portugal – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.ES.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Spain – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.FR.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "France – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.IE.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Ireland – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/CISS/D.U2.Z0Z.4F.EC.SS_FIN.CON",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Euro area (changing composition) – Contribution from financial intermediary "
        "subindex",
    },
    {
        "id": "ECB/CISS/D.NL.Z0Z.4F.EC.SS_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Netherlands – New Composite Indicator of Systemic Stress (CISS)",
    },
    {
        "id": "ECB/CISS/D.ES.Z0Z.4F.EC.SOV_CIN.IDX",
        "dataset_name": "Composite Indicator of Systemic Stress",
        "question_subject": "Spain – Composite Indicator of Sovereign Stress",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.R25",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Rate at 25th percentile of volume",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF08.CI",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Compounded Euro Short-Term Rate Index – Index of compounded interest",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.VL",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Share of volume of the 5 largest active banks",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF57.CR",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "12-months compounded euro short-term average rate",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.NT",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Number of transactions",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF24.CR",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "1-month compounded euro short-term average rate",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF16.CR",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "1-week compounded euro short-term average rate",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.WT",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Volume-weighted trimmed mean rate",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF40.CR",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "6-months compounded euro short-term average rate",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.NB",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Number of active banks",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.R75",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Rate at 75th percentile of volume",
    },
    {
        "id": "ECB/EST/B.EU000A2X2A25.TT",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "Euro short-term rate – Total volume",
    },
    {
        "id": "ECB/EST/B.EU000A2QQF32.CR",
        "dataset_name": "Euro Short-Term Rate",
        "question_subject": "3-months compounded euro short-term average rate",
    },
    {
        "id": "ECB/EXR/D.E01.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Netherlands "
        "guilder – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.DKK.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Danish krone per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H01.PTE.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Portuguese escudo – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.BRL.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Brazilian real – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Lithuanian litas – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Latvian lats – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Croatian kuna – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.HKD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Hong Kong dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Belgian franc – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.MXN.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Mexican peso per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.ISK.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Iceland krona per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – French franc – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.NZD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "New Zealand dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H01.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Croatian kuna – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E02.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Lithuanian "
        "litas – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Netherlands guilder – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.H03.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Luxembourg franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.DKK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Danish krone – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H01.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Slovenian tolar – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Croatian kuna – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.MXN.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Mexican peso – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.FIM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Finnish markka – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Italian lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Luxembourg "
        "franc – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Maltese lira – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Luxembourg franc – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Latvian lats – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Bulgarian lev – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Luxembourg franc "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.DZD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Algerian dinar – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.MYR.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Malaysian ringgit per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.SGD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Singapore dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Latvian lats – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Irish pound – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.TRY.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Turkish lira per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.EUR.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Euro – Nominal "
        "effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.SGD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Singapore dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Luxembourg franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E02.FIM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Finnish markka "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.CAD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Canadian "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Irish pound – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ILS.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Israeli shekel – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Croatian kuna "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Slovak koruna – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.EEK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Estonian kroon – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Maltese lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Latvian lats – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.FIM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Finnish markka – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Slovenian tolar "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.USD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "US dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Italian lira – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Spanish peseta – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.MAD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Moroccan dirham – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Irish pound – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.RON.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Romanian leu per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.USD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – US dollar – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.EEK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Estonian kroon – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Bulgarian lev – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.EEK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Estonian kroon – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Irish pound – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.SEK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Swedish krona "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Austrian "
        "schilling – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – French franc – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Lithuanian litas – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.PHP.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Philippine peso – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Maltese lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Maltese lira – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.PLN.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Polish zloty – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.KRW.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Korean won "
        "(Republic) – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.GRD.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Greek drachma – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Irish pound – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.JPY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Japanese yen – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.PTE.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Portuguese "
        "escudo – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.USD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – US dollar – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Lithuanian litas "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.NZD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – New Zealand "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Lithuanian litas "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Belgian franc – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.CZK.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Czech koruna per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – French franc – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Belgian franc – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.HKD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Hong Kong dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Croatian kuna – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.KRW.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Korean won "
        "(Republic) – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.DEM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – German mark – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.TRY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Turkish lira – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.NOK.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Norwegian krone per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.DEM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – German mark – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Latvian lats – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.CHF.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Swiss franc – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.PTE.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Portuguese "
        "escudo – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.KRW.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Korean won "
        "(Republic) – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.AUD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Australian "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H01.DEM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – German mark – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.GRD.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Greek drachma – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.CZK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Czech koruna – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – French franc – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.INR.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Indian rupee per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.DKK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Danish krone – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.DKK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Danish krone – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.GBP.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "UK pound sterling per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Italian lira – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.SGD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Singapore "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Austrian "
        "schilling – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ISK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Iceland krona – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – French franc – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Slovak koruna "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Belgian franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Italian lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.PHP.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Philippine peso per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Slovak koruna – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Belgian franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ZAR.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – South African "
        "rand – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.AUD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Australian dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.CAD.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Canadian dollar per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Slovenian "
        "tolar – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.CHF.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Swiss franc – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.FIM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Finnish markka – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.PEN.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Peru nuevo sol – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Spanish peseta – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.LTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Lithuanian litas – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.CNY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Chinese yuan "
        "renminbi – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Bulgarian lev – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.GRD.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Greek drachma – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.H01.LUF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Luxembourg franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Spanish peseta – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.SEK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Swedish krona – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Bulgarian lev – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.HUF.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Hungarian "
        "forint – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H00.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Netherlands guilder – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.TWD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – New Taiwan dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.CAD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Canadian dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.SEK.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Swedish krona per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.JPY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Japanese yen – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.IDR.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Indonesian rupiah per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.CNY.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Chinese yuan renminbi per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Netherlands "
        "guilder – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Maltese lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Austrian schilling – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Italian lira – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.UAH.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Ukraine hryvnia – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.IEP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Irish pound – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.THB.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Thai baht – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.PTE.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Portuguese escudo "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.PTE.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Portuguese escudo – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.H02.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Bulgarian lev – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E02.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Netherlands "
        "guilder – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.KRW.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Korean won (Republic) per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.JPY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Japanese yen – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.BGN.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Bulgarian lev "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.MTL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Maltese lira – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E01.USD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – US dollar – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.NOK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Norwegian krone – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.HKD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Hong Kong dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H02.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Austrian schilling – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.H01.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Netherlands guilder – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.ARS.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Argentine peso – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Slovak koruna – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Belgian franc – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E02.CNY.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Chinese yuan "
        "renminbi – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.DEM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – German mark – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.CYP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Cyprus pound – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Slovenian tolar – Nominal "
        "harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.BEF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Belgian franc "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H00.CYP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries (fixed composition) – Cyprus pound – Nominal harmonised "
        "competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.EEK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Estonian kroon – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.CHF.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Swiss franc per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Croatian kuna – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Spanish peseta "
        "– Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Slovenian tolar – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.SIT.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Slovenian tolar – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.H02.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Spanish peseta – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.ILS.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Israeli shekel per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.CZK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Czech koruna – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H01.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Austrian schilling – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.AED.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – United Arab "
        "Emirates dirham – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.NOK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Norwegian krone "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.HRK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Croatian kuna – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E01.ATS.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Austrian "
        "schilling – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.FRF.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – French franc – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.INR.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Indian rupee – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.COP.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Colombian peso – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E03.AUD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Australian dollar "
        "– Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.LVL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Latvian lats – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H03.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Slovak koruna – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.CLP.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Chilean peso – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.AUD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Australian "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.CYP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Cyprus pound – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.HKD.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Hong Kong "
        "dollar – Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.FIM.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Finnish markka – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.E03.SEK.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Swedish krona – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.E01.ESP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Narrow EER group of trading partners (fixed composition) – Spanish peseta – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H01.CYP.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and narrow EER group of trading partners (fixed "
        "composition) – Cyprus pound – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.PLN.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Polish zloty – "
        "Nominal effective exchange rate",
    },
    {
        "id": "ECB/EXR/D.H03.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and broad EER group of trading partners (fixed "
        "composition) – Italian lira – Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E03.SKK.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Broad EER group of trading partners (fixed composition) – Slovak koruna – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.H02.NLG.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Euro area countries and extended EER group of trading partners (fixed "
        "composition) – Netherlands guilder – Nominal harmonised competitiveness "
        "indicator",
    },
    {
        "id": "ECB/EXR/D.HUF.EUR.SP00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Hungarian forint per euro spot exchange rate",
    },
    {
        "id": "ECB/EXR/D.E02.ITL.NN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Italian lira – "
        "Nominal harmonised competitiveness indicator",
    },
    {
        "id": "ECB/EXR/D.E02.EUR.EN00.A",
        "dataset_name": "Exchange Rates",
        "question_subject": "Extended EER group of trading partners (fixed composition) – Euro – Nominal "
        "effective exchange rate",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.DFR.CHG",
        "dataset_name": "Financial market data",
        "question_subject": "Deposit facility - date of changes (raw data) – Change in percentage points "
        "compared to previous rate",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.DFR.LEV",
        "dataset_name": "Financial market data",
        "question_subject": "Deposit facility - date of changes (raw data) – Level",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.MRR_RT.LEV",
        "dataset_name": "Financial market data",
        "question_subject": "Main refinancing operations - Minimum bid rate/fixed rate (date of changes) "
        "– Level",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.MLFR.CHG",
        "dataset_name": "Financial market data",
        "question_subject": "Marginal lending facility - date of changes (raw data) – Change in "
        "percentage points compared to previous rate",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.MRR_FR.LEV",
        "dataset_name": "Financial market data",
        "question_subject": "Main refinancing operations - fixed rate tenders (fixed rate) (date of "
        "changes) – Level",
    },
    {
        "id": "ECB/FM/D.U2.EUR.4F.KR.MLFR.LEV",
        "dataset_name": "Financial market data",
        "question_subject": "Marginal lending facility - date of changes (raw data) – Level",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_11Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 11-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_28Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 28-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_14Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 14-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_26Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 26-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_19Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 19-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_4Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 4-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_10Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 10-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_24Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 24-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_29Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 29-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_6Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 6-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_15Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 15-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_9Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 9-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_17Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 17-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_29Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 29-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_15Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 15-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_1Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 1-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_11Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 11-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_29Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 29-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_27Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 27-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_9Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 9-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_2Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 2-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_19Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 19-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_13Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 13-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 5-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_9Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 9-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_17Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 17-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_9Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 9-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_17Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 17-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_4Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 4-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_8Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 8-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 6-month maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_1Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 1-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_18Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 18-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_4Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 4-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_10Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 10-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_15Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 15-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_29Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 29-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_1Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 1-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_29Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 29-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_9Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 9-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_22Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 22-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_23Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 23-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_16Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 16-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_10Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 10-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_19Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 19-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_25Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 25-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_19Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 19-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_21Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 21-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_6Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 6-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_13Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 13-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 6-month maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_22Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 22-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_5Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 5-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_1Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 1-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_15Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 15-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 3-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_14Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 14-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_18Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 18-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_7Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 7-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_7Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 7-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_6Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 6-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_2Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 2-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_2Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 2-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_17Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 17-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_14Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 14-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_4Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 4-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_22Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 22-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_25Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 25-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_19Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 19-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_27Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 27-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_1Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 1-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_19Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 19-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_5Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 5-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_10Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 10-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_27Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 27-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_13Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 13-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_9Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 9-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_9Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 9-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_11Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 11-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_16Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 16-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_4Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 4-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_3Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 3-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_15Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 15-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_27Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 27-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_2Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 2-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_5Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 5-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_2Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 2-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_7Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 7-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_7Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 7-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_3Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 3-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_23Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 23-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 2-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_13Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 13-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_6Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 6-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_26Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 26-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_15Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 15-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_3Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 3-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_7Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 7-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_13Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 13-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_11Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 11-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_27Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 27-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_27Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 27-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_8Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 8-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_24Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 24-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_28Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 28-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_23Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 23-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_24Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 24-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_3Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 3-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_8Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 8-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_28Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 28-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_7Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 7-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_24Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 24-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_25Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 25-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_15Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 15-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 1-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 3-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_20Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 20-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_10Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 10-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_26Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 26-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_11Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 11-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_11Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 11-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_15Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 15-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_10Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 10-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_27Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 27-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_24Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 24-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_11Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 11-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_2Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 2-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 9-month maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_23Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 23-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_27Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 27-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_20Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 20-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_27Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 27-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_17Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 17-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_19Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 19-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_10Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 10-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_18Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 18-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_9Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 9-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_3Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 3-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_29Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 29-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_1Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 1-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_21Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 21-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_18Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 18-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_3Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 3-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_11Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 11-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_9Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 9-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_13Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 13-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_26Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 26-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_18Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 18-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_16Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 16-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_18Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 18-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_27Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 27-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_27Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 27-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_17Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 17-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_16Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 16-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_2Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 2-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_12Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 12-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_18Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 18-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_26Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 26-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_14Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 14-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_3Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 3-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_19Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 19-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_21Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 21-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_22Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 22-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_1Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 1-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_22Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 22-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_4Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 4-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_8Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 8-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_1Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 1-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_19Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 19-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_24Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 24-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_25Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 25-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_4Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 4-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_29Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 29-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_12Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 12-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_7Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 7-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_20Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 20-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_17Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 17-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_18Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 18-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_26Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 26-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_23Y1M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 23-year 1-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_15Y6M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 15-year 6-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_21Y8M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 21-year 8-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_28Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 28-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_19Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 19-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_4Y2M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 4-year 2-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_10Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 10-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_26Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 26-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_14Y11M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 14-year 11-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_29Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 29-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_7Y3M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 7-year 3-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.IF_29Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve "
        "instantaneous forward rate, 29-year 10-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_28Y4M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 28-year 4-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.PY_22Y5M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Par yield curve "
        "rate, 22-year 5-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.IF_22Y",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "instantaneous forward rate, 22-year maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_14Y7M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 14-year 7-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.PY_25Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Par yield "
        "curve rate, 25-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_13Y9M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – issuers of all ratings – Yield curve "
        "spot rate, 13-year 9-month residual maturity",
    },
    {
        "id": "ECB/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_16Y10M",
        "dataset_name": "Financial market data - yield curve",
        "question_subject": "Euro-area nominal government bonds – AAA-rated issuers – Yield curve spot "
        "rate, 16-year 10-month residual maturity",
    },
    {
        "id": "ECB/ILM/D.U2.C.A050500.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Marginal lending facility (Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.EXLIQ.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Excess liquidity (Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.NLIQ.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Net liquidity effect from Autonomous Factors and MonPol portfolios "
        "(Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.L020100.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Current accounts (covering the minimum reserves system) (Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.MRR.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Minimum reserve requirements (Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.L020200.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Deposit facility (Eurosystem)",
    },
    {
        "id": "ECB/ILM/D.U2.C.TOMO.U2.EUR",
        "dataset_name": "Internal Liquidity Management",
        "question_subject": "Open market operations excl. MonPol portfolios (Eurosystem)",
    },
]
