"""FRED Question List."""

SOURCE_INTRO = (
    "The Federal Reserve Economic Data database (FRED) provides economic data from national, "
    "international, public, and private sources.You're going to predict how questions based on "
    "this data will resolve."
)

RESOLUTION_CRITERIA = "Resolves to the value found at {url} once the data is published."

NULLIFIED_IDS = [
    "AMERIBOR",
]

# flake8: noqa: B950

fred_questions = [
    {
        "id": "AAA10Y",
        "series_name": "Moody's Aaa Corporate Bond Yield compared to the 10-year Treasury yield",
    },
    {
        "id": "ANFCI",
        "series_name": "the Chicago Fed's Adjusted National Financial Conditions Index",
    },
    {
        "id": "BAA10Y",
        "series_name": "Moody's Seasoned Baa Corporate Bond Yield compared to the 10-year Treasury yield",
    },
    {
        "id": "BAMLC0A0CM",
        "series_name": "the option-adjusted spread of the ICE BofA Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A0CMEY",
        "series_name": "the effective yield of the ICE BofA Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A1CAAA",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of AAA in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A1CAAAEY",
        "series_name": "the effective yield of securities with an investment grade rating of AAA in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A2CAA",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of AA in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A2CAAEY",
        "series_name": "the effective yield of securities with an investment grade rating of AA in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A3CA",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of A in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A3CAEY",
        "series_name": "the effective yield of securities with an investment grade rating of A in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A4CBBB",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of BBB in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC0A4CBBBEY",
        "series_name": "the effective yield of securities with an investment grade rating of BBB in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLC4A0C710YEY",
        "series_name": "the effective yield of securities with a remaining term to maturity of 7-10 years in the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLCC0A0CMTRIV",
        "series_name": "the total return of the ICE BofA US Corporate Index, which tracks the performance of corporate debt issued in the US domestic market,",
    },
    {
        "id": "BAMLEMCBPIOAS",
        "series_name": "the option-adjusted spread for the ICE BofA Emerging Markets Corporate Plus Index, which tracks the performance of emerging markets non-sovereign debt within major domestic and Eurobond markets,",
    },
    {
        "id": "BAMLEMHBHYCRPIOAS",
        "series_name": "the option-adjusted spread for the ICE BofA High Yield Emerging Markets Corporate Plus Index, which tracks the performance of emerging markets securities rated BB1 or lower within major domestic and Eurobond markets,",
    },
    {
        "id": "BAMLH0A0HYM2",
        "series_name": "the option-adjusted spread for the ICE BofA US High Yield Index, which tracks the performance of corporate debt denominated below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A0HYM2EY",
        "series_name": "the effective yield of the ICE BofA US High Yield Index, which tracks the performance of corporate debt denominated below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A1HYBB",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of BB in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A1HYBBEY",
        "series_name": "the effective yield of securities with an investment grade rating of BB in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A2HYB",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of B in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A2HYBEY",
        "series_name": "the effective yield of securities with an investment grade rating of B in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A3HYC",
        "series_name": "the option-adjusted spread of securities with an investment grade rating of CCC or below in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLH0A3HYCEY",
        "series_name": "the effective yield of securities with an investment grade rating of CCC or below in the ICE BofA US High Yield Master II Index, which tracks the performance of corporate debt below investment grade in the US domestic market,",
    },
    {
        "id": "BAMLHE00EHYIEY",
        "series_name": "the effective yield of the ICE BofA Euro High Yield Index, which tracks the performance of below investment grade corporate debt issued in the euro domestic or eurobond markets,",
    },
    {
        "id": "BAMLHE00EHYIOAS",
        "series_name": "the option-adjusted spread of the ICE BofA Euro High Yield Index, which tracks the performance of below investment grade corporate debt issued in the euro domestic or eurobond markets,",
    },
    {
        "id": "BAMLHYH0A0HYM2TRIV",
        "series_name": "the total return of the ICE BofA US High Yield Index, which tracks the performance of below investment grade corporate debt publicly issued in the US domestic market,",
    },
    {
        "id": "CARACBW027SBOG",
        "series_name": "the total dollar amount representing all automobile loans made by commercial banks in the US",
    },
    {"id": "CASACBW027SBOG", "series_name": "the cash assets of all commercial US banks"},
    {"id": "CBBTCUSD", "series_name": "the price of Bitcoin, as measured by Coinbase,"},
    {"id": "CC4WSA", "series_name": "the 4-week moving average of insured unemployment claims"},
    {
        "id": "CCLACBW027SBOG",
        "series_name": "the amount of money representing all credit card loans and other revolving plans made by commercial banks in the US",
    },
    {"id": "CCSA", "series_name": "the number of insured unemployment claims"},
    {
        "id": "CREACBW027SBOG",
        "series_name": "the amount of money representing all commercial real estate loans made by commercial banks in the US",
    },
    {"id": "CURRCIR", "series_name": "the number of US dollars in circulation"},
    {
        "id": "D2WLTGAL",
        "series_name": "the amount of money held by the US Treasury in its general account at the Federal Reserve Bank of New York",
    },
    {"id": "DAAA", "series_name": "Moody's Seasoned Aaa Corporate Bond Yield"},
    {"id": "DBAA", "series_name": "Moody's Seasoned Baa Corporate Bond Yield"},
    {"id": "DCOILBRENTEU", "series_name": "the price of Brent crude oil"},
    {
        "id": "DCOILWTICO",
        "series_name": "the price of West Texas Intermediate (WTI - Cushing) crude oil",
    },
    {"id": "DEXCAUS", "series_name": "the spot exchange rate of Canadian dollars to US dollars"},
    {
        "id": "DEXCHUS",
        "series_name": "the spot exchange rate of Chinese yuan renminbi to US dollars",
    },
    {"id": "DEXJPUS", "series_name": "the spot exchange rate of Japanese yen to US dollars"},
    {"id": "DEXKOUS", "series_name": "the spot exchange rate of South Korean won to US dollars"},
    {"id": "DEXMXUS", "series_name": "the spot exchange rate of Mexican pesos to US dollars"},
    {"id": "DEXUSEU", "series_name": "the spot exchange rate of US dollars to euros"},
    {"id": "DEXUSUK", "series_name": "the spot exchange rate of US dollars to UK pound sterling"},
    {
        "id": "DFEDTARL",
        "series_name": "the lower limit of the target range of the federal funds rate (interest rate) set by the Federal Open Market Committee",
    },
    {
        "id": "DFEDTARU",
        "series_name": "the upper limit of the target range of the federal funds rate (interest rate) set by the Federal Open Market Committee",
    },
    {
        "id": "DFF",
        "series_name": "the effective federal funds rate (interest rate)",
    },
    {
        "id": "DFII10",
        "series_name": "the market yield on US treasury securities at 10-year constant maturity, quoted on an investment basis and inflation-indexed,",
    },
    {
        "id": "DFII20",
        "series_name": "the market yield on US treasury securities at 20-year constant maturity, quoted on an investment basis and inflation-indexed,",
    },
    {
        "id": "DFII30",
        "series_name": "the market yield on US treasury securities at 30-year constant maturity, quoted on an investment basis and inflation-indexed,",
    },
    {
        "id": "DFII5",
        "series_name": "the market yield on US treasury securities at 5-year constant maturity, quoted on an investment basis and inflation-indexed,",
    },
    {
        "id": "DGS1",
        "series_name": "the market yield on US treasury securities at 1-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS10",
        "series_name": "the market yield on US treasury securities at 10-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS1MO",
        "series_name": "the market yield on US treasury securities at 1-month constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS2",
        "series_name": "the market yield on US treasury securities at 2-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS20",
        "series_name": "the market yield on US treasury securities at 20-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS3",
        "series_name": "the market yield on US treasury securities at 3-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS30",
        "series_name": "the market yield on US treasury securities at 30-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS3MO",
        "series_name": "the market yield on US treasury securities at 3-month constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS5",
        "series_name": "the market yield on US treasury securities at 5-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS6MO",
        "series_name": "the market yield on US treasury securities at 6-month constant maturity, quoted on an investment basis,",
    },
    {
        "id": "DGS7",
        "series_name": "the market yield on US treasury securities at 7-year constant maturity, quoted on an investment basis,",
    },
    {"id": "DHHNGSP", "series_name": "the spot price of Henry Hub natural gas"},
    {"id": "DJIA", "series_name": "the Dow Jones Industrial Average"},
    {
        "id": "DPCREDIT",
        "series_name": "the discount rate for the Federal Reserve's primary credit discount window program",
    },
    {
        "id": "DPRIME",
        "series_name": "the Federal Reserve's Bank Prime Loan Rate, the rate posted by a majority of top US commercial banks",
    },
    {
        "id": "DPSACBW027SBOG",
        "series_name": "the amount of money representing deposits in all US commercial banks",
    },
    {
        "id": "DTB1YR",
        "series_name": "the Federal Reserve's 1-year secondary market treasury bill rate",
    },
    {
        "id": "DTB3",
        "series_name": "the Federal Reserve's 3-month secondary market treasury bill rate",
    },
    {
        "id": "DTB4WK",
        "series_name": "the Federal Reserve's 4-week secondary market treasury bill rate",
    },
    {
        "id": "DTB6",
        "series_name": "the Federal Reserve's 6-month secondary market treasury bill rate",
    },
    {
        "id": "DTWEXAFEGS",
        "series_name": "the Nominal Advanced Foreign Economies US Dollar Index, a weighted average of the foreign exchange value of the US dollar against a subset of broad index currencies that are advanced foreign economies,",
    },
    {
        "id": "DTWEXBGS",
        "series_name": "the Nominal Broad US Dollar Index, a weighted average of the foreign exchange value of the US dollar against currencies of a broad group of major US trading partners,",
    },
    {
        "id": "ECBASSETSW",
        "series_name": "the amount of money representing all central bank assets for the euro area",
    },
    {
        "id": "ECBDFR",
        "series_name": "the European Central Bank's deposit facility rate for the euro area",
    },
    {
        "id": "ECBESTRVOLWGTTRMDMNRT",
        "series_name": "the euro short-term rate (volume-weighted trimmed mean), a measure of the borrowing costs of banks in the euro area,",
    },
    {
        "id": "EFFR",
        "series_name": "the effective federal funds rate (interest rate) set by the Federal Reserve",
    },
    {
        "id": "EXPINF10YR",
        "series_name": "the Federal Reserve Bank of Cleveland's 10-year expected inflation rate",
    },
    {
        "id": "EXPINF1YR",
        "series_name": "the Federal Reserve Bank of Cleveland's 1-year expected inflation rate",
    },
    {
        "id": "EXPINF2YR",
        "series_name": "the Federal Reserve Bank of Cleveland's 2-year expected inflation rate",
    },
    {
        "id": "EXPINF30YR",
        "series_name": "the Federal Reserve Bank of Cleveland's 30-year expected inflation rate",
    },
    {
        "id": "EXPINF5YR",
        "series_name": "the Federal Reserve Bank of Cleveland's 5-year expected inflation rate",
    },
    {"id": "GASDESW", "series_name": "the average price of diesel in the US"},
    {"id": "GASREGW", "series_name": "the average price of regular gas in the US"},
    {
        "id": "GVZCLS",
        "series_name": "the Chicago Board Options Exchange's Gold ETF Volatility Index",
    },
    {
        "id": "H41RESPPALDKNWW",
        "series_name": "the amount of money loaned as part of the Bank Term Funding Program",
    },
    {
        "id": "H41RESPPALDKXAWNWW",
        "series_name": "the weekly average of the amount of money loaned as part of the Bank Term Funding Program",
    },
    {"id": "IC4WSA", "series_name": "the 4-week moving average of initial unemployment claims"},
    {"id": "ICSA", "series_name": "the weekly number of initial unemployment claims"},
    {"id": "IHLIDXUS", "series_name": "the number of US job postings on Indeed"},
    {
        "id": "IHLIDXUSTPSOFTDEVE",
        "series_name": "the number of US software development job postings on Indeed",
    },
    {"id": "IORB", "series_name": "the Federal Reserve's interest rate on reserve balances"},
    {
        "id": "IUDSOIA",
        "series_name": "the daily Sterling Overnight Index Average, the interest rate applied to bank transactions in the British Sterling Market during off hours,",
    },
    {
        "id": "MMTY",
        "series_name": "the yield on money market investments based on US treasury obligations",
    },
    {"id": "MORTGAGE15US", "series_name": "the 15-year fixed rate mortgage average in the US"},
    {"id": "MORTGAGE30US", "series_name": "the 30-year fixed rate mortgage average in the US"},
    {
        "id": "NASDAQ100",
        "series_name": "the NASDAQ 100 Index, which represents the daily index value at market close,",
    },
    {
        "id": "NASDAQCOM",
        "series_name": "the NASDAQ Composite Index, which represents the daily index value at market close,",
    },
    {
        "id": "NDR12MCD",
        "series_name": "the US national deposit rate for 12-month certificates of deposit (CDs)",
    },
    {"id": "NFCI", "series_name": "the Chicago Fed's National Financial Conditions Index"},
    {
        "id": "NFCICREDIT",
        "series_name": "the Chicago Fed's National Financial Conditions Credit Subindex",
    },
    {
        "id": "NFCILEVERAGE",
        "series_name": "the Chicago Fed's National Financial Conditions Leverage Subindex",
    },
    {
        "id": "NFCIRISK",
        "series_name": "the Chicago Fed's National Financial Conditions Risk Subindex",
    },
    {
        "id": "NIKKEI225",
        "series_name": "the Nikkei 225 Stock Average, which represent the daily index value at market close,",
    },
    {"id": "OBFR", "series_name": "the Federal Reserve's overnight bank funding rate"},
    {"id": "OBMMIFHA30YF", "series_name": "the 30-year fixed rate FHA mortgage index"},
    {"id": "OBMMIJUMBO30YF", "series_name": "the 30-year fixed rate jumbo mortgage index"},
    {"id": "OBMMIVA30YF", "series_name": "the 30-year fixed rate Veterans Affairs mortgage index"},
    {
        "id": "OVXCLS",
        "series_name": "the Chicago Board Options Exchange's Crude Oil ETF Volatility Index",
    },
    {
        "id": "REAINTRATREARAT10Y",
        "series_name": "the Federal Reserve Bank of Cleveland's estimate for the 10-year real interest rate",
    },
    {
        "id": "REAINTRATREARAT1YE",
        "series_name": "the Federal Reserve Bank of Cleveland's estimate for the 1-year real interest rate",
    },
    {
        "id": "RESPPANWW",
        "series_name": "the total dollar amount of assets held by all US Federal Reserve banks",
    },
    {
        "id": "RESPPLLOPNWW",
        "series_name": "the total weekly remittance of earnings by the Federal Reserve to the US Treasury",
    },
    {
        "id": "RIFSPPFAAD90NB",
        "series_name": "the 90-day AA Financial Commercial Paper Interest Rate",
    },
    {
        "id": "RPONTSYD",
        "series_name": "the aggregated daily value of US Treasury securities repurchased overnight by the Federal Reserve in temporary open market operations ",
    },
    {
        "id": "RRPONTSYAWARD",
        "series_name": "the award rate of US Treasury securities sold by the Federal Reserve in overnight temporary open market operations",
    },
    {
        "id": "RRPONTSYD",
        "series_name": "the aggregated daily value of US Treasury securities sold by the Federal Reserve in temporary open market operations",
    },
    {
        "id": "RRPONTTLD",
        "series_name": "the aggregated daily value of securities sold by the Federal Reserve in temporary open market operations",
    },
    {
        "id": "SNDR",
        "series_name": "the aggregated value of US national average interest rates for savings accounts",
    },
    {"id": "SOFR", "series_name": "the Federal Reserve's Secured Overnight Financing Rate"},
    {
        "id": "SOFR180DAYAVG",
        "series_name": "the 180-day average of the Federal Reserve's Secured Overnight Financing Rate",
    },
    {
        "id": "SOFR30DAYAVG",
        "series_name": "the 30-day average of the Federal Reserve's Secured Overnight Financing Rate",
    },
    {
        "id": "SOFR90DAYAVG",
        "series_name": "the 90-day average of the Federal Reserve's Secured Overnight Financing Rate",
    },
    {
        "id": "SOFRINDEX",
        "series_name": "the Federal Reserve's SOFR (Secured Overnight Financing Rate) Index",
    },
    {
        "id": "SP500",
        "series_name": "the S&P 500, which represents the daily index value at market close",
    },
    {"id": "STLFSI4", "series_name": "the St. Louis Fed Financial Stress Index"},
    {
        "id": "SWPT",
        "series_name": "the weekly value of central bank liquidity swaps held by the Federal Reserve",
    },
    {
        "id": "T10Y2Y",
        "series_name": "the yield spread between 10-year and 2-year US Treasury bonds",
    },
    {
        "id": "T10Y3M",
        "series_name": "the yield spread between 10-year and 3-month US Treasury bonds",
    },
    {
        "id": "T10YFF",
        "series_name": "the yield spread between the 10-year US Treasury bond and the Effective Federal Funds Rate (interest rate)",
    },
    {"id": "T10YIE", "series_name": "the US' 10-year breakeven inflation rate"},
    {"id": "T5YIE", "series_name": "the US' 5-year breakeven inflation rate"},
    {"id": "T5YIFR", "series_name": "the US' 5-year forward inflation expectation rate"},
    {"id": "THREEFYTP10", "series_name": "the term premium on a 10-year zero-coupon bond"},
    {
        "id": "TLAACBW027SBOG",
        "series_name": "the total dollar amount of assets held by all US commercial banks",
    },
    {
        "id": "TMBACBW027SBOG",
        "series_name": "the total dollar amount of mortgage-backed securities held by all US commercial banks",
    },
    {
        "id": "TOTBKCR",
        "series_name": "the total dollar amount of bank credit held by all US commercial banks",
    },
    {
        "id": "TOTCI",
        "series_name": "the total dollar amount representing all commercial and industrial loans made by commercial banks in the US",
    },
    {
        "id": "TOTLL",
        "series_name": "the total dollar amount representing all loans and leases in bank credit made by commercial banks in the US",
    },
    {
        "id": "TREAST",
        "series_name": "the total value of US Treasury securities held by the Federal Reserve",
    },
    {
        "id": "USEPUINDXD",
        "series_name": "the Economic Policy Uncertainty Index for the US",
    },
    {"id": "VIXCLS", "series_name": "the Chicago Board Options Exchange's Volatility Index"},
    {
        "id": "VXVCLS",
        "series_name": "the Chicago Board Options Exchange's S&P 500 3-Month Volatility Index",
    },
    {
        "id": "WALCL",
        "series_name": "the total dollar amount of assets held by all US Federal Reserve banks",
    },
    {
        "id": "WDTGAL",
        "series_name": "the total dollar amount of deposits in the US Treasury's general accounts of Federal Reserve Banks, other than reserve balances,",
    },
    {"id": "WEI", "series_name": "the Weekly Economic Index (Lewis-Mertens-Stock)"},
    {
        "id": "WGS10YR",
        "series_name": "the market yield on US treasury securities at 10-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "WGS1YR",
        "series_name": "the market yield on US treasury securities at 1-year constant maturity, quoted on an investment basis,",
    },
    {
        "id": "WLCFLL",
        "series_name": "the weekly dollar amount of loans made by the Federal Reserve under its liquidity and credit facilities",
    },
    {
        "id": "WLCFLPCL",
        "series_name": "the weekly dollar amount of loans made under the primary credit lending program by the Federal Reserve",
    },
    {
        "id": "WLODLL",
        "series_name": "the weekly dollar amount of balances in the accounts of depository institutions in the Federal Reserve Banks",
    },
    {
        "id": "WLRRAL",
        "series_name": "the weekly dollar amount associated with Federal Reserve reverse repurchase agreements",
    },
    {"id": "WM1NS", "series_name": "USD money supply as measured by M1"},
    {"id": "WM2NS", "series_name": "USD money supply as measured by M2"},
    {
        "id": "WORAL",
        "series_name": "the weekly dollar amount associated with Federal Reserve repurchase agreements",
    },
    {
        "id": "WRBWFRBL",
        "series_name": "the total dollar amount of reserve balances held with Federal Reverse Banks",
    },
    {
        "id": "WRESBAL",
        "series_name": "the weekly average of reserve balances held with Federal Reserve Banks",
    },
    {
        "id": "WRMFNS",
        "series_name": "Retail Money Market Funds, a component of M2, a measure of USD money supply,",
    },
    {
        "id": "WSHOMCB",
        "series_name": "the total dollar amount of mortgage-backed securities held by the US Federal Reserve Banks",
    },
    {
        "id": "WSHOSHO",
        "series_name": "the total dollar amount of securities held by US Federal Reserve Banks",
    },
    {
        "id": "WTREGEN",
        "series_name": "the weekly average of deposits other than reserve balances held in the US treasury's general accounts with Federal Reserve Banks",
    },
    {
        "id": "LNU01075379",
        "series_name": "the number of US civilians employed or available for employment with no disability and 65 years old and older",
    },
    {
        "id": "CGRAL16O",
        "series_name": "the number of US civilians employed or available for employment with a Bachelor's Degree or higher and 16 years old and older",
    },
    {
        "id": "ADEGL16O",
        "series_name": "the number of US civilians 16 years old and older employed or available for employment with an Associate Degree",
    },
    {
        "id": "LNU01075600",
        "series_name": "the number of US civilians 65 years old and older employed or available for employment with a disability",
    },
    {
        "id": "LNU01073397",
        "series_name": "the number of foreign born, female US civilians employed or available for employment",
    },
    {
        "id": "LNU01073396",
        "series_name": "the number of foreign born, male US civilians employed or available for employment",
    },
    {
        "id": "LNU01073415",
        "series_name": "the number of native born, female US civilians employed or available for employment",
    },
    {
        "id": "LNU01074597",
        "series_name": "the number of US civilians 16 years old and older employed or available for employment with a disability",
    },
    {
        "id": "CLF16OV",
        "series_name": "the number of US civilians employed or available for employment",
    },
    {
        "id": "LNU01073395",
        "series_name": "the number of foreign born US civilians employed or available for employment",
    },
    {
        "id": "LNS11000060",
        "series_name": "the number of US civilians between 25 and 54 years of age that are employed or available for employment",
    },
    {
        "id": "LNU01076960",
        "series_name": "the number of female US civilians employed or available for employment with a disability and between 16 and 64 years of age",
    },
    {
        "id": "LNU01073413",
        "series_name": "the number of foreign born US civilians employed or available for employment",
    },
    {
        "id": "LNS11024230",
        "series_name": "the number of US civilians aged 55 years and above employed or available for employment",
    },
    {
        "id": "LNS11000002",
        "series_name": "the number of female US civilians employed or available for employment",
    },
    {
        "id": "TOTLL65O",
        "series_name": "the number of US civilians aged 65 years and above employed or available for employment",
    },
    {
        "id": "LNS11000001",
        "series_name": "the number of male US civilians employed or available for employment",
    },
    {
        "id": "LNU01076955",
        "series_name": "the number of male US civilians employed or available for employment with a disability and between 16 and 64 years of age",
    },
    {
        "id": "LNS11000009",
        "series_name": "the number of Hispanic or Latino US civilians employed or available for employment",
    },
    {
        "id": "LNS11000003",
        "series_name": "the number of White US civilians employed or available for employment",
    },
    {
        "id": "LNS11000006",
        "series_name": "the number of Black or African American US civilians employed or available for employment",
    },
    {
        "id": "TOTLL3544",
        "series_name": "the number of US civilians employed or available for employment between 35 and 44 years of age",
    },
    {
        "id": "TOTLL5564",
        "series_name": "the number of US civilians employed or available for employment between 55 and 64 years of age",
    },
    {
        "id": "TOTLL2534",
        "series_name": "the number of US civilians employed or available for employment between 25 and 34 years of age",
    },
    {
        "id": "LNS11000036",
        "series_name": "the number of US civilians employed or available for employment between 20 and 24 years of age",
    },
    {
        "id": "LNS11000012",
        "series_name": "the number of US civilians employed or available for employment between 16 and 19 years of age",
    },
    {
        "id": "LNU01032183",
        "series_name": "the number of Asian US civilians employed or available for employment",
    },
    {
        "id": "LNU01375600",
        "series_name": "the labor force participation rate among US civilians 65 years and older with a disability",
    },
    {
        "id": "LNU01373414",
        "series_name": "the labor force participation rate among native born, male US civilians",
    },
    {
        "id": "LNU01373396",
        "series_name": "the labor force participation rate among foreign born, male US civilians",
    },
    {
        "id": "LNU01373415",
        "series_name": "the labor force participation rate among native born, female US civilians",
    },
    {
        "id": "LNU01373397",
        "series_name": "the labor force participation rate among foreign born, female US civilians",
    },
    {
        "id": "LNU01300003",
        "series_name": "the labor force participation rate among White US civilians",
    },
    {
        "id": "LNU01373395",
        "series_name": "the labor force participation rate among foreign born US civilians",
    },
    {
        "id": "LNU01300009",
        "series_name": "the labor force participation rate among Hispanic or Latino US civilians",
    },
    {
        "id": "LNU01373413",
        "series_name": "the labor force participation rate among native born US civilians",
    },
    {
        "id": "LNU01332183",
        "series_name": "the labor force participation rate among Asian US civilians",
    },
    {
        "id": "CIVPART",
        "series_name": "the labor force participation rate among US civilians",
    },
    {
        "id": "LNS11300060",
        "series_name": "the labor force participation rate among US civilians between 25 and 54 years of age",
    },
    {
        "id": "LNU01300002",
        "series_name": "the labor force participation rate among female US civilians",
    },
    {
        "id": "LNU01300001",
        "series_name": "the labor force participation rate among male US civilians",
    },
    {
        "id": "LNS11324230",
        "series_name": "the labor force participation rate among US civilians 55 years old and older",
    },
    {
        "id": "LNU01300012",
        "series_name": "the labor force participation rate among US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNU01300006",
        "series_name": "the labor force participation rate among Black or African American US civilians",
    },
    {
        "id": "LNS11300036",
        "series_name": "the labor force participation rate among US civilians between 20 and 24 years of age",
    },
    {
        "id": "LNU01375379",
        "series_name": "the labor force participation rate among US civilians 65 years old and older with no disability",
    },
    {
        "id": "LNU01374597",
        "series_name": "the labor force participation rate among US civilians 16 years old and older with a disability",
    },
    {
        "id": "LNU01327662",
        "series_name": "the labor force participation rate among US civilians 25 years old and older with a Bachelor's Degree",
    },
    {
        "id": "LNS12600000",
        "series_name": "the number of employed US civilians who usually work part time",
    },
    {
        "id": "LNS12500000",
        "series_name": "the number of employed US civilians who usually work full time",
    },
    {
        "id": "LNU02075600",
        "series_name": "the number of employed US civilians 65 years old and older with a disability",
    },
    {
        "id": "LNU02074597",
        "series_name": "the number of employed US civilians 16 years old and older with a disability",
    },
    {
        "id": "LNU02074593",
        "series_name": "the number of employed US civilians 16 years old and older with no disability",
    },
    {
        "id": "LNU02075379",
        "series_name": "the number of employed US civilians 65 years old and older with no disability",
    },
    {
        "id": "CE16OV",
        "series_name": "the number of employed US civilians",
    },
    {
        "id": "LNU02000086",
        "series_name": "the number of employed US civilians between 16 and 17 years of age",
    },
    {
        "id": "LNS12000012",
        "series_name": "the number of employed US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNS12000088",
        "series_name": "the number of employed US civilians between 18 and 19 years of age",
    },
    {
        "id": "LNS12000036",
        "series_name": "the number of employed US civilians between 20 and 24 years of age",
    },
    {
        "id": "LNS12000024",
        "series_name": "the number of employed US civilians 20 years old and older",
    },
    {
        "id": "LNS12000089",
        "series_name": "the number of employed US civilians between 25 and 34 years of age",
    },
    {
        "id": "LNS12000060",
        "series_name": "the number of employed US civilians between 25 and 54 years of age",
    },
    {
        "id": "LNS12000048",
        "series_name": "the number of employed US civilians 25 years old and older",
    },
    {
        "id": "LNS12000091",
        "series_name": "the number of employed US civilians between 35 and 44 years of age",
    },
    {
        "id": "LNS12000093",
        "series_name": "the number of employed US civilians between 45 and 54 years of age",
    },
    {
        "id": "LNS12024230",
        "series_name": "the number of employed US civilians 55 years old and older",
    },
    {
        "id": "LNS12034560",
        "series_name": "the number of US civilians employed in agriculture and related industries",
    },
    {
        "id": "LNS12027714",
        "series_name": "the number of self-employed, unincorporated US civilians",
    },
    {
        "id": "LNU02032183",
        "series_name": "the number of employed Asian US civilians",
    },
    {
        "id": "LNS12027662",
        "series_name": "the number of employed US civilians 25 years old and older with a Bachelor's Degree and higher",
    },
    {
        "id": "LNS12000006",
        "series_name": "the number of employed Black or African American US civilians",
    },
    {
        "id": "LNU02032210",
        "series_name": "the number of US civilians employed in construction and extraction occupations",
    },
    {
        "id": "LNU02032209",
        "series_name": "the number of US civilians employed in farming, fishing and forestry occupations",
    },
    {
        "id": "LNU02073395",
        "series_name": "the number of employed foreign born US civilians",
    },
    {
        "id": "LNS12000009",
        "series_name": "the number of employed Hispanic or Latino US civilians",
    },
    {
        "id": "LNU02032211",
        "series_name": "the number of US civilians employed in installation, maintenance and repair occupations",
    },
    {
        "id": "LNU02032202",
        "series_name": "the number of US civilians employed in management, business, and financial operations occupations",
    },
    {
        "id": "LNU02032201",
        "series_name": "the number of US civilians employed in management, professional, and related occupations",
    },
    {
        "id": "LNS12000001",
        "series_name": "the number of employed male US civilians",
    },
    {
        "id": "LNU02073413",
        "series_name": "the number of employed native born US civilians",
    },
    {
        "id": "LNU02032208",
        "series_name": "the number of US civilians employed in natural resources, construction, and maintenance occupations",
    },
    {
        "id": "LNS12035019",
        "series_name": "the number of US civilians employed in nonagricultural industries",
    },
    {
        "id": "LNU02032207",
        "series_name": "the number of US civilians employed in office and administrative support occupations",
    },
    {
        "id": "LNS12032197",
        "series_name": "the number of US civilians employed part-time for economic reasons in nonagricultural industries",
    },
    {
        "id": "LNS12032199",
        "series_name": "the number of US civilians employed part-time for economic reasons in nonagricultural industries, who could only find part-time work",
    },
    {
        "id": "LNS12032200",
        "series_name": "the number of employed US civilians employed part-time for noneconomic reasons in nonagricultural industries",
    },
    {
        "id": "LNU02032213",
        "series_name": "the number of US civilians employed in production occupations",
    },
    {
        "id": "LNU02032212",
        "series_name": "the number of US civilians employed in production, transportation and material moving occupations",
    },
    {
        "id": "LNU02032203",
        "series_name": "the number of US civilians employed in professional and related occupations",
    },
    {
        "id": "LNU02032205",
        "series_name": "the number of US civilians employed in sales and office occupations",
    },
    {
        "id": "LNU02032206",
        "series_name": "the number of US civilians employed in sales and related occupations",
    },
    {
        "id": "LNU02032204",
        "series_name": "the number of US civilians employed in service occupations",
    },
    {
        "id": "LNU02048984",
        "series_name": "the number of incorporated self-employed US civilians",
    },
    {
        "id": "LNS12000003",
        "series_name": "the number of employed White US civilians",
    },
    {
        "id": "LNS12000002",
        "series_name": "the number of employed female US civilians",
    },
    {
        "id": "LNU02374597",
        "series_name": "the employment-population ratio for US civilians 16 years and older with a disability",
    },
    {
        "id": "LNU02375600",
        "series_name": "the employment-population ratio for US civilians 65 years and older with a disability",
    },
    {
        "id": "LNU02374593",
        "series_name": "the employment-population ratio for US civilians 16 years and older with no disability",
    },
    {
        "id": "LNU02375379",
        "series_name": "the employment-population ratio for US civilians 65 years and older with no disability",
    },
    {
        "id": "LNS12300002",
        "series_name": "the employment-population ratio for female US civilians",
    },
    {
        "id": "LNS12327689",
        "series_name": "the employment-population ratio for US civilians 25 years and older with some college or associate degree",
    },
    {
        "id": "LNS12327660",
        "series_name": "the employment-population ratio for US civilians 25 years and older with a high school diploma",
    },
    {
        "id": "LNS12327659",
        "series_name": "the employment-population ratio for US civilians 25 years and older with less than a high school diploma",
    },
    {
        "id": "EMRATIO",
        "series_name": "the employment-population ratio for US civilians",
    },
    {
        "id": "LNS12300012",
        "series_name": "the employment-population ratio for US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNS12300060",
        "series_name": "the employment-population ratio for US civilians between 25 and 54 years of age",
    },
    {
        "id": "LNU02332183",
        "series_name": "the employment-population ratio for Asian US civilians",
    },
    {
        "id": "LNS12327662",
        "series_name": "the employment-population ratio for US civilians 25 years and older with a Bachelor's degree and higher",
    },
    {
        "id": "LNS12300006",
        "series_name": "the employment-population ratio for Black or African American US civilians",
    },
    {
        "id": "LNU02373395",
        "series_name": "the employment-population ratio for foreign born US civilians",
    },
    {
        "id": "LNS12300009",
        "series_name": "the employment-population ratio for Hispanic or Latino US civilians",
    },
    {
        "id": "LNS12300001",
        "series_name": "the employment-population ratio for male US civilians",
    },
    {
        "id": "LNU02373413",
        "series_name": "the employment-population ratio for native born US civilians",
    },
    {
        "id": "LNS12300003",
        "series_name": "the employment-population ratio for White US civilians",
    },
    {
        "id": "LNU03074597",
        "series_name": "number of unemployed US civilians 16 years and older with a disability",
    },
    {
        "id": "LNU03075600",
        "series_name": "number of unemployed US civilians 65 years and older with a disability",
    },
    {
        "id": "LNU03074593",
        "series_name": "number of unemployed US civilians 16 years and older with no disability",
    },
    {
        "id": "LNU03075379",
        "series_name": "number of unemployed US civilians 65 years and older with no disability",
    },
    {
        "id": "UNEMPLOY",
        "series_name": "number of unemployed US civilians",
    },
    {
        "id": "LNS13000012",
        "series_name": "number of unemployed US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNS13000036",
        "series_name": "number of unemployed US civilians between 20 and 24 years of age",
    },
    {
        "id": "LNS13000089",
        "series_name": "number of unemployed US civilians between 25 and 34 years of age",
    },
    {
        "id": "TOTLU2564",
        "series_name": "number of unemployed US civilians between 25 and 64 years of age",
    },
    {
        "id": "TOTLU25O",
        "series_name": "number of unemployed US civilians 25 years and older",
    },
    {
        "id": "TOTLU3544",
        "series_name": "number of unemployed US civilians between 35 and 44 years of age",
    },
    {
        "id": "LNS13000093",
        "series_name": "number of unemployed US civilians between 45 and 54 years of age",
    },
    {
        "id": "TOTLU5564",
        "series_name": "number of unemployed US civilians between 55 and 64 years of age",
    },
    {
        "id": "TOTLU65O",
        "series_name": "number of unemployed US civilians 65 years and older",
    },
    {
        "id": "LNU03032183",
        "series_name": "number of unemployed Asian US civilians",
    },
    {
        "id": "ADEGU16O",
        "series_name": "number of unemployed US civilians 16 years and older with an associate degree",
    },
    {
        "id": "ADAPU16O",
        "series_name": "number of unemployed US civilians 16 years and older with an associate degree (academic program)",
    },
    {
        "id": "ADOPU16O",
        "series_name": "number of unemployed US civilians 16 years and older with an associate degree (occupational program)",
    },
    {
        "id": "CGRAU16O",
        "series_name": "number of unemployed US civilians 16 years and older with a Bachelor's degree or higher",
    },
    {
        "id": "LNS13000006",
        "series_name": "number of unemployed Black or African American US civilians",
    },
    {
        "id": "CGBDU16O",
        "series_name": "number of unemployed US civilians 16 years and older with a doctoral degree",
    },
    {
        "id": "CGMDU16O",
        "series_name": "number of unemployed US civilians 16 years and older with a Master's degree",
    },
    {
        "id": "LNU03073395",
        "series_name": "number of unemployed foreign born US civilians",
    },
    {
        "id": "HSGSU16O",
        "series_name": "number of unemployed US civilians 16 years and older with a high school diploma",
    },
    {
        "id": "LNS13000009",
        "series_name": "number of unemployed Hispanic or Latino US civilians",
    },
    {
        "id": "LNU03023705",
        "series_name": "number of unemployed US civilians who left (as opposed to lost) their job",
    },
    {
        "id": "LNU03023621",
        "series_name": "number of unemployed US civilians who lost (as opposed to left) their job",
    },
    {
        "id": "LNS13025699",
        "series_name": "number of unemployed US civilians who lost their job not on layoff",
    },
    {
        "id": "LNS13023653",
        "series_name": "number of unemployed US civilians who lost their job on layoff",
    },
    {
        "id": "LHSDU16O",
        "series_name": "number of unemployed US civilians 16 years and older with less than a high school diploma",
    },
    {
        "id": "LNS13100000",
        "series_name": "number of unemployed US civilians looking for full-time work",
    },
    {
        "id": "LNS13200000",
        "series_name": "number of unemployed US civilians looking for part-time work",
    },
    {
        "id": "LNS13000001",
        "series_name": "number of unemployed male US civilians",
    },
    {
        "id": "LNU03073413",
        "series_name": "number of unemployed native born US civilians",
    },
    {
        "id": "LNU03023569",
        "series_name": "number of unemployed US civilians who are new entrants",
    },
    {
        "id": "LNS13026638",
        "series_name": "number of permanently unemployed US civilians",
    },
    {
        "id": "LNS13026637",
        "series_name": "number of unemployed US civilians who completed temporary jobs",
    },
    {
        "id": "SCADU16O",
        "series_name": "number of unemployed US civilians 16 years and older with some college or associate degree",
    },
    {
        "id": "LNS13000003",
        "series_name": "number of unemployed White US civilians",
    },
    {
        "id": "LNS13000002",
        "series_name": "number of unemployed female US civilians",
    },
    {
        "id": "LNU03000313",
        "series_name": "number of unemployed female US civilians who maintain families",
    },
    {
        "id": "LNU04000006",
        "series_name": "the unemployement rate for Black or African American US civilians",
    },
    {
        "id": "CGBD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with a Bachelor's degree",
    },
    {
        "id": "CGDD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with a doctoral degree",
    },
    {
        "id": "CGMD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with a Master's degree",
    },
    {
        "id": "CGPD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with a professional degree",
    },
    {
        "id": "LNU04032240",
        "series_name": "the unemployement rate for US private wage and salary workers in education and health services",
    },
    {
        "id": "LNU04032233",
        "series_name": "the unemployement rate for US wage and salary workers in the durable goods industry",
    },
    {
        "id": "LNU04032231",
        "series_name": "the unemployement rate for US private wage and salary workers in the construction industry",
    },
    {
        "id": "LNU04032224",
        "series_name": "the unemployement rate for US civilians in construction and extraction occupations",
    },
    {
        "id": "LNU04032223",
        "series_name": "the unemployement rate for US civilians in farming, fishing, and forestry occupations",
    },
    {
        "id": "LNU04032238",
        "series_name": "the unemployement rate for US private wage and salary workers in the financial activities industry",
    },
    {
        "id": "LNU04073395",
        "series_name": "the unemployement rate for foreign born US civilians",
    },
    {
        "id": "LNS14100000",
        "series_name": "the unemployement rate for US full-time workers",
    },
    {
        "id": "HSGS16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with a high school diploma",
    },
    {
        "id": "LNU04000009",
        "series_name": "the unemployement rate for Hispanic or Latino US civilians",
    },
    {
        "id": "LNU04032237",
        "series_name": "the unemployement rate for US private wage and salary workers in the information industry",
    },
    {
        "id": "LNU04032225",
        "series_name": "the unemployement rate for US civilians in installation, maintenance, and repair occupations",
    },
    {
        "id": "LNS14023705",
        "series_name": "the unemployement rate for US civilians who left (as opposed to lost) their job",
    },
    {
        "id": "LNU04032241",
        "series_name": "the unemployement rate for US private wage and salary workers in leisure and hospitality",
    },
    {
        "id": "LHSD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with less than a high school diploma",
    },
    {
        "id": "LNU04032232",
        "series_name": "the unemployement rate for US private wage and salary workers in the manufacturing industry",
    },
    {
        "id": "LNU04032215",
        "series_name": "the unemployement rate for US civilians in management, professional, and related occupations",
    },
    {
        "id": "LNU04032216",
        "series_name": "the unemployement rate for US civilians in management, business, and financial operations occupations",
    },
    {
        "id": "LNS14000001",
        "series_name": "the unemployement rate for male US civilians",
    },
    {
        "id": "LNU04073413",
        "series_name": "the unemployement rate for native born US civilians",
    },
    {
        "id": "LNS14023569",
        "series_name": "the unemployement rate for US civilians who are new entrants",
    },
    {
        "id": "LNU04032229",
        "series_name": "the unemployement rate for US private wage and salary workers in nonagriculture occupations",
    },
    {
        "id": "LNU04032234",
        "series_name": "the unemployement rate for US private wage and salary workers in the non durable goods industry",
    },
    {
        "id": "LNU04032221",
        "series_name": "the unemployement rate for US civilians in office and administrative support occupations",
    },
    {
        "id": "LNS14200000",
        "series_name": "the unemployement rate for US part-time workers",
    },
    {
        "id": "LNU04032227",
        "series_name": "the unemployement rate for US civilians in production occupations",
    },
    {
        "id": "LNU04032239",
        "series_name": "the unemployement rate for US private wage and salary workers in the professional and business services industry",
    },
    {
        "id": "LNU04032226",
        "series_name": "the unemployement rate for US civilians in production, transportation and material moving occupations",
    },
    {
        "id": "LNU04032217",
        "series_name": "the unemployement rate for US civilians in professional and related occupations",
    },
    {
        "id": "LNS14023557",
        "series_name": "the unemployement rate for US reentrants to labor force",
    },
    {
        "id": "LNU04032219",
        "series_name": "the unemployement rate for US civilians in the sales and office occupations",
    },
    {
        "id": "LNU04032218",
        "series_name": "the unemployement rate for US civilians in service occupations",
    },
    {
        "id": "SCAD16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with some college or associate degree",
    },
    {
        "id": "LNU04032228",
        "series_name": "the unemployement rate for US civilians in transportation and material moving occupations",
    },
    {
        "id": "LNU04032236",
        "series_name": "the unemployement rate for US wage and salary workers in transportation and utilities industries",
    },
    {
        "id": "LNU04000003",
        "series_name": "the unemployement rate for White US civilians",
    },
    {
        "id": "LNU04075600",
        "series_name": "the unemployement rate for US civilians 65 years and older with a disability",
    },
    {
        "id": "LNU04074597",
        "series_name": "the unemployement rate for US civilians 16 years and older with a disability",
    },
    {
        "id": "LNU04074593",
        "series_name": "the unemployement rate for US civilians 16 years and older with no disability",
    },
    {
        "id": "LNU04075379",
        "series_name": "the unemployement rate for US civilians 65 years and older with no disability",
    },
    {
        "id": "LNS14000002",
        "series_name": "the unemployement rate for female US civilians",
    },
    {
        "id": "LNU04000313",
        "series_name": "the unemployement rate for female US civilians who maintain families",
    },
    {
        "id": "UNRATE",
        "series_name": "the unemployement rate for US civilian labor force",
    },
    {
        "id": "LNU04000012",
        "series_name": "the unemployement rate for US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNS14000036",
        "series_name": "the unemployement rate for US civilians between 20 and 24 years of age",
    },
    {
        "id": "LNS14000089",
        "series_name": "the unemployement rate for US civilians between 25 and 34 years of age",
    },
    {
        "id": "LNS14000060",
        "series_name": "the unemployement rate for US civilians between 25 and 54 years of age",
    },
    {
        "id": "TOTL2564",
        "series_name": "the unemployement rate for US civilians between 25 and 64 years of age",
    },
    {
        "id": "LNS14000091",
        "series_name": "the unemployement rate for US civilians between 35 and 44 years of age",
    },
    {
        "id": "LNS14000093",
        "series_name": "the unemployement rate for US civilians between 45 sand 54 years of age",
    },
    {
        "id": "LNU04000095",
        "series_name": "the unemployement rate for US civilians between 55 and 64 years of age",
    },
    {
        "id": "LNU04000097",
        "series_name": "the unemployement rate for US civilians 65 years and older",
    },
    {
        "id": "LNS14032183",
        "series_name": "the unemployement rate for Asian US civilians",
    },
    {
        "id": "ADEG16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with an associate degree",
    },
    {
        "id": "ADAP16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with an associate degree (academic program)",
    },
    {
        "id": "ADOP16O",
        "series_name": "the unemployement rate for US civilians 16 years and older with an associate degree (occupational program)",
    },
    {
        "id": "LNU05000003",
        "series_name": "the number of White US civilians not in the labor force",
    },
    {
        "id": "LNU05074597",
        "series_name": "the number of US civilians 16 years and older with a disability who are not in the labor force",
    },
    {
        "id": "LNU05075600",
        "series_name": "the number of US civilians 65 years and older with a disability who are not in the labor force",
    },
    {
        "id": "LNU05074593",
        "series_name": "the number of US civilians 16 years and older with no disability who are not in the labor force",
    },
    {
        "id": "LNU05075379",
        "series_name": "the number of US civilians 65 years and older with no disability who are not in the labor force",
    },
    {
        "id": "LNU05000002",
        "series_name": "the number of female US civilians not in the labor force",
    },
    {
        "id": "LNU05000001",
        "series_name": "the number of male US civilians not in the labor force",
    },
    {
        "id": "LNU05026640",
        "series_name": "the number of male US civilians not in the labor force who want a job now",
    },
    {
        "id": "LNU05026641",
        "series_name": "the number of female US civilians not in the labor force who want a job now",
    },
    {
        "id": "LNU05000009",
        "series_name": "the number of Hispanic or Latino US civilians not in the labor force",
    },
    {
        "id": "LNU05000006",
        "series_name": "the number of Black or African American US civilians not in the labor force",
    },
    {
        "id": "LNU05000012",
        "series_name": "the number of US civilians between 16 and 19 not in the labor force",
    },
    {
        "id": "LNU05000000",
        "series_name": "the number of US civilians not in the labor force",
    },
    {
        "id": "LNU05073395",
        "series_name": "the number of foreign born US civilians not in the labor force",
    },
    {
        "id": "LNU05032183",
        "series_name": "the number of Asian US civilians not in the labor force",
    },
    {
        "id": "LNU05073413",
        "series_name": "the number of native born US civilians not in the labor force",
    },
    {
        "id": "LNS11300012",
        "series_name": "the labor force participation rate of US civilians between 16 and 19 years of age",
    },
    {
        "id": "LNS11300006",
        "series_name": "the labor force participation rate of Black or African American US civilians",
    },
    {
        "id": "LNS11327662",
        "series_name": "the labor force participation rate of US civilians 25 years and older with a Bachelor's degree and higher",
    },
    {
        "id": "LNS11300003",
        "series_name": "the labor force participation rate of White US civilians",
    },
    {
        "id": "LNS11327660",
        "series_name": "the labor force participation rate of US civilians 25 years and older with a high school diploma",
    },
    {
        "id": "LNS11300009",
        "series_name": "the labor force participation rate of Hispanic or Latino US civilians",
    },
    {
        "id": "LNS11327689",
        "series_name": "the labor force participation rate of US civilians 25 years and older with some college or associate degree",
    },
    {
        "id": "LNS11300002",
        "series_name": "the labor force participation rate of female US civilians",
    },
    {
        "id": "LNS11300001",
        "series_name": "the labor force participation rate of male US civilians",
    },
    {
        "id": "LNS12026620",
        "series_name": "the percentage of employed US civilians who have more than one job",
    },
    {
        "id": "LNU02026631",
        "series_name": "the number of US civilians who have more than one full-time job",
    },
    {
        "id": "LNU02026625",
        "series_name": "the number of US civilians who have one full-time and at least one part-time job",
    },
    {
        "id": "LNS12026619",
        "series_name": "the number of US civilians who have more than one job",
    },
    {
        "id": "LNU02026628",
        "series_name": "the numer of US civilians who have at least two part-time jobs",
    },
    {
        "id": "LNU02026623",
        "series_name": "the number of female US civilians who have more than one job",
    },
    {
        "id": "LNU02026624",
        "series_name": "the percentage of employed, female US civilians who have more than one job",
    },
    {
        "id": "LNU02026622",
        "series_name": "the percentage of employed, male US civilians who have more than one job",
    },
    {
        "id": "LNU02026621",
        "series_name": "the number of male US civilians who have more than one job",
    },
    {
        "id": "UEMPMEAN",
        "series_name": "the average number of weeks that US civilians have been unemployed",
    },
    {
        "id": "LNU03008276",
        "series_name": "the median number of weeks that US civilians have been unemployed",
    },
    {
        "id": "LNU03008636",
        "series_name": "the number of US civilians who have been unemployed for 27 weeks or more",
    },
    {
        "id": "LNU03008396",
        "series_name": "the number of US civilians who have been unemployed for 5 weeks or less",
    },
    {
        "id": "LNS13025703",
        "series_name": "the percentage of unemployed US civilians who have been unemployed for 27 weeks or more",
    },
    {
        "id": "LNU03008756",
        "series_name": "the nubmer of US civilians who have been unemployed for 5 to 14 weeks",
    },
    {
        "id": "LNS13008397",
        "series_name": "the percentage of unemployed US civilians who have been unemployed for less than 5 weeks",
    },
    {
        "id": "LNS13023622",
        "series_name": "the percentage of unemployed US civilians who have lost (as opposed to left) their job",
    },
    {
        "id": "LNS13023706",
        "series_name": "the percentage of unemployed US civilians who have left (as opposed to lost) their job",
    },
    {
        "id": "LNS13023654",
        "series_name": "the percentage of unemployed US civilians who have lost their job on layoff",
    },
    {
        "id": "LNS13026511",
        "series_name": "the percentage of unemployed US civilians who have not lost their job on layoff",
    },
    {
        "id": "LNS13023558",
        "series_name": "the percentage of unemployed US civilians who are reentrants",
    },
    {
        "id": "LNS13023570",
        "series_name": "the percentage of unemployed US civilians who are new entrants",
    },
    {
        "id": "LNS17800000",
        "series_name": "the number of US civilians who went from 'employed' to 'not in labor force'",
    },
    {
        "id": "LNS17900000",
        "series_name": "the number of US civilians who went from 'unemployed' to 'not in labor force'",
    },
    {
        "id": "LNS17000000",
        "series_name": "the number of US civilians who remain employed",
    },
    {
        "id": "LNS17400000",
        "series_name": "the number of US civilians who went from 'employed' to 'unemployed'",
    },
    {
        "id": "LNS17100000",
        "series_name": "the number of US civilians who went from 'unemployed' to 'employed'",
    },
    {
        "id": "LNS17200000",
        "series_name": "the number of US civilians who went from 'not in labor force' to 'employed'",
    },
    {
        "id": "LNS17600000",
        "series_name": "the number of US civilians who went from 'not in labor force' to 'unemployed'",
    },
    {
        "id": "LNS17500000",
        "series_name": "the number of US civilians who remained unemployed",
    },
    {
        "id": "LNS18000000",
        "series_name": "the number of US civilians who remained 'not in labor force'",
    },
    {
        "id": "AWHAETP",
        "series_name": "the average weekly hours of US employees in the private sector",
    },
    {
        "id": "CES0600000010",
        "series_name": "the total number of female US employees in goods-producing businesses",
    },
    {
        "id": "CES1021100001",
        "series_name": "the total number of US employees in oil and gas extraction businesses",
    },
    {
        "id": "USMINE",
        "series_name": "the total number of US employees in mining and logging businesses",
    },
    {
        "id": "AWHAEMAL",
        "series_name": "the average weekly hours of US employees in mining and logging businesses",
    },
    {
        "id": "CEU1000000011",
        "series_name": "the average weekly earnings of US employees in mining and logging businesses",
    },
    {
        "id": "CEU1000000010",
        "series_name": "the total number of female US employees in mining and logging businesses",
    },
    {
        "id": "AWHAEGP",
        "series_name": "the average weekly hours of US employees in goods-producing businesses",
    },
    {
        "id": "AWHAECON",
        "series_name": "the average weekly hours of US employees in construction businesses",
    },
    {
        "id": "CES2000000039",
        "series_name": "the female US employees-to-all US employees ratio in construction businesses",
    },
    {
        "id": "MANEMP",
        "series_name": "the number of US employees in manufacturing",
    },
    {
        "id": "CES3000000010",
        "series_name": "the number of female US employees in manufacturing",
    },
    {
        "id": "AWHAEDG",
        "series_name": "the average weekly hours of US employees in durable goods businesses",
    },
    {
        "id": "DMANEMP",
        "series_name": "the total number of US employees in durable goods businesses",
    },
    {
        "id": "CES3133400001",
        "series_name": "the total number of US employees in computer and electronic product manufacturing",
    },
    {
        "id": "CES3133440001",
        "series_name": "the total number of US employees in semiconductor and other electronic component manufacturing",
    },
    {
        "id": "CES3133300001",
        "series_name": "the total number of US employees in machinery manufacturing",
    },
    {
        "id": "CES3132100001",
        "series_name": "the total number of US employees in wood product manufacturing",
    },
    {
        "id": "CES3133100001",
        "series_name": "the total number of US employees in primary metal manufacturing",
    },
    {
        "id": "CES3133660001",
        "series_name": "the total number of US employees in ship and boat building businesses",
    },
    {
        "id": "CEU3100000004",
        "series_name": "the average weekly overtime hours of US employees in durable goods businesses",
    },
    {
        "id": "CES3133420001",
        "series_name": "the total number of US employees in communications and equipment manufacturing",
    },
    {
        "id": "CES3100000010",
        "series_name": "the total number of female US employees in durable goods businesses",
    },
    {
        "id": "NDMANEMP",
        "series_name": "the total number of US employees in nondurable goods businesses",
    },
    {
        "id": "CES3231100001",
        "series_name": "the total number of US employees in food manufacturing",
    },
    {
        "id": "CES3232500001",
        "series_name": "the total number of US employees in chemical manufacturing",
    },
    {
        "id": "CES3231500001",
        "series_name": "the total number of US employees in apparel manufacturing",
    },
    {
        "id": "CES3231300001",
        "series_name": "the total number of US employees in textile mills",
    },
    {
        "id": "CES3232600001",
        "series_name": "the total number of US employees in plastics and rubber products manufacturing",
    },
    {
        "id": "CES3232400001",
        "series_name": "the total number of US employees in petroleum and coal products manufacturing",
    },
    {
        "id": "AWHAENDG",
        "series_name": "the average weekly hours of US employees in nondurable goods businesses",
    },
]
