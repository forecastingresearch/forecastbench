"""Resolution Criteria."""

acled = "Resolves to the value calculated from the ACLED dataset once the data is published."
yfinance = "Resolves to the value found on Yahoo! Finance for the resolution date once he data is published."
wikipedia = "Resolves to the value found on Wikipedia on the date of resolution."
market = (
    "Resolves to the market value on {f_string_value} at 11:59:59PM UTC "
    "on the resolution date(s)."
)
metaculus = (
    "Resolves to the simplified history community prediction on {f_string_value} at 11:59:59PM UTC "
    "on the resolution date(s)."
)
infer = (
    "Resolves to the community prediction on {f_string_value} at 11:59:59PM UTC "
    "on the resolution date(s)."
)
