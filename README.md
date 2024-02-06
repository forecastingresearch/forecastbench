# Proof of Concept for LLM Benchmark project using GPT

### Outline
1. Select pertinent questions from Manifold by hand from the endpoint shown in `src/api_manifold_markets.py`.
1. Hard coding these into `manifold_questions.py`, `main.py` does the following:
   1. Query a custom GPT news query Assistant to rephrase the forecast question as a query to News API
   1. Pass the generated query to News API to get relevant news articles
   1. Query a custom GPT forecasting Assistant to provide a forecast given the forecast question, Manifold market value, and news articles.
   1. Save the Manifold market value, GPT forecast, and GPT forecast reasoning to a JSON file.
1. Create Plotly charts from JSON file in `src/create_charts.py`

---
### To run
1. Set the following variables in `src/.env`
   1. `OPENAI_API_KEY`
   1. `OPENAI_NEWS_QUERY_ASSISTANT_ID`
   1. `OPENAI_FORECASTING_ASSISTANT_ID`
   1. `NEWS_API_KEY`
1. `python3 -m venv venv`
1. `pip install -r requirements.txt`
1. `python api_manifold_markets.py` (only necessary to generate new questions). Copy selected questions into `manifold_questions.py`
1. `python main.py`
1. `python create_charts.py`
1. open `table_of_contents.html`

---
### Contributions

* Before pushing to this repo, please run `make lint` and fix any errors/warnings.
---

### Hand picked questions from Manifold using `src/api_manifold_markets.py`. (manifold topics as section headers)
#### movies
- `7n8J9UqjVkeeASTbnzi0` Will Oppenheimer Win the Oscars for Best Picture, Best Actor (Cillian Murphy) and Best Director (Christopher Nolan)?
- `UY00HaONtwy8Z8JbejwP` Will Count Fenring be a speaking character in Dune 2 (2024)?
- `WmvX4e2VMFyq3ULMuN91` Will Emma Stone win the Oscar for Best Actress at the 96th Academy Awards?

#### technology-default
- `TPLKhjts2rLerqUEJY2E` Will the S&P 500 be up on March 1st at the close of trading
- `KSafefdwvUFNpsTEdH9R` Will Code Llama 70B beat GPT-4 on llm coding benchmarks by March 1st?
- `6cbXRDKISHYHz4UtvDZO` Will Palworld sell more units than the Nintendo 64 (32.93M) by the end of Mario Day (March 10th)?

#### geopolitics
- `6dY0lYsivjha12rcYS8v` Will Sweden join NATO before the end of the Ides of March? (March 15)
- `6m9DWW3D2KYvvdkNLyB1` Will Indian troops leave the Maldives by March 15?
- `1qyrPbXTF4RsiDC5v5gC` Will Saudi Arabia agree to sell oil to China in Yuan by March 15th 2024?

#### us-politics
- `McWq1tz7AlO1dYzvko33` Will Elon Musk change his Twitter profile description before the end of Feb 2024?
- `w9jgAa254klUcufknfTP` Will thé NASDAQ reach an All time high by April 1st?
- `Bcx7cdWMVZBUG9VF1TOg` Will any Democratic Primary National poll in Feb 2024 show >85% support for Biden?
