"""Lightweight source metadata — no heavy deps.

Single source of truth for source identity strings and name lists.
Importable by any Cloud Run Job without triggering source-specific dependencies.
"""

from datetime import date

from _fb_types import NullifiedQuestion, SourceType
from helpers.constants import BENCHMARK_START_DATE_DATETIME_DATE

_B = BENCHMARK_START_DATE_DATETIME_DATE  # shorthand for nullification entries

SOURCE_METADATA = {
    "acled": {
        "source_type": SourceType.DATASET,
        "source_intro": (
            "The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on "
            "the locations, dates, actors, fatalities, and types of all reported political violence "
            "and protest events around the world. You're going to predict how questions based on "
            "this data will resolve."
        ),
        "resolution_criteria": (
            "Resolves to the value calculated from the ACLED dataset once the data is published."
        ),
    },
    "dbnomics": {
        "source_type": SourceType.DATASET,
        "source_intro": (
            "DBnomics collects data on topics such as population and living conditions, "
            "environment and energy, agriculture, finance, trade and others from publicly "
            "available resources, for example national and international statistical institutions, "
            "researchers and private companies. You're going to predict how questions based on "
            "this data will resolve."
        ),
        "resolution_criteria": "Resolves to the value found at {url} once the data is published.",
    },
    "fred": {
        "source_type": SourceType.DATASET,
        "source_intro": (
            "The Federal Reserve Economic Data database (FRED) provides economic data from "
            "national, international, public, and private sources.You're going to predict how "
            "questions based on this data will resolve."
        ),
        "resolution_criteria": "Resolves to the value found at {url} once the data is published.",
        "nullified_questions": [
            NullifiedQuestion(
                id="AMERIBOR",
                nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE,
            ),
        ],
    },
    "infer": {
        "source_type": SourceType.MARKET,
        "source_intro": (
            "We would like you to predict the outcome of a prediction market. A prediction "
            "market, in this context, is the aggregate of predictions submitted by users on the "
            "website INFER Public. You're going to predict the probability that the market will "
            "resolve as 'Yes'."
        ),
        "resolution_criteria": "Resolves to the outcome of the question found at {url}.",
    },
    "manifold": {
        "source_type": SourceType.MARKET,
        "source_intro": (
            "We would like you to predict the outcome of a prediction market. A prediction "
            "market, in this context, is the aggregate of predictions submitted by users on the "
            "website Manifold. You're going to predict the probability that the market will "
            "resolve as 'Yes'."
        ),
        "resolution_criteria": "Resolves to the outcome of the question found at {url}.",
    },
    "metaculus": {
        "source_type": SourceType.MARKET,
        "source_intro": (
            "We would like you to predict the outcome of a prediction market. A prediction "
            "market, in this context, is the aggregate of predictions submitted by users on the "
            "website Metaculus. You're going to predict the probability that the market will "
            "resolve as 'Yes'."
        ),
        "resolution_criteria": "Resolves to the outcome of the question found at {url}.",
    },
    "polymarket": {
        "source_type": SourceType.MARKET,
        "source_intro": (
            "We would like you to predict the outcome of a prediction market. A prediction "
            "market, in this context, is the aggregate of predictions submitted by users on the "
            "website Polymarket. You're going to predict the probability that the market will "
            "resolve as 'Yes'."
        ),
        "resolution_criteria": "Resolves to the outcome of the question found at {url}.",
        # IDs for which it is no longer possible to fetch data on Polymarket
        # (though it was once possible)
        "nullified_questions": [
            NullifiedQuestion(id=nid, nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE)
            for nid in sorted(
                {
                    "0x525820c5314f4143091d05079a8d810ecc07c8d5c8954ec2e6b6e163e40de9cb",
                    "0x9b46e4d85db0b2cd29acc36b836e1dad6cd2ac4fe495643cca64f7b962b6ab24",
                    "0x1e4d38c9b9e4aa154e350099216f4d86d94f1277eaa0d22fd33f48c0402155d5",
                    "0x738a551b7e2680669ea268911b2dc2079d156c350e40dc847d2a00eb0c57cfc2",
                    "0x0edd688013e4d08dd5367b9171bf85c6df73f2a4f561ed3c8ce004271c8278b7",
                    "0x42b4e02c1e95ca7b5e8610c3c1fad1dff6c0a46d01de6ae12565df026e3fc5a6",
                    "0x4afb076c5d9dfe1c33bf300cfd9fb93a5a8d9bfce8fe2beaeccbde5f8c269fc1",
                    "0x5642824719fa2e4d164de9a9ddaa1b5ca4f6fc57483eb222bec54082ad0bb57c",
                    "0xd8bf9a22e052cc97b14047a48552f3bd0e2605654e4fe580f48fa65e98d8487f",
                }
            )
        ],
    },
    "wikipedia": {
        "source_type": SourceType.DATASET,
        "source_intro": (
            "Wikipedia is an online encyclopedia created and edited by volunteers. You're going "
            "to predict how questions based on data sourced from Wikipedia will resolve."
        ),
        "resolution_criteria": "Resolves to the value calculated from {url} on the resolution date.",
        "nullified_questions": [
            # Name changed: "R. Vaishali" --> "Vaishali Rameshbabu"
            NullifiedQuestion(
                id="149b5a465d9640ee10afcd1c6dde90627a4b58918111c14455d369f304aae454",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="98e72a2d4c6daa0b0d8aee1d02a8628bbacf713f0e44b02f80a12b1dae1c618f",
                nullification_start_date=_B,
            ),  # noqa: B950
            # Name changed: "Erigaisi Arjun" --> "Arjun Erigaisi"
            NullifiedQuestion(
                id="b70970a0440d1b7dedde9220fb60ffe3f2ed8b00ef12b45341772046caa12092",
                nullification_start_date=_B,
            ),  # noqa: B950
            # Rameshbabu Praggnanandhaa — too many repeated name changes
            # NB: _not_ nullifying ff153a13... (first asked 2025-05-25) or a987eef3... (2025-03-30)
            NullifiedQuestion(
                id="7687186d5e0807f8925a694beafb3d6e057978a9a01f0d1a3e0eaf1a49959e78",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="479a40c45087510f72ee43a77aaccf78d563361728151ed3aab9b2b186db0b72",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="4b9175c88f855ee0d0fc54640158fc7da10b7b2dcc4fe1053bd180ac1a72bf39",
                nullification_start_date=_B,
            ),  # noqa: B950
            # Virus common name changed from "Monkeypox" to "Mpox"
            NullifiedQuestion(
                id="f9323386a651ce67fc0da31285bee22a4ec53b8a2ea5220431ecb4560fb44c77",
                nullification_start_date=date(2022, 8, 21),
            ),  # noqa: B950
            NullifiedQuestion(
                id="3f04d0cfccd38b26e86c0939516c483eb31edf6aaa3a1eaaabe38a48f7a0996a",
                nullification_start_date=date(2022, 8, 21),
            ),  # noqa: B950
            # Leinier Domínguez Pérez — too many repeated name changes
            NullifiedQuestion(
                id="c8cc0816ce50a7fc018eccb7e6ed19628dc1f56e1cda26aca4b8f09c4edc7beb",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="21f7534aaa7292ba1e71ed0d1ce0fc350febe64414083b4b60d35765781eab35",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="9ab6734c6bf88f28a8c71b9d73995541b351f2663a7d8331a2c56dd5116d78a3",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="a9783d8184c3f43668cc21417788be00fd4ff70eec91064c5539ed5ebb0019e8",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="fa118e263e1218af8bb24cf7f6dd1c68e179d430584adf5b9b37d1b8488932d8",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="60d86f26a5b1e6576d218076ae7a66bf0fadc0bfe042ff1adf875918cc8d2781",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="6f8a3d10d39d69ecbdb10db2fabb66d852af39b95ce1af9f48ce5d9fd0175d87",
                nullification_start_date=_B,
            ),  # noqa: B950
            NullifiedQuestion(
                id="dfa2dc6d7511437365132459a03e4d7bc10632ffd78c145fb98496699647f968",
                nullification_start_date=_B,
            ),  # noqa: B950
            # Resolved keys from _TRANSFORM_ID_MAPPING — old erroneous IDs superseded
            # Tatjana Schoenmaker, lost swimming WR
            NullifiedQuestion(
                id="25891a351e97154028edc8075558470a6ec21d6d37dbd75f74268ee1b48253bf",
                nullification_start_date=date(2023, 7, 5),
            ),  # noqa: B950
            NullifiedQuestion(
                id="94297b75a6d18445c35a179a860b810bf0be7b6f296c502cec7caab24c8c1775",
                nullification_start_date=date(2023, 7, 5),
            ),  # noqa: B950
            # Anthony Ervin, lost swimming WR
            NullifiedQuestion(
                id="cf02d516cc8b14b7b2880baae0ca4d520b167fe271123e6adfeedaefb83a3ec5",
                nullification_start_date=date(2023, 8, 8),
            ),  # noqa: B950
            NullifiedQuestion(
                id="6358ab9dab0aa4b6fc2abe8aacf1b31c8cbed08d54557eb4982c230fe19fe774",
                nullification_start_date=date(2023, 8, 8),
            ),  # noqa: B950
            # Michael Phelps, lost swimming WR
            NullifiedQuestion(
                id="eea4cb0741c001c18ec28a58f64fb02bfba72e776f2d9ef2257309269b119526",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            NullifiedQuestion(
                id="234175128275d109b5ffe5f8a30f863f150051e892e56566f88936b961be1f2f",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            # Benedetta Pilato, lost swimming WR
            NullifiedQuestion(
                id="e4afa18eb3d8d08fbc37c114f876a93ddceac453da415512ef5d73c7d26f391d",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            NullifiedQuestion(
                id="747aa3406023deab8175b051bac64b55c061d38c2aebc73c1ded759de7b0477a",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            # Zac Stubblety-Cook, lost swimming WR
            NullifiedQuestion(
                id="5b078ec5a0d0a51c3668c62fe93441bd177ad4c58a1ff1d50b62a8bf6bc609fe",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            NullifiedQuestion(
                id="afd040f28eb27f973ba1dc2cfeb3f613a7c29a543b14cbab4ba8d44ca8eb0d36",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            # Federica Pellegrini, lost swimming WR
            NullifiedQuestion(
                id="6e295dc29db5dce0672097160d432e7a3af469317298cb3153d745b2270041f1",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            NullifiedQuestion(
                id="f0054684e6c6c24c5595e5cdf8498ffc5479e82d26a8b0318af35a26cd9b9ce7",
                nullification_start_date=date(2023, 8, 29),
            ),  # noqa: B950
            # Liu Xiang, lost swimming WR
            NullifiedQuestion(
                id="245eb0146484bad467bbdb3d0c871f30390fb1a902105f86c85ec4637c52a9f4",
                nullification_start_date=date(2023, 10, 20),
            ),  # noqa: B950
            NullifiedQuestion(
                id="e222aa0998ad2e53a4cbfbdb11f3d80dfd13a263b4748e4a6cd8f4b965f0506f",
                nullification_start_date=date(2023, 10, 20),
            ),  # noqa: B950
            # Hunter Armstrong, lost swimming WR
            NullifiedQuestion(
                id="851337578d0bf07dc60b233f5ef2a49d0309c1728621dd7b4ac0724414887fde",
                nullification_start_date=date(2023, 11, 13),
            ),  # noqa: B950
            NullifiedQuestion(
                id="56e00c66d9d2bfa3dd3ad0656c81701e04033438f90320ba96a63b62e61a4ea5",
                nullification_start_date=date(2023, 11, 13),
            ),  # noqa: B950
            # David Popovici, lost swimming WR
            NullifiedQuestion(
                id="646cd3619a16c273007816e559834682e19754dcaf7d0ecb6ffebe64d351f177",
                nullification_start_date=date(2024, 3, 21),
            ),  # noqa: B950
            NullifiedQuestion(
                id="0e0f5a6cf1ac926657d43b909af4d2fb27ba975dfe3a274fbe0930dcf667d499",
                nullification_start_date=date(2024, 3, 21),
            ),  # noqa: B950
            # Mollie O'Callaghan, lost swimming WR
            NullifiedQuestion(
                id="ebb4e1e85bed81266e94dda8e84eafe1479d5697f850792d84b5fab7251f483f",
                nullification_start_date=date(2024, 7, 18),
            ),  # noqa: B950
            NullifiedQuestion(
                id="b4c4989ac25edfbb8510e8ffa9aeee70c0de0d82e22a360faac590304f67c575",
                nullification_start_date=date(2024, 7, 18),
            ),  # noqa: B950
            # Sun Yang, lost swimming WR
            NullifiedQuestion(
                id="7558c5b4f539cc922552c4f18a9a5cdaccbc100d6108acf117e886bd9dc67857",
                nullification_start_date=date(2024, 8, 4),
            ),  # noqa: B950
            NullifiedQuestion(
                id="04bfcc27745a1813367fcb5aad43423db616dccff54c1cc929bd32de3f43a38a",
                nullification_start_date=date(2024, 8, 4),
            ),  # noqa: B950
            # Kate Douglass, lost swimming WR
            NullifiedQuestion(
                id="eaf10e98fdc5ddd2227b212f1e446a1937a2e0529b8f89c9a2528cb469e7cc27",
                nullification_start_date=date(2024, 11, 2),
            ),  # noqa: B950
            NullifiedQuestion(
                id="c539c3ef6d2534204b4fc67a94b14eebc7c51f141fea3c30f337cb3ede390b11",
                nullification_start_date=date(2024, 11, 2),
            ),  # noqa: B950
            # Katinka Hosszú, lost swimming WR
            NullifiedQuestion(
                id="2e88b046538e239140043da9471c2b4894615a12173c3a52ee707321acf2ed8d",
                nullification_start_date=date(2025, 6, 10),
            ),  # noqa: B950
            NullifiedQuestion(
                id="c4db6cf85ef3ef4165705b863f1491f2903df3a2534e2d4e25f57edcbdfaac4b",
                nullification_start_date=date(2025, 6, 10),
            ),  # noqa: B950
            # Vaccine was created in 2023 but Wikipedia table had not been updated
            NullifiedQuestion(
                id="242926fea271734ef8d4920e532414b38dbfdf301516fd9f0c988abd0ce777dd",
                nullification_start_date=_B,
            ),  # noqa: B950
            # Ryan Lochte, lost swimming WR
            NullifiedQuestion(
                id="12486c21df689124f8fdad70760247dffe2b7696599748bcb5c7a738735285d5",
                nullification_start_date=date(2025, 7, 30),
            ),  # noqa: B950
            NullifiedQuestion(
                id="c6ee39b4504603aa5ddbe73f378d48d94ab128406e5dd1bbb70ead0207a43840",
                nullification_start_date=date(2025, 7, 30),
            ),  # noqa: B950
        ],
    },
    "yfinance": {
        "source_type": SourceType.DATASET,
        "source_intro": (
            "Yahoo Finance provides financial data on stocks, bonds, and currencies and also "
            "offers news, commentary and tools for personal financial management. You're going "
            "to predict how questions based on this data will resolve."
        ),
        "resolution_criteria": (
            "Resolves to the market close price at {url} for the resolution date. If the "
            "resolution date coincides with a day the market is closed (weekend, holiday, etc.) "
            "the previous market close price is used."
        ),
    },
}

ALL_SOURCE_NAMES = sorted(SOURCE_METADATA.keys())
DATASET_SOURCE_NAMES = sorted(
    name for name, m in SOURCE_METADATA.items() if m["source_type"] == SourceType.DATASET
)
MARKET_SOURCE_NAMES = sorted(
    name for name, m in SOURCE_METADATA.items() if m["source_type"] == SourceType.MARKET
)
