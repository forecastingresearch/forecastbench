"""Wikipedia question source."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import ClassVar

import numpy as np
import pandas as pd

from _types import NullifiedQuestion, SourceType
from helpers import constants, dates

from ._dataset import DatasetSource

logger = logging.getLogger(__name__)


class QuestionType(Enum):
    """Comparison types for Wikipedia questions."""

    SAME = 0
    SAME_OR_MORE = 1
    MORE = 2
    ONE_PERCENT_MORE = 3
    SAME_OR_LESS = 4


# Old question IDs that map to replacement IDs.  # noqa: B950
_TRANSFORM_ID_MAPPING = {
    "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92": "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f",  # noqa: B950
    "eb5bcf6a467ca2b850a28b95d51c5b58d314f909d72afdd66935f2e28d8334a3": "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f",  # noqa: B950
    "8702851a2593fcd3d2587481a2fcb623e775e3cbfe86abad2641bb34a13138ce": "c097d5216ff8068a20a5c9592860a003ccc06dd4eb7da842d86c3816a68c3ab6",  # noqa: B950
    "91dd441b57571c8d23b83a40d11c4a9a87d55eb3948f034e3e5e778f1f0b98c6": "c097d5216ff8068a20a5c9592860a003ccc06dd4eb7da842d86c3816a68c3ab6",  # noqa: B950
    "0bce98434da73edce73e9570b99dac301d39b224d49946908ee34673b5e0e4d1": "793b2cd84b35aaf26c07464c21690ad171f2168f639513b9883d63234e515e03",  # noqa: B950
    "7a100cc5019c37fd083618aa560229e4ce1011f5cace5b6d0e6817b6a40b3ffa": "c28f340263644425dd87c3cf915351620e452358d2118a20e27fb20ba76cfa64",  # noqa: B950
    "d2cfcce09363ddad01df31559624e330557f69eabcab39ed3734c11a60f153c7": "a987eef385663d96115ba6c113ffb3dc7e83affdcaa8c53421220e4e9e1f95f8",  # noqa: B950
    "3ff636ffa947b8f0f3adb55964cd75294716abea2c27933ad89d7abff42d633e": "127f33fb2530ea03d3af0420afc5e0f283b23503e3dc7ff0ccd8e84dfd241f49",  # noqa: B950
    "7c17d34e37d8cea481d3933f4e1c2c091bd523c3980043e539cde90fbc08f29a": "828ceafd45d4bef413280614944d4e2d579fc83e089592d8d9e363c3b58b44d2",  # noqa: B950
    "b2d7953344ca7b2fd37c2ef0d9664053da9a26d774fcd4c6e315e74340bd6bf0": "e79a0c5058411513ad3fa8c65448fdf41c27f83f9fbafe4d1ac58bebfd713bde",  # noqa: B950
    "9fc87840aa0bebfcd6c03cdfbcdb1b6a11120bdb0419d0d9334301cf536a88fd": "ba4008b18e2ef8ad82fc8ea5f066d464ce8e45133d0e620bae27b2ce740d4c1d",  # noqa: B950
    "7558c5b4f539cc922552c4f18a9a5cdaccbc100d6108acf117e886bd9dc67857": "04bfcc27745a1813367fcb5aad43423db616dccff54c1cc929bd32de3f43a38a",  # noqa: B950
    "5ca04a3a78c7ffe5817f080f95e883a83edd6a1471caba48d435448a2d879b52": "fe29236219f59bc35575d70c4b8f2897eaaac32a87f31c9abe56e0a251de0663",  # noqa: B950
    "72a33a32e409997da4782833a3893e503ff84ad71005d422f9c2e00dd193350a": "cc6923cbc7b882a64ae1d99ab4bda02ed0268393dbeab1b5b173bd05aab73a57",  # noqa: B950
    "61c0fb3703e68cee2439afd5c2d71522bc6649a1fa154491f58981456fa8ab68": "42f335ea171402fb761bd367e7ca94292a52b2cbebe4f2edacf23b87552bd5d6",  # noqa: B950
    "926083f9ae268e48beea5516d8b48024d0a4d5ae7b5ef0c7d18f205dfc831b90": "1e5f84dd79c1a731f71ef1c7100fd66791275e78f266c27ae6c1568847c087db",  # noqa: B950
    "c90a910e5ea0ef3bdaedf23ce591e20a8da2df5b88c5b04e6264761959ddfcc0": "9d14f6afd960fcf12aaeaf741bab57e7f2e5002c5a72d3035f7db8cf98fbdee9",  # noqa: B950
    "7be763022f7a4e8a84a4c78d8934b9f47dd708514a9373a810892a34a679254e": "12af44da7b699297e8be3140315e693ca414ef693010ceb078ef92700ce6d998",  # noqa: B950
    "3c37fef353460bfd130fde0117638badaee913ee8c79b8cdf4c35e2c5710126a": "690617e9a2ad8ed147767aa6dd0220d0c05026291f2ad92eaf42dee14f0873e2",  # noqa: B950
    "12486c21df689124f8fdad70760247dffe2b7696599748bcb5c7a738735285d5": "c6ee39b4504603aa5ddbe73f378d48d94ab128406e5dd1bbb70ead0207a43840",  # noqa: B950
    "cb3336ed4ee8ebe8364f97814f75d9777ef8dd30a8f775c29ac727372fcd14be": "203bdb0b73fd156109cbb2227e92d16a0e84ee5d86f71b22bf7ded1d9bd8a924",  # noqa: B950
    "ebb4e1e85bed81266e94dda8e84eafe1479d5697f850792d84b5fab7251f483f": "b4c4989ac25edfbb8510e8ffa9aeee70c0de0d82e22a360faac590304f67c575",  # noqa: B950
    "7221cc24a88774591bc4c40046c92d692c12d8bc1b63c39c3f295522e9181c57": "3835c0448587f4c28471e27f597c6f7ae89d4060a8e634cca21f899dcd057925",  # noqa: B950
    "c846c6eb73a939076d4054972dfb1149cc8b3bac31526882171d6b9ff87a7adb": "6cd2092339ebef0efad5610588facdec6c4d0f9c2607791034f44dbc7ae86f4c",  # noqa: B950
    "558aebef0c3c95c9559e54f27e8fa908da02917d9833abb9a2330d10b1b2b953": "6e5c5efbd814430094697396276bd121a7f941eede08d4f108b4c5ecb3590458",  # noqa: B950
    "d563befa9ded6fdc765857663fceeb546cd5b983b2de7850615c037880a25390": "48728daeb0c53c76232dc0d9a1c8f8efaeafd9dd2d725feb2bd5d81b5dd5af10",  # noqa: B950
    "d5e1fda224104cd3bac9ce8ab4b4face291793306f3bcf515c4be96b4fef9f7e": "5b5cca41a01c8cbec95d84aac92bd4f4c91dd4627bfd0ceeff720f5a53c8ba31",  # noqa: B950
    "50ff16043ba629140f82ada741c6f24245bb98b8eefd7d55aac10f750d2d43ad": "37d0cf293cc84f9388f9f3a032f90ef55c8549c0f7bbcdab6a426553af31f128",  # noqa: B950
    "4651d16e683bc20fe0c0a04dcbb52fddf2982ec3df016a52643e5aee291b37ce": "5096dbb513988ae3252e7621e1754277d56b95703074341c54a080a0b7821571",  # noqa: B950
    "eaf10e98fdc5ddd2227b212f1e446a1937a2e0529b8f89c9a2528cb469e7cc27": "c539c3ef6d2534204b4fc67a94b14eebc7c51f141fea3c30f337cb3ede390b11",  # noqa: B950
    "646cd3619a16c273007816e559834682e19754dcaf7d0ecb6ffebe64d351f177": "0e0f5a6cf1ac926657d43b909af4d2fb27ba975dfe3a274fbe0930dcf667d499",  # noqa: B950
    "851337578d0bf07dc60b233f5ef2a49d0309c1728621dd7b4ac0724414887fde": "56e00c66d9d2bfa3dd3ad0656c81701e04033438f90320ba96a63b62e61a4ea5",  # noqa: B950
    "245eb0146484bad467bbdb3d0c871f30390fb1a902105f86c85ec4637c52a9f4": "e222aa0998ad2e53a4cbfbdb11f3d80dfd13a263b4748e4a6cd8f4b965f0506f",  # noqa: B950
    "5b078ec5a0d0a51c3668c62fe93441bd177ad4c58a1ff1d50b62a8bf6bc609fe": "afd040f28eb27f973ba1dc2cfeb3f613a7c29a543b14cbab4ba8d44ca8eb0d36",  # noqa: B950
    "eea4cb0741c001c18ec28a58f64fb02bfba72e776f2d9ef2257309269b119526": "234175128275d109b5ffe5f8a30f863f150051e892e56566f88936b961be1f2f",  # noqa: B950
    "6e295dc29db5dce0672097160d432e7a3af469317298cb3153d745b2270041f1": "f0054684e6c6c24c5595e5cdf8498ffc5479e82d26a8b0318af35a26cd9b9ce7",  # noqa: B950
    "e4afa18eb3d8d08fbc37c114f876a93ddceac453da415512ef5d73c7d26f391d": "747aa3406023deab8175b051bac64b55c061d38c2aebc73c1ded759de7b0477a",  # noqa: B950
    "cf02d516cc8b14b7b2880baae0ca4d520b167fe271123e6adfeedaefb83a3ec5": "6358ab9dab0aa4b6fc2abe8aacf1b31c8cbed08d54557eb4982c230fe19fe774",  # noqa: B950
    "25891a351e97154028edc8075558470a6ec21d6d37dbd75f74268ee1b48253bf": "94297b75a6d18445c35a179a860b810bf0be7b6f296c502cec7caab24c8c1775",  # noqa: B950
    "1d0989190ba1a2a4b3f3738f02e6dd5f463afc712d7507c8a89d7f971d4c27e4": "2c42ee57b6879cdf61bb608b564eef91d4b3a2642392527bbc8532502029e906",  # noqa: B950
    "831a289e8d494cce6ac96eb97eadd8a2d80ec3d7e406ae440bad864583a12adb": "6e13696b1502516e89fa7bea8d3df930959ecd772498363bb019ea562b70533f",  # noqa: B950
    "20efdec28913ecfb1e3a3e26ad2c99e1b4d7ad3f43b5a6202c46f9c277c17406": "0ba788cac9fef02dae3b7b3713a085306f9c7e1d321ad7dc9e2473666f65b6c3",  # noqa: B950
    "6797a1d0a791aa20ab4de7d1a465a06b24e6c8100ec1a796c306f47e612923be": "ad589ed82fb268fd2dec1dda10d211b2e82fcfa86cd526e44ba8e20e81265176",  # noqa: B950
    "7233fa748a364e7e93f1899c23ff71571fb3e78b55a1bd951648209211af3cc7": "0742587fdf80b228a9f77c97b4dc0aea2ef60598138b2d08a161d153ae59c9b7",  # noqa: B950
    "2e88b046538e239140043da9471c2b4894615a12173c3a52ee707321acf2ed8d": "c4db6cf85ef3ef4165705b863f1491f2903df3a2534e2d4e25f57edcbdfaac4b",  # noqa: B950
    "2b6d5c38b8ee7751461358ec55a5fa80040f996b824eee281e00ac6593133cef": "211fd2e3e651f5c5de584e5b3ec89049347d6ca1f5ff4a15440835f105a6047c",  # noqa: B950
    "d8ef6ba516706b350a1a40149914034def70217a6152904b4b7be5b9c4c64ce5": "d32864887ea4fba0a850c9da3588265b82b23098d8fdded2be8f2b8cd584329d",  # noqa: B950
    "f9323386a651ce67fc0da31285bee22a4ec53b8a2ea5220431ecb4560fb44c77": "3f04d0cfccd38b26e86c0939516c483eb31edf6aaa3a1eaaabe38a48f7a0996a",  # noqa: B950
}

_IDS_TO_NULLIFY = [
    {
        "id": "149b5a465d9640ee10afcd1c6dde90627a4b58918111c14455d369f304aae454",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "98e72a2d4c6daa0b0d8aee1d02a8628bbacf713f0e44b02f80a12b1dae1c618f",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "b70970a0440d1b7dedde9220fb60ffe3f2ed8b00ef12b45341772046caa12092",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "7687186d5e0807f8925a694beafb3d6e057978a9a01f0d1a3e0eaf1a49959e78",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "479a40c45087510f72ee43a77aaccf78d563361728151ed3aab9b2b186db0b72",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "4b9175c88f855ee0d0fc54640158fc7da10b7b2dcc4fe1053bd180ac1a72bf39",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "f9323386a651ce67fc0da31285bee22a4ec53b8a2ea5220431ecb4560fb44c77",
        "nullify_start_date": datetime(2022, 8, 21).date(),
    },
    {
        "id": "3f04d0cfccd38b26e86c0939516c483eb31edf6aaa3a1eaaabe38a48f7a0996a",
        "nullify_start_date": datetime(2022, 8, 21).date(),
    },
    {
        "id": "c8cc0816ce50a7fc018eccb7e6ed19628dc1f56e1cda26aca4b8f09c4edc7beb",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "21f7534aaa7292ba1e71ed0d1ce0fc350febe64414083b4b60d35765781eab35",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "9ab6734c6bf88f28a8c71b9d73995541b351f2663a7d8331a2c56dd5116d78a3",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "a9783d8184c3f43668cc21417788be00fd4ff70eec91064c5539ed5ebb0019e8",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "fa118e263e1218af8bb24cf7f6dd1c68e179d430584adf5b9b37d1b8488932d8",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "60d86f26a5b1e6576d218076ae7a66bf0fadc0bfe042ff1adf875918cc8d2781",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "6f8a3d10d39d69ecbdb10db2fabb66d852af39b95ce1af9f48ce5d9fd0175d87",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "dfa2dc6d7511437365132459a03e4d7bc10632ffd78c145fb98496699647f968",
        "nullify_start_date": constants.BENCHMARK_START_DATE_DATETIME_DATE,
    },
    {
        "id": "25891a351e97154028edc8075558470a6ec21d6d37dbd75f74268ee1b48253bf",
        "nullify_start_date": datetime(2023, 7, 5).date(),
    },
    {
        "id": "94297b75a6d18445c35a179a860b810bf0be7b6f296c502cec7caab24c8c1775",
        "nullify_start_date": datetime(2023, 7, 5).date(),
    },
    {
        "id": "cf02d516cc8b14b7b2880baae0ca4d520b167fe271123e6adfeedaefb83a3ec5",
        "nullify_start_date": datetime(2023, 8, 8).date(),
    },
    {
        "id": "6358ab9dab0aa4b6fc2abe8aacf1b31c8cbed08d54557eb4982c230fe19fe774",
        "nullify_start_date": datetime(2023, 8, 8).date(),
    },
    {
        "id": "eea4cb0741c001c18ec28a58f64fb02bfba72e776f2d9ef2257309269b119526",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "234175128275d109b5ffe5f8a30f863f150051e892e56566f88936b961be1f2f",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "e4afa18eb3d8d08fbc37c114f876a93ddceac453da415512ef5d73c7d26f391d",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "747aa3406023deab8175b051bac64b55c061d38c2aebc73c1ded759de7b0477a",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "5b078ec5a0d0a51c3668c62fe93441bd177ad4c58a1ff1d50b62a8bf6bc609fe",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "afd040f28eb27f973ba1dc2cfeb3f613a7c29a543b14cbab4ba8d44ca8eb0d36",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "6e295dc29db5dce0672097160d432e7a3af469317298cb3153d745b2270041f1",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "f0054684e6c6c24c5595e5cdf8498ffc5479e82d26a8b0318af35a26cd9b9ce7",
        "nullify_start_date": datetime(2023, 8, 29).date(),
    },
    {
        "id": "245eb0146484bad467bbdb3d0c871f30390fb1a902105f86c85ec4637c52a9f4",
        "nullify_start_date": datetime(2023, 10, 20).date(),
    },
    {
        "id": "e222aa0998ad2e53a4cbfbdb11f3d80dfd13a263b4748e4a6cd8f4b965f0506f",
        "nullify_start_date": datetime(2023, 10, 20).date(),
    },
    {
        "id": "851337578d0bf07dc60b233f5ef2a49d0309c1728621dd7b4ac0724414887fde",
        "nullify_start_date": datetime(2023, 11, 13).date(),
    },
    {
        "id": "56e00c66d9d2bfa3dd3ad0656c81701e04033438f90320ba96a63b62e61a4ea5",
        "nullify_start_date": datetime(2023, 11, 13).date(),
    },
    {
        "id": "646cd3619a16c273007816e559834682e19754dcaf7d0ecb6ffebe64d351f177",
        "nullify_start_date": datetime(2024, 3, 21).date(),
    },
    {
        "id": "0e0f5a6cf1ac926657d43b909af4d2fb27ba975dfe3a274fbe0930dcf667d499",
        "nullify_start_date": datetime(2024, 3, 21).date(),
    },
    {
        "id": "ebb4e1e85bed81266e94dda8e84eafe1479d5697f850792d84b5fab7251f483f",
        "nullify_start_date": datetime(2024, 7, 18).date(),
    },
    {
        "id": "b4c4989ac25edfbb8510e8ffa9aeee70c0de0d82e22a360faac590304f67c575",
        "nullify_start_date": datetime(2024, 7, 18).date(),
    },
    {
        "id": "7558c5b4f539cc922552c4f18a9a5cdaccbc100d6108acf117e886bd9dc67857",
        "nullify_start_date": datetime(2024, 8, 4).date(),
    },
    {
        "id": "04bfcc27745a1813367fcb5aad43423db616dccff54c1cc929bd32de3f43a38a",
        "nullify_start_date": datetime(2024, 8, 4).date(),
    },
    {
        "id": "eaf10e98fdc5ddd2227b212f1e446a1937a2e0529b8f89c9a2528cb469e7cc27",
        "nullify_start_date": datetime(2024, 11, 2).date(),
    },
    {
        "id": "c539c3ef6d2534204b4fc67a94b14eebc7c51f141fea3c30f337cb3ede390b11",
        "nullify_start_date": datetime(2024, 11, 2).date(),
    },
    {
        "id": "2e88b046538e239140043da9471c2b4894615a12173c3a52ee707321acf2ed8d",
        "nullify_start_date": datetime(2025, 6, 10).date(),
    },
    {
        "id": "c4db6cf85ef3ef4165705b863f1491f2903df3a2534e2d4e25f57edcbdfaac4b",
        "nullify_start_date": datetime(2025, 6, 10).date(),
    },
    {
        "id": "12486c21df689124f8fdad70760247dffe2b7696599748bcb5c7a738735285d5",
        "nullify_start_date": datetime(2025, 7, 30).date(),
    },
    {
        "id": "c6ee39b4504603aa5ddbe73f378d48d94ab128406e5dd1bbb70ead0207a43840",
        "nullify_start_date": datetime(2025, 7, 30).date(),
    },
]

# Wikipedia page configs: id_root identifies the page, question_type determines comparison logic.
_PAGES = [
    {"id_root": "FIDE_rankings_elo_rating", "question_type": QuestionType.ONE_PERCENT_MORE},
    {"id_root": "FIDE_rankings_ranking", "question_type": QuestionType.SAME_OR_LESS},
    {"id_root": "List_of_world_records_in_swimming", "question_type": QuestionType.SAME},
    {"id_root": "List_of_infectious_diseases", "question_type": QuestionType.MORE},
]


class WikipediaSource(DatasetSource):
    """Wikipedia dataset source with custom row-by-row resolution logic."""

    name: ClassVar[str] = "wikipedia"
    display_name: ClassVar[str] = "Wikipedia"
    source_type: ClassVar[SourceType] = SourceType.DATASET
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(id=entry["id"], nullification_start_date=entry["nullify_start_date"])
        for entry in _IDS_TO_NULLIFY
    ]

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve Wikipedia questions row by row."""
        logger.info("Resolving Wikipedia questions.")

        dfr = self._ffill_dfr(dfr)

        yesterday = pd.Timestamp(dates.get_date_yesterday())
        mask = df["resolution_date"] <= yesterday
        for index, row in df[mask].iterrows():
            forecast_due_date = row["forecast_due_date"].date()
            resolution_date = row["resolution_date"].date()
            if not self._is_combo(row):
                value = self._resolve_single_question(
                    mid=row["id"],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
            else:
                value1 = self._resolve_single_question(
                    mid=row["id"][0],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
                value2 = self._resolve_single_question(
                    mid=row["id"][1],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
                value = self._combo_change_sign(
                    value1, row["direction"][0]
                ) * self._combo_change_sign(value2, row["direction"][1])
            df.at[index, "resolved_to"] = float(value)
        df.loc[mask, "resolved"] = True
        return df

    def _resolve_single_question(self, mid, dfr, forecast_due_date, resolution_date):
        """Resolve an individual Wikipedia question by comparing values at two dates.

        Nullification is handled by
        BaseSource.resolve() which strips nullified rows before calling _resolve().
        """
        mid = self._transform_id(mid)
        d = self._id_unhash(mid)
        if d is None:
            logger.error(f"Wikipedia: could NOT unhash {mid}")
            return np.nan

        def get_value(dfr, mid, date):
            value = dfr[(dfr["id"] == mid) & (dfr["date"].dt.date == date)]["value"]
            return value.iloc[0] if not value.empty else None

        forecast_due_date_value = get_value(dfr, mid, forecast_due_date)
        resolution_date_value = get_value(dfr, mid, resolution_date)

        if forecast_due_date_value is None:
            logger.info(
                f"Nullifying Wikipedia market {mid}. "
                "The forecast question resolved between the freeze date and the forecast due date."
            )
            return np.nan

        question_type = [q["question_type"] for q in _PAGES if q["id_root"] == d["id_root"]]
        if len(question_type) != 1:
            logger.error(
                f"Nullifying Wikipedia market {mid}. Couldn't find comparison type "
                "(should not arrive here)."
            )
            return np.nan

        return self._compare_values(
            question_type=question_type[0],
            resolution_date_value=resolution_date_value,
            forecast_due_date_value=forecast_due_date_value,
        )

    @staticmethod
    def _compare_values(question_type, resolution_date_value, forecast_due_date_value):
        """Compare resolution-date and due-date values according to the question type."""
        if question_type == QuestionType.SAME:
            return resolution_date_value == forecast_due_date_value
        elif question_type == QuestionType.SAME_OR_MORE:
            return resolution_date_value >= forecast_due_date_value
        elif question_type == QuestionType.SAME_OR_LESS:
            return resolution_date_value <= forecast_due_date_value
        elif question_type == QuestionType.MORE:
            return resolution_date_value > forecast_due_date_value
        elif question_type == QuestionType.ONE_PERCENT_MORE:
            return resolution_date_value >= forecast_due_date_value * 1.01
        else:
            raise ValueError("Invalid QuestionType")

    @staticmethod
    def _ffill_dfr(dfr):
        """Forward-fill resolution values to yesterday for all IDs."""
        dfr = dfr.sort_values(by=["id", "date"])
        dfr = dfr.drop_duplicates(subset=["id", "date"])
        yesterday = dates.get_date_yesterday()
        yesterday = pd.Timestamp(yesterday)
        chunks = []
        for unique_id in dfr["id"].unique():
            temp_df = (
                dfr[dfr["id"] == unique_id].set_index("date").resample("D").ffill().reset_index()
            )
            if temp_df["date"].max() < yesterday:
                last_value = temp_df.iloc[-1]["value"]
                additional_days = pd.date_range(
                    start=temp_df["date"].max() + timedelta(days=1), end=yesterday
                )
                additional_df = pd.DataFrame(
                    {"date": additional_days, "id": unique_id, "value": last_value}
                )
                temp_df = pd.concat([temp_df, additional_df])
            chunks.append(temp_df)
        dfr = pd.concat(chunks).sort_values(by=["id", "date"]).reset_index(drop=True)
        return dfr

    @staticmethod
    def _transform_id(wid):
        """Map deprecated question IDs to their replacement IDs."""
        new_id = _TRANSFORM_ID_MAPPING.get(wid)
        if new_id is not None:
            logger.info(f"In wikipedia._transform_id(): Transforming {wid} --> {new_id}.")
            return new_id
        return wid

    # ------------------------------------------------------------------
    # Hash mapping
    # ------------------------------------------------------------------

    def populate_hash_mapping(self, raw_json: str) -> None:
        """Parse hash mapping from raw JSON string."""
        self.hash_mapping = json.loads(raw_json) if raw_json else {}

    def dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON, removing deprecated keys first."""
        for k in _TRANSFORM_ID_MAPPING:
            self.hash_mapping.pop(k, None)
        return json.dumps(self.hash_mapping, indent=4)

    def _id_hash(self, id_root: str, id_field_value: str) -> str:
        """Encode wikipedia Ids and store in hash_mapping."""
        d = {"id_root": id_root, "id_field_value": id_field_value}
        dictionary_json = json.dumps(d, sort_keys=True)
        hash_key = hashlib.sha256(dictionary_json.encode()).hexdigest()
        self.hash_mapping[hash_key] = d
        return hash_key

    def _id_unhash(self, hash_key: str):
        """Look up the original question dict, applying ID transform first."""
        hash_key = self._transform_id(hash_key)
        return self.hash_mapping.get(hash_key)
