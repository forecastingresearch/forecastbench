"""Polymarket-specific variables."""

SOURCE_INTRO = (
    "We would like you to predict the outcome of a prediction market. A prediction market, in this "
    "context, is the aggregate of predictions submitted by users on the website Polymarket. "
    "You're going to predict the probability that the market will resolve as 'Yes'."
)

RESOLUTION_CRITERIA = "Resolves to the outcome of the question found at {url}."


# These are question IDs for which it is no longer possible to fetch data on Polymarket
# (though it was once possible)
NULLIFIED_QUESTION_IDS = {
    "0x525820c5314f4143091d05079a8d810ecc07c8d5c8954ec2e6b6e163e40de9cb",
    "0x9b46e4d85db0b2cd29acc36b836e1dad6cd2ac4fe495643cca64f7b962b6ab24",
    "0x1e4d38c9b9e4aa154e350099216f4d86d94f1277eaa0d22fd33f48c0402155d5",
    "0x738a551b7e2680669ea268911b2dc2079d156c350e40dc847d2a00eb0c57cfc2",
    "0x0edd688013e4d08dd5367b9171bf85c6df73f2a4f561ed3c8ce004271c8278b7",
    "0x42b4e02c1e95ca7b5e8610c3c1fad1dff6c0a46d01de6ae12565df026e3fc5a6",
    "0x4afb076c5d9dfe1c33bf300cfd9fb93a5a8d9bfce8fe2beaeccbde5f8c269fc1",
    "0x5642824719fa2e4d164de9a9ddaa1b5ca4f6fc57483eb222bec54082ad0bb57c",
}
