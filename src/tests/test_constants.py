from helpers import constants


def test_logo_lookup_lives_with_leaderboard_code():
    assert not hasattr(constants, "get_org_logo")
    assert not hasattr(constants, "ORG_TO_LOGO")
    assert not hasattr(constants, "EXTERNAL_TOURNAMENT_MODELS_TO_LOGO")
