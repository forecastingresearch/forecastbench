"""ACLED-specific variables."""

from . import constants

FETCH_COLUMN_DTYPE = {
    "event_id_cnty": str,
    "event_date": str,
    "iso": int,
    "region": str,
    "country": str,
    "admin1": str,
    "event_type": str,
    "fatalities": int,
    "timestamp": str,
}
FETCH_COLUMNS = list(FETCH_COLUMN_DTYPE.keys())

QUESTION_FILE_COLUMN_DTYPE = {
    **constants.QUESTION_FILE_COLUMN_DTYPE,
    "lhs_func": str,
    "lhs_args": object,  # <dict>
    "comparison_operator": str,
    "rhs_func": str,
    "rhs_args": object,  # <dict>
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

BACKGROUND = """
ACLED classifies events into six distinct categories:

1. Battles: violent interactions between two organized armed groups at a particular time and
   location;
2. Protests: in-person public demonstrations of three or more participants in which the participants
   do not engage in violence, though violence may be used against them;
3. Riots: violent events where demonstrators or mobs of three or more engage in violent or
   destructive acts, including but not limited to physical fights, rock throwing, property
   destruction, etc.;
4. Explosions/Remote violence: incidents in which one side uses weapon types that, by their nature,
   are at range and widely destructive;
5. Violence against civilians: violent events where an organized armed group inflicts violence upon
   unarmed non-combatants; and
6. Strategic developments: contextually important information regarding incidents and activities of
   groups that are not recorded as any of the other event types, yet may trigger future events or
   contribute to political dynamics within and across states.

Detailed information about the categories can be found at:
https://acleddata.com/knowledge-base/codebook/#acled-events
"""

SOURCE_INTRO = (
    "The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the "
    "locations, dates, actors, fatalities, and types of all reported political violence and "
    "protest events around the world. You’re going to predict the probability of the following "
    "potential outcome we’ve come up with about some of the data ACLED tracks."
)

RESOLUTION_CRITERIA = (
    "Resolves to the value calculated from the ACLED dataset once the data is published."
)
