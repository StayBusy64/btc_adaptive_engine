from __future__ import annotations

# All session boundary constants below are expressed in UTC hours (0-23).
#
# Session reference times (UTC):
#   Asia session    : 00:00 – 08:00  (Tokyo open 00:00, Hong Kong/Singapore 01:00)
#   London session  : 07:00 – 16:00  (London open 07:00, close 16:30 rounded to 16)
#   New York session: 12:00 – 21:00  (NYSE open 13:30 EST = 18:30 UTC; pre-market
#                                      activity begins ~12:00 UTC)
#   London/NY overlap: 12:00 – 16:00
#
# "Opening window" = first 60 minutes of each major session.
#   Asia opening  : 00:00 – 01:00
#   London opening: 07:00 – 08:00
#   NY opening    : 12:00 – 13:00
#
# "Midday" = UTC 08:00 – 12:00 (Asian session close / pre-London/pre-NY lull)
# "Late"   = UTC 20:00 – 23:59 (after NY primary hours)
#
# "High activity" covers London + NY hours minus the midday lull, i.e.
#   07:00 – 20:00 UTC.
#
# All times use the UTC timestamp embedded in the bar row.  If a bar carries
# no parseable timestamp the engine returns sensible zero / neutral defaults.

from __future__ import annotations

import math

from backend.feature_contract import FeatureContext, FeatureMap, FeatureSpec

# --------------------------------------------------------------------------- #
# Session boundary constants (UTC hours, inclusive start / exclusive end).
# --------------------------------------------------------------------------- #

ASIA_START: int = 0
ASIA_END: int = 8  # exclusive

LONDON_START: int = 7
LONDON_END: int = 16  # exclusive

NEW_YORK_START: int = 12
NEW_YORK_END: int = 21  # exclusive

OVERLAP_START: int = 12  # London / NY overlap
OVERLAP_END: int = 16  # exclusive

# Opening windows (first 60 min of each major session)
OPENING_HOURS: tuple[tuple[int, int], ...] = (
    (ASIA_START, ASIA_START + 1),
    (LONDON_START, LONDON_START + 1),
    (NEW_YORK_START, NEW_YORK_START + 1),
)

MIDDAY_START: int = 8
MIDDAY_END: int = 12  # exclusive

LATE_START: int = 20
LATE_END: int = 24  # exclusive — covers 20:00 – 23:59

HIGH_ACTIVITY_START: int = 7
HIGH_ACTIVITY_END: int = 20  # exclusive


class SessionContextEngine:
    name = "session_context_engine"

    def specs(self) -> tuple[FeatureSpec, ...]:
        return (
            FeatureSpec(self.name, "session_asia_flag", "int", "1 when bar timestamp falls inside Asia session (UTC 00:00–08:00)", "flag"),
            FeatureSpec(self.name, "session_london_flag", "int", "1 when bar timestamp falls inside London session (UTC 07:00–16:00)", "flag"),
            FeatureSpec(self.name, "session_new_york_flag", "int", "1 when bar timestamp falls inside New York session (UTC 12:00–21:00)", "flag"),
            FeatureSpec(self.name, "session_overlap_flag", "int", "1 when bar falls inside London/NY overlap (UTC 12:00–16:00)", "flag"),
            FeatureSpec(self.name, "session_opening_window_flag", "int", "1 when bar falls within the first 60 min of Asia, London, or NY session", "flag"),
            FeatureSpec(self.name, "session_midday_flag", "int", "1 when bar falls in midday lull window (UTC 08:00–12:00)", "flag"),
            FeatureSpec(self.name, "session_late_flag", "int", "1 when bar falls in late-session window (UTC 20:00–23:59)", "flag"),
            FeatureSpec(self.name, "hour_of_day_normalized", "float", "UTC hour of day mapped to 0.0–1.0 (0 = midnight, 1 = just before midnight)", "ratio"),
            FeatureSpec(self.name, "day_of_week_normalized", "float", "Day of ISO week mapped to 0.0–1.0 (Mon=0/7, Sun=6/7)", "ratio"),
            FeatureSpec(self.name, "high_activity_window_flag", "int", "1 when bar falls during London or NY primary hours (UTC 07:00–20:00)", "flag"),
        )

    def compute(self, context: FeatureContext) -> FeatureMap:
        _defaults: FeatureMap = {
            "session_asia_flag": 0,
            "session_london_flag": 0,
            "session_new_york_flag": 0,
            "session_overlap_flag": 0,
            "session_opening_window_flag": 0,
            "session_midday_flag": 0,
            "session_late_flag": 0,
            "hour_of_day_normalized": 0.0,
            "day_of_week_normalized": 0.0,
            "high_activity_window_flag": 0,
        }

        if not context.bars:
            return _defaults

        latest = context.bars[-1]
        timestamp_raw = latest.get("timestamp")

        dt = _parse_utc_timestamp(timestamp_raw)
        if dt is None:
            return _defaults

        utc_hour = dt.hour
        utc_minute = dt.minute
        # fractional hour for sub-hour checks (opening window uses whole hours
        # but the normalisation uses full precision)
        fractional_hour = utc_hour + utc_minute / 60.0

        asia_flag = int(ASIA_START <= utc_hour < ASIA_END)
        london_flag = int(LONDON_START <= utc_hour < LONDON_END)
        ny_flag = int(NEW_YORK_START <= utc_hour < NEW_YORK_END)
        overlap_flag = int(OVERLAP_START <= utc_hour < OVERLAP_END)

        opening_flag = int(
            any(start <= utc_hour < end for start, end in OPENING_HOURS)
        )

        midday_flag = int(MIDDAY_START <= utc_hour < MIDDAY_END)
        late_flag = int(LATE_START <= utc_hour < LATE_END)
        high_activity_flag = int(HIGH_ACTIVITY_START <= utc_hour < HIGH_ACTIVITY_END)

        # Normalise hour to [0, 1).  23:59 maps to ≈0.9993; midnight to 0.0.
        hour_normalized = fractional_hour / 24.0

        # ISO weekday: Monday=1 … Sunday=7.  Map to [0, 1) so Monday = 0/7 ≈ 0.
        iso_weekday = dt.isoweekday()  # 1..7
        day_normalized = (iso_weekday - 1) / 7.0

        return {
            "session_asia_flag": asia_flag,
            "session_london_flag": london_flag,
            "session_new_york_flag": ny_flag,
            "session_overlap_flag": overlap_flag,
            "session_opening_window_flag": opening_flag,
            "session_midday_flag": midday_flag,
            "session_late_flag": late_flag,
            "hour_of_day_normalized": round(hour_normalized, 6),
            "day_of_week_normalized": round(day_normalized, 6),
            "high_activity_window_flag": high_activity_flag,
        }


# --------------------------------------------------------------------------- #
# Private helper
# --------------------------------------------------------------------------- #

def _parse_utc_timestamp(raw: object) -> "datetime | None":  # noqa: F821
    """Return a UTC-aware :class:`datetime` from a raw timestamp value, or
    ``None`` if the value cannot be parsed.

    Supported formats
    -----------------
    * ISO-8601 strings with or without timezone offset, e.g.
      ``"2026-01-01T12:34:00+00:00"``, ``"2026-01-01T12:34:00Z"``,
      ``"2026-01-01T12:34:00"``.
    * Unix epoch integers or floats (seconds since 1970-01-01 UTC).
    """
    from datetime import datetime, timezone

    if raw is None:
        return None

    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    if not isinstance(raw, str):
        return None

    raw = raw.strip()
    if not raw:
        return None

    # Normalise the "Z" suffix that Python's fromisoformat doesn't accept
    # prior to Python 3.11.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    # If the string carried no timezone info, assume UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt
