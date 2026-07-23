"""
shared helpers for rebalancing detection.
imported by both the eda notebook and feature engineering notebook.
"""

BATCH_THRESHOLD  = 3  # any route: 3+ bikes same A->B same scrape = rebalancing
UPHILL_THRESHOLD = 2  # known uphill routes: even 2 bikes is very unlikely organic

# weekday rush hours where batch detection produces too many false positives.
# a ~52-min scraper gap means independent commuters arriving separately all
# appear with the same arrive_time — indistinguishable from a truck move.
# company rebalancing during rush hour is also rare (trucks avoid traffic).
PEAK_HOURS_WEEKDAY = frozenset({8, 9, 10, 11, 17, 18, 19})

# station pairs that go significantly uphill based on prague geography.
# a truck move is the obvious explanation for 2+ bikes on these routes
# outside of peak hours.
UPHILL_PAIRS = frozenset([
    # → hradcanska (letna hill ~225m), sources are riverside/flat at ~175-190m
    ("P7 - Maroldovo panoráma",                        "P6 - Hradčanská (výstup E6)"),
    ("P7 - Za elektrárnou (vstup do Stromovky)",        "P6 - Hradčanská (výstup E6)"),
    ("P6-Praha Podbaba",                                "P6 - Hradčanská (výstup E6)"),
    ("P1 - Haštalské náměstí",                         "P6 - Hradčanská (výstup E6)"),
    ("P7 - Tusarova x Komunardů",                      "P6 - Hradčanská (výstup E6)"),
    ("P7 - Portík",                                     "P6 - Hradčanská (výstup E6)"),
    ("P7 - Nádraží Holešovice 3",                      "P6 - Hradčanská (výstup E6)"),
    ("P7 - Sportovní areál Loděnice",                  "P6 - Hradčanská (výstup E6)"),
    ("P7 - Maroldovo panoráma",                        "P7-Kostelní - Technické Muzeum"),
    # → zizkov plateau (~260m) from karlin valley (~190m)
    ("P8 - roh Křižíkova/ Thámova",                    "P3-Slavíkova - Kubelíkova"),
    ("P8 - roh Křižíkova/ Thámova",                    "P3-Milešovská - Ondříčkova"),
    ("P8 - roh Křižíkova/ Thámova",                    "P3-Náměstí Barikád - Černínova"),
    ("P8 - Křižíkova 2",                               "P3-Náměstí Barikád - Černínova"),
    ("P8 - Prvního pluku / Pernerova",                 "P3-Slavíkova - Kubelíkova"),
    ("P4 - Přístaviště (u zábradlí)",                  "P3 - Olšanská x Ke Kapslovně"),
    # → dejvice/ntk (~220m) from podbaba valley (~175m)
    ("P6-Praha Podbaba",                               "P6-Národní technická knihovna"),
    ("P6-Praha Podbaba",                               "P6-Vítězné náměstí"),
])


def flag_rebalancing(trips):
    """
    add rebalancing columns to a trips dataframe. returns a copy.

    new columns: hour_local, dow_local, is_night_move, is_peak_hour, batch_size,
                 is_batch_move, is_uphill_pair, is_rebalancing.
    expects: from_station, to_station, arrive_time, name_from, name_to, bike_id.
    """
    df = trips.copy()

    if "hour_local" not in df.columns:
        df["hour_local"] = df["arrive_time"].dt.tz_convert("Europe/Prague").dt.hour
    if "dow_local" not in df.columns:
        df["dow_local"] = df["arrive_time"].dt.tz_convert("Europe/Prague").dt.dayofweek

    df["is_night_move"] = df["hour_local"].between(1, 5)

    # peak hours on weekdays: scraper gaps make commuters look like truck moves
    df["is_peak_hour"] = (
        ~df["dow_local"].isin([5, 6]) &
        df["hour_local"].isin(PEAK_HOURS_WEEKDAY)
    )

    df["batch_size"] = df.groupby(
        ["from_station", "to_station", "arrive_time"]
    )["bike_id"].transform("count")

    df["is_batch_move"] = (df["batch_size"] >= BATCH_THRESHOLD) & ~df["is_peak_hour"]

    pairs = list(zip(df["name_from"], df["name_to"]))
    df["is_uphill_pair"] = [p in UPHILL_PAIRS for p in pairs]

    # batch_size >= 3 outside peak hours OR batch_size >= 2 on a known uphill route outside peak hours
    df["is_rebalancing"] = df["is_batch_move"] | (
        df["is_uphill_pair"] & (df["batch_size"] >= UPHILL_THRESHOLD) & ~df["is_peak_hour"]
    )

    return df
