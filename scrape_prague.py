{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNB/yihncaTlT2ukDT0/ixa",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/vinechai/nextbike/blob/main/scrape_prague.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "myucA29OU1vO"
      },
      "outputs": [],
      "source": [
        "import requests\n",
        "import pandas as pd\n",
        "from datetime import datetime, timezone\n",
        "from pathlib import Path\n",
        "\n",
        "PRAGUE_CITY_ID = 661\n",
        "DATA_DIR = Path(\"data\")\n",
        "DATA_DIR.mkdir(exist_ok=True)\n",
        "\n",
        "LIVE_DATA_URL = \"https://maps.nextbike.net/maps/nextbike-live.flatjson\"\n",
        "\n",
        "\n",
        "def append_parquet(df, path):\n",
        "    if path.exists():\n",
        "        old = pd.read_parquet(path)\n",
        "        df = pd.concat([old, df], ignore_index=True)\n",
        "    df.to_parquet(path, index=False)\n",
        "\n",
        "\n",
        "def scrape_prague_once():\n",
        "    resp = requests.get(LIVE_DATA_URL)\n",
        "    resp.raise_for_status()\n",
        "    data = resp.json()\n",
        "\n",
        "    scrape_time = datetime.now(timezone.utc)\n",
        "\n",
        "    # -------------------------------------------\n",
        "    # 1. STATIONS (FROM places)\n",
        "    # -------------------------------------------\n",
        "    stations = [\n",
        "        place for place in data[\"places\"]\n",
        "        if place.get(\"city_id\") == PRAGUE_CITY_ID\n",
        "    ]\n",
        "\n",
        "    stations_df = pd.json_normalize(stations)\n",
        "    stations_df[\"scrape_time\"] = scrape_time\n",
        "\n",
        "    stations_df.to_parquet(DATA_DIR / \"stations_latest.parquet\", index=False)\n",
        "\n",
        "    # -------------------------------------------\n",
        "    # 2. BIKES (EXTRACTED FROM stations)\n",
        "    # -------------------------------------------\n",
        "    bike_rows = []\n",
        "\n",
        "    for st in stations:\n",
        "        bike_numbers = st.get(\"bike_numbers\")\n",
        "        if not bike_numbers:\n",
        "            continue\n",
        "\n",
        "        # convert \"485396,481489\" → [\"485396\", \"481489\"]\n",
        "        bikes = [b.strip() for b in bike_numbers.split(\",\")]\n",
        "\n",
        "        for bike_id in bikes:\n",
        "            bike_rows.append({\n",
        "                \"bike_id\": bike_id,\n",
        "                \"station_uid\": st[\"uid\"],\n",
        "                \"station_name\": st[\"name\"],\n",
        "                \"lat\": st[\"lat\"],\n",
        "                \"lng\": st[\"lng\"],\n",
        "                \"scrape_time\": scrape_time\n",
        "            })\n",
        "\n",
        "    bikes_df = pd.DataFrame(bike_rows)\n",
        "\n",
        "    bikes_df.to_parquet(DATA_DIR / \"bikes_latest.parquet\", index=False)\n",
        "    append_parquet(bikes_df, DATA_DIR / \"bikes_history.parquet\")\n",
        "\n",
        "    print(f\"[OK] Scraped Prague at {scrape_time}\")\n",
        "    print(f\"     Stations: {len(stations)}\")\n",
        "    print(f\"     Bikes: {len(bikes_df)}\")\n",
        "\n",
        "\n",
        "if __name__ == \"__main__\":\n",
        "    scrape_prague_once()"
      ]
    }
  ]
}