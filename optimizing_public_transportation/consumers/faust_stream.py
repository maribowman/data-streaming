"""Defines trends calculations for stations"""
import logging
from abc import ABC
from dataclasses import dataclass

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record, ABC):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record, ABC):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
source_topic = app.topic("postgres_cta_stations", value_type=Station)
sink_topic = app.topic("faust.cta.stations.transformed", value_type=TransformedStation, partitions=1)
table = app.Table(
    "transformed_stations_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=sink_topic,
)


@app.agent(source_topic)
async def transform_stations(stations):
    async for station in stations:
        line = "unknown"
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"

        table[station.station_id] = TransformedStation(station_id=station.station_id,
                                                       station_name=station.station_name,
                                                       order=station.order,
                                                       line=line)


if __name__ == "__main__":
    app.main()
