"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict, dataclass

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
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
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
source_topic = app.topic("postgres_cta_stations", value_type=Station)
sink_topic = app.topic("faust.transformed.cta.stations", partitions=1)
table = app.Table(
    "faust.transformed.cta.stations.table",
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
        logger.info(f"station in faust with color:{line}")
        table[station.station_id] = asdict(TransformedStation(station_id=station.station_id,
                                                              station_name=station.station_name,
                                                              order=station.order,
                                                              line=line))


if __name__ == "__main__":
    app.main()
