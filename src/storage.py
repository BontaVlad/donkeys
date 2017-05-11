from datetime import datetime
from elasticsearch_dsl import (DocType, Date, Integer, Float, Keyword, Boolean,
                               GeoPoint, Text, analyzer)
from elasticsearch_dsl.connections import connections


connections.create_connection(hosts=['localhosts'])

html_strip = analyzer(
    'html_strip', tokenizer="standard",
    filter=['standard', 'lowercase', 'stop', 'snowball'],
    char_filter=["html_strip"]
)


class Record(DocType):
    url = Text()
    title = Text(fields={'raw': Keyword()})
    address = Text(fields={'raw': Keyword()})
    location = GeoPoint(lat_lon=True)
    contract_type = Keyword()
    description = Text(analyzer=html_strip)
    extra = Text(analyzer=html_strip)
    building_type = Text(fields={'raw': Keyword()})
    structure_materials = Text(fields={'raw': Keyword()})
    agency_broker = Text(fields={'raw': Keyword()})
    num_of_rooms = Integer()
    num_of_kitchens = Integer()
    num_of_bathrooms = Integer()
    built_year = Integer()
    build_surface_area = Float()
    usable_surface_area = Float()
    price = Integer()
    currency = Keyword()
    created_at = Date()
    added_at = Date()

    class Meta:
        index = 'imobiliare'

    def save(self, **kwargs):
        self.created_at = datetime.now()
        return super().save(**kwargs)


Record.init()
