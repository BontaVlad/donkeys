from datetime import datetime
from elasticsearch_dsl import (DocType, Date, Integer, Float, Keyword, Boolean,
                               GeoPoint, Text, analyzer)
from elasticsearch_dsl.connections import connections


connections.create_connection(hosts=['localhost'])

html_strip = analyzer(
    'html_strip', tokenizer="standard",
    filter=['standard', 'lowercase', 'stop', 'snowball'],
    char_filter=["html_strip"]
)


class Record(DocType):
    url = Text()
    title = Text(fields={'raw': Keyword()})
    address = Text(fields={'raw': Keyword()})
    location = GeoPoint()
    contract_type = Keyword()
    description = Text(analyzer=html_strip)
    extra = Text(analyzer=html_strip)
    building_type = Text(fields={'raw': Keyword()})
    structure_materials = Text(fields={'raw': Keyword()})
    agency_broker = Text(fields={'raw': Keyword()})
    compartiment = Keyword()
    num_of_rooms = Integer()
    num_of_kitchens = Integer()
    num_of_bathrooms = Integer()
    built_year = Integer()
    floor = Keyword()
    build_surface_area = Integer()
    usable_surface_area = Integer()
    price = Integer()
    height_category = Keyword()
    currency = Keyword()
    created_at = Date()
    added_at = Date()

    class Meta:
        index = 'imobiliare'

    def save(self, **kwargs):
        self.created_at = datetime.now()
        return super().save(**kwargs)


Record.init()
