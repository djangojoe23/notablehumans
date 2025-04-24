import json
import os

from django.core.management.base import BaseCommand
from tqdm import tqdm  # 👈 Add this import
from datetime import datetime

from ...models import NotableHuman


class Command(BaseCommand):
    help = "Export NotableHumans with valid birth locations to GeoJSON"

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=None, help='Limit number of humans to export')

    def handle(self, *args, **options):
        limit = options['limit']

        humans = NotableHuman.objects.filter(
            birth_year__isnull=False, birth_place__latitude__isnull=False, birth_place__longitude__isnull=False
        ).distinct()

        if limit:
            humans = humans[:limit]

        def abbreviate_properties(human):
            mapping = {
                "wikidata_id": "id",
                "wikipedia_url": "wu",
                "name": "n",
                "description": "d",
                "birth_year": "by",
                "birth_date": "bd",
                "birth_place_name": "bp",
                "death_year": "dy",
                "death_date": "dd",
                "death_place_name": "dp",
                "article_created_date": "cd",
                "article_length": "al",
                "article_recent_views": "rv",
                "article_total_edits": "te",
                "article_recent_edits": "re",
            }

            props = {}

            # Handle direct fields
            for full_key, short_key in mapping.items():
                val = getattr(human, full_key, None)
                if val is not None:
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    props[short_key] = val

            # Handle related foreign key fields
            if human.birth_place and human.birth_place.name:
                props["bp"] = human.birth_place.name

            if human.death_place and human.death_place.name:
                props["dp"] = human.death_place.name

            # Handle M2M attribute categories
            for category, short in sorted({
                                              "academic_degree": "ad",
                                              "award_received": "ar",
                                              "cause_of_death": "cod",
                                              "conflict": "c",
                                              "convicted_of": "co",
                                              "educated_at": "ed",
                                              "ethnic_group": "eg",
                                              "field_of_work": "fw",
                                              "gender": "g",
                                              "handedness": "h",
                                              "honorific_prefix": "hp",
                                              "manner_of_death": "md",
                                              "medical_condition": "mc",
                                              "member_of": "mo",
                                              "native_language": "nl",
                                              "occupation": "o",
                                              "political_ideology": "pi",
                                              "position_held": "ph",
                                              "religion_or_worldview": "wv",
                                              "social_classification": "sc",
                                          }.items()):
                values = human.attributes.filter(category=category).values_list("label", flat=True)
                values_list = list(values)
                if values_list:
                    props[short] = values_list

            return props

        features = []
        for human in tqdm(humans, desc="Exporting Notable Humans"):
            if not (human.birth_place and human.birth_place.latitude and human.birth_place.longitude):
                continue

            properties = abbreviate_properties(human)
            if not properties:
                continue

            # Then use it inside your loop
            feature = {
                "type": "Feature",
                "id": human.wikidata_id,
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        round(float(human.birth_place.longitude), 3),
                        round(float(human.birth_place.latitude), 3),
                    ],
                },
                "properties": properties,
            }

            features.append(feature)

        geojson = {"type": "FeatureCollection", "features": features}

        output_path = os.path.join(os.getcwd(), "notablehumans/data_collection/management/notable_humans.geojson")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=4, ensure_ascii=False)

        self.stdout.write(self.style.SUCCESS(f"Exported {len(features)} NotableHumans to notable_humans.geojson"))
