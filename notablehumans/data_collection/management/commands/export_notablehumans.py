import json
import os

from django.core.management.base import BaseCommand

from ...models import NotableHuman


class Command(BaseCommand):
    help = "Export NotableHumans with valid birth locations to GeoJSON"

    def handle(self, *args, **kwargs):
        humans = NotableHuman.objects.filter(
            birth_year__isnull=False, birth_place__latitude__isnull=False, birth_place__longitude__isnull=False
        ).distinct()

        features = []
        for human in humans:
            if not (human.birth_place and human.birth_place.latitude and human.birth_place.longitude):
                continue

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
                "properties": {
                    "wikidata_id": human.wikidata_id,
                    "name": human.name,
                    "birth_year": int(human.birth_year) if human.birth_year is not None else None,
                    "birth_place_name": human.birth_place.name,
                    "death_year": int(human.death_year) if human.death_year is not None else None,
                    "death_place_name": human.death_place.name if human.death_place else None,
                    "article_created_date": human.article_created_date.isoformat()
                    if human.article_created_date
                    else None,
                    "article_length": int(human.article_length) if human.article_length is not None else None,
                    "article_recent_views": int(human.article_recent_views)
                    if human.article_recent_views is not None
                    else None,
                    "article_total_edits": int(human.article_total_edits)
                    if human.article_total_edits is not None
                    else None,
                    "article_recent_edits": int(human.article_recent_edits)
                    if human.article_recent_edits is not None
                    else None,
                },
            }
            features.append(feature)

        geojson = {"type": "FeatureCollection", "features": features}

        output_path = os.path.join(os.getcwd(), "notablehumans/data_collection/management/notable_humans.geojson")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=4, ensure_ascii=False)

        self.stdout.write(self.style.SUCCESS(f"Exported {len(features)} NotableHumans to notable_humans.geojson"))
