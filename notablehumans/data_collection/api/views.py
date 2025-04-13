import json
from pathlib import Path

from django.conf import settings
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet


class NotableHumansGeoJSONViewSet(ViewSet):
    permission_classes = [AllowAny]  # Make this public to your React frontend

    def list(self, request, *args, **kwargs):
        try:
            base_path = Path(settings.BASE_DIR)
            file_path = base_path / "notablehumans" / "data_collection" / "management" / "notable_humans.geojson"
            with open(file_path, encoding="utf-8") as f:
                geojson = json.load(f)
            return Response(geojson)
        except FileNotFoundError:
            return Response({"error": "GeoJSON file not found."}, status=404)
        except Exception as e:
            return Response({"error": "Failed to load GeoJSON", "details": str(e)}, status=500)
