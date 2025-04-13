from django.conf import settings
from rest_framework.routers import DefaultRouter
from rest_framework.routers import SimpleRouter

from notablehumans.data_collection.api.views import NotableHumansGeoJSONViewSet
from notablehumans.users.api.views import UserViewSet

router = DefaultRouter() if settings.DEBUG else SimpleRouter()

router.register("users", UserViewSet)
router.register("notable-humans-geojson", NotableHumansGeoJSONViewSet, basename="notable-humans-geojson")

app_name = "api"
urlpatterns = router.urls
