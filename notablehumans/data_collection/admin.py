# Register your models here.
from django.contrib import admin

from .models import NotableHuman  # Import your models
from .models import NotableHumanAttribute  # Import your models
from .models import Place  # Import your models


@admin.register(NotableHuman)
class NotableHumanAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "wikidata_id",
        "description",
        "birth_date",
        "is_birth_bc",
        "death_date",
        "is_death_bc",
        "birth_place",
        "death_place",
        "article_length",
        "article_recent_views",
        "article_quality",
        "article_total_edits",
        "article_recent_edits",
    )
    search_fields = ("name", "wikidata_id")
    filter_horizontal = ("attributes",)  # Enables a horizontal filter UI


@admin.register(Place)
class PlaceAdmin(admin.ModelAdmin):
    list_display = ("name", "wikidata_id", "latitude", "longitude")
    search_fields = ("name", "wikidata_id")


@admin.register(NotableHumanAttribute)
class NotableHumanAttributeAdmin(admin.ModelAdmin):
    list_display = ("label", "wikidata_id", "category")
    search_fields = ("label", "wikidata_id", "category")
