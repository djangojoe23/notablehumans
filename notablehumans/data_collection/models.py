from django.db import models


class Gender(models.Model):
    wikidata_id = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label


class Place(models.Model):
    wikidata_id = models.CharField(max_length=255, primary_key=True)
    name = models.CharField(max_length=255, unique=True)
    latitude = models.DecimalField(
        max_digits=9,
        decimal_places=6,
        null=True,
        blank=True,
    )
    longitude = models.DecimalField(
        max_digits=9,
        decimal_places=6,
        null=True,
        blank=True,
    )

    def __str__(self):
        return self.name


class NotableHuman(models.Model):
    wikidata_id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    wikipedia_url = models.URLField(max_length=500, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    death_date = models.DateField(null=True, blank=True)
    gender = models.ManyToManyField(Gender, related_name="genders", blank=True)
    birth_place = models.ForeignKey(
        Place,
        related_name="births",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    death_place = models.ForeignKey(
        Place,
        related_name="deaths",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )

    """
    part_of
    country_of_citizenship
    manner_of_death
    cause_of_death
    place_of_burial
    educated_at
    ethnic_group
    handedness
    convicted_of
    residence
    work_period_start
    work_period_end
    eye_color
    hair_color
    political_ideology
    employer
    academic_degree
    social_media_followers
    field_of_work
    native_language
    occupation
    position_held
    honorific_prefix
    social_classification
    religion_or_worldview
    medical_condition
    military_or_police_rank
    conflict
    member_of
    award_received
    height
    """
    created_at = models.DateTimeField(
        auto_now_add=True,
    )  # Timestamp for record creation
    updated_at = models.DateTimeField(auto_now=True)  # Timestamp for record updates

    def __str__(self):
        return f"{self.name or 'Unknown'} ({self.wikidata_id})"
