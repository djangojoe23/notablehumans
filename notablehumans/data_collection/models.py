from datetime import datetime
from urllib.parse import quote

from django.db import models
from django.utils.timezone import get_current_timezone
from django.utils.timezone import now


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
    last_updated = models.DateTimeField(default=now)

    def __str__(self):
        return self.name

    @staticmethod
    def parse_coordinates(coord_string):
        """
        Parse coordinate string into latitude and longitude.
        """
        if coord_string and not coord_string.startswith("http"):
            coord_parts = coord_string.replace("Point(", "").replace(")", "").split()
            return float(coord_parts[1]), float(coord_parts[0])
        return None, None

    @staticmethod
    def parse_and_update(result, recent_updates, existing_records):
        to_create = {}
        for place_label_key, place_id_key, coord_key in [
            ("birthPlaceLabel", "birthPlaceID", "birthPlaceCoordinates"),
            ("deathPlaceLabel", "deathPlaceID", "deathPlaceCoordinates"),
        ]:
            place_id = result.get(place_id_key, {}).get("value")
            if place_id and place_id not in recent_updates:
                place_data = {
                    "name": result.get(place_label_key, {}).get("value"),
                    "latitude": None
                    if not coord_key
                    else Place.parse_coordinates(result.get(coord_key, {}).get("value"))[0],
                    "longitude": None
                    if not coord_key
                    else Place.parse_coordinates(result.get(coord_key, {}).get("value"))[1],
                }
                if place_id and place_id in existing_records:
                    # Update existing place
                    place = existing_records[place_id]
                    if (
                        place.name != place_data["name"]
                        or place.latitude != place_data["latitude"]
                        or place.longitude != place_data["longitude"]
                    ):
                        place.name = place_data["name"]
                        place.latitude = place_data["latitude"]
                        place.longitude = place_data["longitude"]
                        place.last_updated = now()  # Mark as updated
                        place.save()
                elif place_id not in to_create:
                    to_create[place_id] = Place(wikidata_id=place_id, **place_data)

        return to_create


class AttributeType(models.TextChoices):
    GENDER = "gender", "Gender"
    OCCUPATION = "occupation", "Occupation"
    ETHNIC_GROUP = "ethnic_group", "Ethnic Group"
    FIELD_OF_WORK = "field_of_work", "Field of Work"
    MEMBER_OF = "member_of", "Member Of"
    MANNER_OF_DEATH = "manner_of_death", "Manner of Death"
    CAUSE_OF_DEATH = "cause_of_death", "Cause of Death"
    HANDEDNESS = "handedness", "Handedness"
    CONVICTED_OF = "convicted_of", "Convicted Of"
    AWARD_RECEIVED = "award_received", "Award Received"
    NATIVE_LANGUAGE = "native_language", "Native Language"
    POLITICAL_IDEOLOGY = "political_ideology", "Political Ideology"
    HONORIFIC_PREFIX = "honorific_prefix", "Honorific Prefix"
    RELIGION_OR_WORLDVIEW = "religion_or_worldview", "Religion or Worldview"
    MEDICAL_CONDITION = "medical_condition", "Medical Condition"
    CONFLICT = "conflict", "Conflict"
    EDUCATED_AT = "educated_at", "Educated At"
    ACADEMIC_DEGREE = "academic_degree", "Academic Degree"
    SOCIAL_CLASSIFICATION = "social_classification", "Social Classification"
    POSITION_HELD = "position_held", "Position Held"


class NotableHumanAttribute(models.Model):
    wikidata_id = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255)
    category = models.CharField(max_length=50, choices=AttributeType.choices)
    last_updated = models.DateTimeField(default=now)

    def __str__(self):
        return f"{self.wikidata_id}: {self.label} ({self.get_category_display()})"


class NotableHuman(models.Model):
    UNRATED_ARTICLE = "unrated"
    GOOD_ARTICLE = "good"
    FEATURED_ARTICLE = "featured"

    ARTICLE_QUALITY_CHOICES = [
        (UNRATED_ARTICLE, "Unrated Article"),
        (GOOD_ARTICLE, "Good Article"),
        (FEATURED_ARTICLE, "Featured Article"),
    ]

    wikidata_id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    description = models.CharField(max_length=255, blank=True)
    wikipedia_url = models.URLField(max_length=500, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    is_birth_bc = models.BooleanField(default=False)
    death_date = models.DateField(null=True, blank=True)
    is_death_bc = models.BooleanField(default=False)  # Flag to mark BC dates
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
    attributes = models.ManyToManyField(NotableHumanAttribute, related_name="notable_humans")
    created_at = models.DateTimeField(
        auto_now_add=True,
    )  # Timestamp for record creation
    last_wikidata_update = models.DateTimeField()
    article_length = models.IntegerField(null=True, blank=True)
    article_recent_views = models.IntegerField(null=True, blank=True)
    article_quality = models.CharField(max_length=10, choices=ARTICLE_QUALITY_CHOICES, default=UNRATED_ARTICLE)
    article_created_date = models.DateTimeField(null=True, blank=True)
    article_total_edits = models.IntegerField(null=True, blank=True)
    article_recent_edits = models.IntegerField(null=True, blank=True)
    last_wikipedia_update = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.name or 'Unknown'} ({self.wikidata_id})"

    @staticmethod
    def get_sparql_query(titles, attribute_batch, is_first_batch):
        if is_first_batch:
            query_select = """SELECT ?item ?itemLabel ?article ?wikipediaUrl
                          (GROUP_CONCAT(DISTINCT STR(?dob); separator="|") AS ?dobValues)
                          (GROUP_CONCAT(DISTINCT STR(?dobStatement); separator="|") AS ?dobStatements)
                          (GROUP_CONCAT(DISTINCT STR(?dod); separator="|") AS ?dodValues)
                          (GROUP_CONCAT(DISTINCT STR(?dodStatement); separator="|") AS ?dodStatements)
                          ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
                          ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates"""
            query_body = """OPTIONAL {{
                       ?wikipediaUrl schema:about ?article .
                       FILTER(CONTAINS(STR(?wikipediaUrl), "en.wikipedia.org"))
                     }}
                     OPTIONAL {{ ?item wdt:P569 ?dob. }}         # Direct birth date
                     OPTIONAL {{ ?item p:P569/ps:P569 ?dobStatement. }}  # Birth date statement

                     OPTIONAL {{ ?item wdt:P570 ?dod. }}         # Direct death date
                     OPTIONAL {{ ?item p:P570 ?dodStatement. }}  # Death date statement
                     OPTIONAL {{
                       ?item wdt:P19 ?birthPlace.
                       ?birthPlace rdfs:label ?birthPlaceLabel;
                         wdt:P625 ?birthPlaceCoordinates.
                       BIND(STRAFTER(STR(?birthPlace), "/entity/") AS ?birthPlaceID)
                       FILTER(LANG(?birthPlaceLabel) = "en")
                     }}
                     OPTIONAL {{
                       ?item wdt:P20 ?deathPlace.
                       ?deathPlace rdfs:label ?deathPlaceLabel;
                         wdt:P625 ?deathPlaceCoordinates.
                       BIND(STRAFTER(STR(?deathPlace), "/entity/") AS ?deathPlaceID)
                       FILTER(LANG(?deathPlaceLabel) = "en")
                     }}"""
            query_groupbys = """?article ?wikipediaUrl ?dobValues ?dobStatements ?dodValues ?dodStatements
                            ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
                            ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates"""
        else:
            query_select = """SELECT ?item ?itemLabel"""
            query_body = ""
            query_groupbys = ""

        optional_fields = {
            "gender": "P21",
            "occupation": "P106",
            "ethnic_group": "P172",
            "field_of_work": "P101",
            "member_of": "P463",
            "manner_of_death": "P1196",
            "cause_of_death": "P509",
            "handedness": "P552",
            "convicted_of": "P1399",
            "award_received": "P166",
            "native_language": "P103",
            "political_ideology": "P102",
            "honorific_prefix": "P511",
            "religion_or_worldview": "P140",
            "medical_condition": "P1050",
            "conflict": "P607",
            "educated_at": "P69",
            "academic_degree": "P512",
            "social_classification": "P3716",
            "position_held": "P39",
        }

        optional_group_concats = []
        optional_statements = []
        for field, p_value in optional_fields.items():
            if field not in attribute_batch:
                continue

            group_concat = (
                f"""(GROUP_CONCAT(DISTINCT CONCAT(?{field}ID, "||", ?{field}); SEPARATOR="@@") AS ?{field})"""
            )
            optional_group_concats.append(group_concat)
            statement = f"""
                OPTIONAL {{
                    ?item p:{p_value} ?{field}Statement.
                    ?{field}Statement ps:{p_value} ?{field}Entity.
                    ?{field}Entity rdfs:label ?{field}.
                    BIND(STRAFTER(STR(?{field}Entity), "/entity/") AS ?{field}ID)
                    FILTER(LANG(?{field}) = "en")
                }}
                """
            optional_statements.append(statement)

        optional_group_concats = "\n".join(optional_group_concats)
        optional_statements = "\n".join(optional_statements)

        """
        Generates the SPARQL query for a list of titles.
        """
        # Build the query here, assuming titles is a list
        articles_str = " ".join(
            [f"<https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'), safe=':/')}>" for title in titles]
        )
        return f""" {query_select}
                          {optional_group_concats}
                   WHERE {{
                     VALUES ?article {{ {articles_str} }}

                     ?article schema:about ?item.

                     ?item rdfs:label ?itemLabel;
                           wdt:P31 wd:Q5.
                     FILTER(LANG(?itemLabel) = "en")
                     {query_body}
                     {optional_statements}
                   }}
                   GROUP BY ?item ?itemLabel {query_groupbys}
                   """

    @staticmethod
    def parse_date(date_values, date_statements):
        # Initialize a set to hold valid date candidates
        date_candidates = set()

        # Add dobValues to candidates if it is not a URL and is valid date
        if date_values:
            for value in date_values.split("|"):
                if not value.startswith("http"):  # Skip URLs
                    date_candidates.add(value.strip())  # Add cleaned value to set

        # Add dobStatements to candidates
        if date_statements:
            for statement in date_statements.split("|"):
                if not statement.startswith("http"):  # Skip URLs
                    date_candidates.add(statement.strip())  # Add cleaned statement to set

        # If no valid date candidates found, return None
        if not date_candidates:
            return None, False

        # Try to parse each candidate date string
        for date_str in date_candidates:
            try:
                # Extract YYYY-MM-DD part (ignore the time portion)
                date_str = date_str.split("T")[0]
                if date_str[0] == "-":
                    date_str = date_str[1:]
                    is_bc = True
                else:
                    is_bc = False

                year, month, day = map(int, date_str.split("-"))

                return datetime(year, month, day, tzinfo=get_current_timezone()).date(), is_bc

            except ValueError:
                continue  # Skip invalid date strings

        return None, False  # No valid date found

    def get_genders(self):
        return self.attributes.filter(category=AttributeType.GENDER)

    def get_occupations(self):
        return self.attributes.filter(category=AttributeType.OCCUPATION)
