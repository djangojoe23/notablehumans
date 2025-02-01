import hashlib
import json
import time
from datetime import timedelta

import redis
import requests
from celery import group
from celery import shared_task
from celery.exceptions import Ignore
from celery.utils.log import get_task_logger
from django.db import transaction
from django.utils.timezone import now
from SPARQLWrapper import JSON
from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

from .models import Gender
from .models import NotableHuman
from .models import Place
from .tasks_helper import is_probably_human
from .tasks_helper import parse_coordinates
from .tasks_helper import parse_date

logger = get_task_logger(__name__)

REDIS_CLIENT = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
LOCK_EXPIRE_TIME = 30  # 1 hour expiration for locks
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
BATCH_SIZE = 50  # Number of titles per batch for SPARQL query

# Create a shared session object for reusing HTTP connections
session = requests.Session()


@shared_task(rate_limit="10/m")
def get_linked_titles_from_day(month, day):
    """
    Fetch the Wikipedia article content for a specific day of the year,
    extract links to other Wikipedia articles, and filter for human-related titles.
    """
    lock_key = f"wiki_task:{month}:{day}"

    # Acquire lock to ensure only one task per (month, day)
    if REDIS_CLIENT.set(lock_key, "locked", ex=LOCK_EXPIRE_TIME, nx=True):
        try:
            day_title = f"{month}_{day}"
            task_lock = f"lock-{day_title}"  # Unique identifier

            try:
                all_titles = []
                params = {
                    "action": "query",
                    "format": "json",
                    "prop": "links",
                    "titles": day_title,
                    "pllimit": "max",
                }
                continue_query = True
                plcontinue = None

                while continue_query:
                    if plcontinue:
                        params["plcontinue"] = plcontinue

                    response = session.get(WIKIPEDIA_API_URL, params=params)
                    response.raise_for_status()  # Raise an error for HTTP errors

                    data = response.json()
                    pages = data.get("query", {}).get("pages", {})

                    for page_id, page_info in pages.items():
                        if page_id != "-1":  # Page found
                            links = page_info.get("links", [])
                            # Filter for human-related titles
                            human_links = [link["title"] for link in links if is_probably_human(link["title"])]
                            all_titles.extend(human_links)

                    if "continue" in data and "plcontinue" in data["continue"]:
                        plcontinue = data["continue"]["plcontinue"]
                    else:
                        continue_query = False

                logger.info(
                    f"Extracted {len(all_titles)} potential human-related links for {day_title}.",
                )
                get_human_details_in_batches.apply_async(args=(all_titles,))

                return f"Processed {day_title}, found {len(all_titles)} titles."

            except requests.RequestException as e:
                logger.error(f"Error fetching content for {day_title}: {e}")
                return f"Error processing {day_title}: {e}"

        finally:
            REDIS_CLIENT.delete(lock_key)  # Release lock after task execution
    else:
        logger.info(f"Task already exists for {month} and {day}, skipping")
        raise Ignore  # Ignore duplicate tasks


@shared_task
def get_human_details_in_batches(titles):
    """
    Batch process titles and schedule SPARQL queries.
    """
    # Split titles into batches
    batches = [titles[i : i + BATCH_SIZE] for i in range(0, len(titles), BATCH_SIZE)]

    # Schedule tasks for each batch
    for batch in batches:
        # Generate a unique hash for this batch
        batch_hash = hashlib.sha256(json.dumps(batch, sort_keys=True).encode()).hexdigest()
        lock_key = f"batch_task:{batch_hash}:{int(time.time() // 60)}"

        # Try to set lock (if key exists, batch is already processing)
        if not REDIS_CLIENT.set(lock_key, "processing", ex=LOCK_EXPIRE_TIME, nx=True):
            logger.info(f"Batch already scheduled: skipping {batch_hash}")
            continue

        get_human_details.apply_async(args=(batch,))


def get_query_lock_key(query):
    """Generate a unique lock key for each SPARQL query."""
    return f"sparql_lock:{hashlib.sha256(query.encode()).hexdigest()}{int(time.time() // 60)}"


@shared_task(rate_limit="5/m")
def get_human_details(titles, max_retries=5, base_delay=2):
    """
    Query Wikidata SPARQL endpoint for human details for a batch of titles.
    """
    # Generate a unique hash for this batch
    batch_hash = hashlib.sha256(json.dumps(titles, sort_keys=True).encode()).hexdigest()
    lock_key = f"human_details_task:{batch_hash}{int(time.time() // 60)}"

    if not REDIS_CLIENT.set(lock_key, "processing", ex=LOCK_EXPIRE_TIME, nx=True):
        logger.info(f"Human details batch already processing: skipping {batch_hash}")
        return

    try:
        articles_str = " ".join(
            [f"<https://en.wikipedia.org/wiki/{title.replace(' ', '_')}>" for title in titles],
        )
        sparql_query = f"""
                SELECT ?item ?itemLabel ?article ?wikipediaUrl
                       (GROUP_CONCAT(DISTINCT STR(?dob); separator="|") AS ?dobValues)
                       (GROUP_CONCAT(DISTINCT STR(?dobStatement); separator="|") AS ?dobStatements)
                       (GROUP_CONCAT(DISTINCT STR(?dod); separator="|") AS ?dodValues)
                       (GROUP_CONCAT(DISTINCT STR(?dodStatement); separator="|") AS ?dodStatements)
                       ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
                       ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates
                       (GROUP_CONCAT(DISTINCT CONCAT(?genderID, "||", ?gender); SEPARATOR="@@") AS ?genderData)
                WHERE {{
                  VALUES ?article {{ {articles_str} }}

                  ?article schema:about ?item.

                  ?item rdfs:label ?itemLabel;
                        wdt:P31 wd:Q5.
                  FILTER(LANG(?itemLabel) = "en")
                  OPTIONAL {{
                    ?wikipediaUrl schema:about ?article .
                    FILTER(CONTAINS(STR(?wikipediaUrl), "en.wikipedia.org"))
                  }}
                  OPTIONAL {{ ?item wdt:P569 ?dob. }}         # Direct birth date
                  OPTIONAL {{ ?item p:P569/ps:P569 ?dobStatement. }}  # Birth date statement

                  OPTIONAL {{ ?item wdt:P570 ?dod. }}         # Direct death date
                  OPTIONAL {{ ?item p:P570 ?dodStatement. }}  # Death date statement
                  OPTIONAL {{
                    ?item wdt:P21 ?genderEntity.
                    ?genderEntity rdfs:label ?gender.
                    BIND(STRAFTER(STR(?genderEntity), "/entity/") AS ?genderID)
                    FILTER(LANG(?gender) = "en")
                  }}
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
                  }}
                }}
                GROUP BY ?item ?itemLabel ?article ?wikipediaUrl ?dobValues ?dobStatements ?dodValues ?dodStatements
                         ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
                         ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates
                """

        query_lock_key = get_query_lock_key(sparql_query)

        # Prevent duplicate queries from running simultaneously
        if not REDIS_CLIENT.set(query_lock_key, "locked", ex=60, nx=True):
            print(f"Query already in progress with : {titles}")
            return  # Skip duplicate request

        sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)

        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        sparql.addCustomHttpHeader("User-Agent", "NotableHumans/1.0 (jcmeyer23@gmail.com)")

        for attempt in range(max_retries):
            try:
                results = sparql.query().convert()
                humans_to_create_dict = {}
                humans_to_update_dict = {}
                genders_to_create = {}
                places_to_create = {}

                existing_humans = {h.wikidata_id: h for h in NotableHuman.objects.all()}
                existing_genders = {g.wikidata_id: g for g in Gender.objects.all()}
                existing_places = {p.wikidata_id: p for p in Place.objects.all()}

                recent_cutoff = now() - timedelta(minutes=1)  # Define a time window
                recently_updated_humans = NotableHuman.objects.filter(last_updated__gte=recent_cutoff)
                recently_updated_genders = Gender.objects.filter(last_updated__gte=recent_cutoff)
                recently_updated_places = Place.objects.filter(last_updated__gte=recent_cutoff)
                recent_human_ids = set(recently_updated_humans.values_list("wikidata_id", flat=True))
                recent_gender_ids = set(recently_updated_genders.values_list("wikidata_id", flat=True))
                recent_place_ids = set(recently_updated_places.values_list("wikidata_id", flat=True))

                for result in results["results"]["bindings"]:
                    wikidata_id = result["item"]["value"].split("/")[-1]
                    if wikidata_id and wikidata_id in recent_human_ids:
                        continue

                    # Parse main fields
                    name = result["itemLabel"]["value"]
                    wikipedia_url = result.get("article", {}).get("value")
                    birth_date, is_birth_bc = parse_date(
                        result.get("dobValues", {}).get("value"), result.get("dobStatements", {}).get("value")
                    )
                    death_date, is_death_bc = parse_date(
                        result.get("dodValues", {}).get("value"), result.get("dodStatements", {}).get("value")
                    )

                    # Parse and store genders
                    gender_ids = []
                    gender_data = result.get("genderData", {}).get("value")
                    if gender_data:
                        for gender_pair in gender_data.split("@@"):
                            gender_id, gender_label = gender_pair.split("||")
                            if gender_id:
                                gender_ids.append(gender_id)  # Collect the gender ID
                                if gender_id not in recent_gender_ids:
                                    if gender_id in existing_genders:
                                        # Update existing gender
                                        gender = existing_genders[gender_id]
                                        if gender.label != gender_label:
                                            gender.label = gender_label
                                            gender.last_updated = now()  # Mark as updated
                                    elif gender_id not in genders_to_create:
                                        # Prepare new gender for creation
                                        genders_to_create[gender_id] = Gender(
                                            wikidata_id=gender_id, label=gender_label
                                        )

                    # Parse and store places
                    for place_label_key, place_id_key, coord_key in [
                        ("birthPlaceLabel", "birthPlaceID", "birthPlaceCoordinates"),
                        ("deathPlaceLabel", "deathPlaceID", "deathPlaceCoordinates"),
                    ]:
                        place_id = result.get(place_id_key, {}).get("value")
                        if place_id and place_id not in recent_place_ids:
                            place_data = {
                                "name": result.get(place_label_key, {}).get("value"),
                                "latitude": None
                                if not coord_key
                                else parse_coordinates(result.get(coord_key, {}).get("value"))[0],
                                "longitude": None
                                if not coord_key
                                else parse_coordinates(result.get(coord_key, {}).get("value"))[1],
                            }
                            if place_id and place_id in existing_places:
                                # Update existing place
                                place = existing_places[place_id]
                                if (
                                    place.name != place_data["name"]
                                    or place.latitude != place_data["latitude"]
                                    or place.longitude != place_data["longitude"]
                                ):
                                    place.name = place_data["name"]
                                    place.latitude = place_data["latitude"]
                                    place.longitude = place_data["longitude"]
                                    place.last_updated = now()  # Mark as updated
                            elif place_id not in places_to_create:
                                places_to_create[place_id] = Place(wikidata_id=place_id, **place_data)

                    # Prepare human instance
                    human_data = {
                        "name": name,
                        "wikipedia_url": wikipedia_url,
                        "birth_date": birth_date,
                        "is_birth_bc": is_birth_bc,
                        "death_date": death_date,
                        "is_death_bc": is_death_bc,
                        "birth_place_id": result.get("birthPlaceID", {}).get("value"),
                        "death_place_id": result.get("deathPlaceID", {}).get("value"),
                        "genders": gender_ids,
                    }
                    if wikidata_id in existing_humans:
                        # Update existing human
                        human = existing_humans[wikidata_id]
                        humans_to_update_dict[human] = human_data
                    elif wikidata_id not in humans_to_create_dict:
                        # Create new human
                        humans_to_create_dict[wikidata_id] = human_data

                # Perform bulk operations inside a transaction
                try:
                    with transaction.atomic():
                        Gender.objects.bulk_create(genders_to_create.values(), ignore_conflicts=True)
                        existing_genders = {g.wikidata_id: g for g in Gender.objects.all()}

                        Place.objects.bulk_create(places_to_create.values(), ignore_conflicts=True)
                        existing_places = {p.wikidata_id: p for p in Place.objects.all()}

                        humans_to_create = []
                        for wikidata_id, human_data in humans_to_create_dict.items():
                            human = NotableHuman(
                                wikidata_id=wikidata_id,
                                name=human_data["name"],
                                wikipedia_url=human_data["wikipedia_url"],
                                birth_date=human_data["birth_date"],
                                is_birth_bc=human_data["is_birth_bc"],
                                death_date=human_data["death_date"],
                                is_death_bc=human_data["is_death_bc"],
                                birth_place=existing_places.get(human_data["birth_place_id"])
                                if human_data["birth_place_id"]
                                else None,
                                death_place=existing_places.get(human_data["death_place_id"])
                                if human_data["death_place_id"]
                                else None,
                            )
                            humans_to_create.append(human)
                        NotableHuman.objects.bulk_create(humans_to_create, ignore_conflicts=True)
                        if humans_to_create:
                            logger.info(f"Created {len(humans_to_create)} humans including {humans_to_create[0]}")

                        # Map wikidata_id to actual NotableHuman instances
                        humans_created_id_map = {
                            h.wikidata_id: h
                            for h in NotableHuman.objects.filter(wikidata_id__in=humans_to_create_dict.keys())
                        }

                        # Handle ManyToMany: Genders ↔ Humans
                        human_gender_relations = []
                        for wikidata_id, human_data in humans_to_create_dict.items():
                            human = humans_created_id_map.get(wikidata_id)
                            if human:
                                for gender_id in human_data["genders"]:
                                    gender = existing_genders.get(gender_id)
                                    if gender:
                                        human_gender_relations.append(
                                            human.gender.through(notablehuman=human, gender=gender)
                                        )

                        # Bulk create gender relationships
                        NotableHuman.gender.through.objects.bulk_create(human_gender_relations, ignore_conflicts=True)

                        humans_to_update = []
                        for human, human_data in humans_to_update_dict.items():
                            human.name = human_data["name"]
                            human.wikipedia_url = human_data["wikipedia_url"]
                            human.birth_date = human_data["birth_date"]
                            human.is_birth_bc = human_data["is_birth_bc"]
                            human.death_date = human_data["death_date"]
                            human.is_death_bc = human_data["is_death_bc"]
                            human.birth_place = (
                                existing_places.get(human_data["birth_place_id"])
                                if human_data["birth_place_id"]
                                else None
                            )
                            human.death_place = (
                                existing_places.get(human_data["death_place_id"])
                                if human_data["death_place_id"]
                                else None
                            )
                            human.last_updated = now()
                            humans_to_update.append(human)

                        # Perform bulk update
                        NotableHuman.objects.bulk_update(
                            humans_to_update,
                            fields=[
                                "name",
                                "wikipedia_url",
                                "birth_date",
                                "is_birth_bc",
                                "death_date",
                                "is_death_bc",
                                "birth_place",
                                "death_place",
                                "last_updated",
                            ],
                        )
                        if humans_to_update:
                            logger.info(f"Updated {len(humans_to_update)} humans including {humans_to_update[0]}")

                        # Handle ManyToMany: Genders ↔ Updated Humans
                        for human, human_data in humans_to_update_dict.items():
                            gender_objs = [
                                existing_genders[gender_id]
                                for gender_id in human_data["genders"]
                                if gender_id in existing_genders
                            ]
                            human.gender.set(gender_objs)  # This ensures the M2M field is updated

                except Exception as e:
                    logger.error(f"Transaction failed: {e}")
            except QueryBadFormed as e:
                logger.error(f"SPARQL query malformed {e}")
                break  # Don't retry if the query itself is incorrect
            except Exception as e:
                if "429" in str(e):
                    retry_after = base_delay * (2**attempt)  # Exponential backoff
                    logger.info(f"Rate limit hit. Retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                logger.error(f"SPARQL query failed: {e}")
                break
    finally:
        REDIS_CLIENT.delete(lock_key)  # Release lock after processing


@shared_task
def schedule_data_collection():
    """
    Schedule tasks to fetch Wikipedia article content for all days of the year
    using the Wikipedia API.
    """
    tasks = []
    for month in [
        "January",  # "February", "March", "April", "May", "June",
        # "July", "August", "September", "October", "November", "December"
    ]:
        for day in range(1, 2):
            lock_key = f"wiki_task:{month}:{day}"
            # Check if a task is already scheduled
            if REDIS_CLIENT.exists(lock_key):
                print(f"Skipping {month} {day}, already scheduled.")
                continue  # Skip duplicate tasks

            tasks.append(get_linked_titles_from_day.s(month, day))

    if tasks:
        # Distribute tasks as a group
        group(tasks).apply_async()

    return "Scheduled fetching tasks for all days of the year."
