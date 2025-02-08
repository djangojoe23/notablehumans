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

from .models import AttributeType
from .models import NotableHuman
from .models import NotableHumanAttribute
from .models import Place

logger = get_task_logger(__name__)


ATTRIBUTE_CHOICES = list(AttributeType.values)
REDIS_CLIENT = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
LOCK_EXPIRE_TIME = 30  # 30 second expiration for locks
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
BATCH_SIZE = 50  # Number of titles per batch for SPARQL query
OPTIONAL_FIELD_BATCH_SIZE = 5  # Chunk size of optional fields (attributes) per SPARQL query

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
                get_human_details_in_batches.apply_async(args=(month, day, all_titles))

                return f"Processed {day_title}, found {len(all_titles)} titles."

            except requests.RequestException as e:
                logger.error(f"Error fetching content for {day_title}: {e}")
                return f"Error processing {day_title}: {e}"

        finally:
            REDIS_CLIENT.delete(lock_key)  # Release lock after task execution
    else:
        logger.info(f"Task already exists for {month} and {day}, skipping")
        raise Ignore  # Ignore duplicate tasks


def is_probably_human(title):
    """
    Determines if a Wikipedia title likely refers to a human based on keywords
    and number patterns.
    """

    # Exclude titles with specific prefixes or generic terms
    non_human_keywords = [
        "Category:",
        "Template:",
        "Template talk:",
        "File:",
        "Talk:",
        "List of",
        "Portal:",
        "Wikipedia:",
    ]
    # Check if the title starts or ends with a number
    if title.lstrip().startswith(tuple("0123456789")) or title.rstrip().endswith(
        tuple("0123456789"),
    ):
        # Allow titles with parentheses for birth-death years
        if not title.endswith(")"):
            return False

    # Exclude titles containing specific keywords
    return all(keyword.lower() not in title.lower() for keyword in non_human_keywords)


@shared_task
def get_human_details_in_batches(month, day, titles):
    """
    Batch process titles and schedule SPARQL queries.
    """
    # Split titles into batches
    batches = [titles[i : i + BATCH_SIZE] for i in range(0, len(titles), BATCH_SIZE)]
    task_id = str(int(time.time()))  # Unique ID for this execution

    # Store the number of batches in Redis
    REDIS_CLIENT.set(f"wiki_batches:{task_id}", len(batches))

    # Schedule tasks for each batch
    for batch in batches:
        # Generate a unique hash for this batch
        batch_hash = hashlib.sha256(json.dumps(batch, sort_keys=True).encode()).hexdigest()
        lock_key = f"batch_task:{batch_hash}:{int(time.time() // 60)}"

        # Try to set lock (if key exists, batch is already processing)
        if not REDIS_CLIENT.set(lock_key, "processing", ex=LOCK_EXPIRE_TIME, nx=True):
            logger.info(f"Batch already scheduled: skipping {batch_hash}")
            continue

        get_human_details.apply_async(args=(month, day, batch, task_id))

    logger.info(f"Started processing {month} {day} {len(batches)} batches. Task ID: {task_id}")


def get_query_lock_key(query):
    """Generate a unique lock key for each SPARQL query."""
    return f"sparql_lock:{hashlib.sha256(query.encode()).hexdigest()}{int(time.time() // 60)}"


def chunk_optional_fields(n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(ATTRIBUTE_CHOICES), n):
        yield ATTRIBUTE_CHOICES[i : i + n]


@shared_task(rate_limit="5/m")
def get_human_details(month, day, titles, task_id):
    max_retries = 5
    base_delay = 2
    """
    Query Wikidata SPARQL endpoint for human details for a batch of titles.
    """
    # Generate a unique hash for this batch
    batch_hash = hashlib.sha256(json.dumps(titles, sort_keys=True).encode()).hexdigest()
    lock_key = f"human_details_task:{batch_hash}{int(time.time() // 60)}"

    if not REDIS_CLIENT.set(lock_key, "processing", ex=LOCK_EXPIRE_TIME, nx=True):
        logger.info(f"Human details batch already processing: skipping {batch_hash}")
        return

    # Anything updated within this time is "recent" and will not get updated
    two_minutes_ago = now() - timedelta(minutes=2)
    one_minute_ago = now() - timedelta(minutes=1)

    humans_to_create_dict = {}
    humans_to_update_dict = {}
    existing_humans = {h.wikidata_id: h for h in NotableHuman.objects.all()}
    recently_updated_humans = NotableHuman.objects.filter(
        last_updated__gte=two_minutes_ago, last_updated__lt=one_minute_ago
    )
    recent_human_ids = set(recently_updated_humans.values_list("wikidata_id", flat=True))

    places_to_create = {}
    existing_places = {p.wikidata_id: p for p in Place.objects.all()}
    recently_updated_places = Place.objects.filter(last_updated__gte=two_minutes_ago)
    recent_place_ids = set(recently_updated_places.values_list("wikidata_id", flat=True))

    attributes_to_create = {}
    existing_attributes = {a.wikidata_id: a for a in NotableHumanAttribute.objects.all()}
    recently_updated_attributes = NotableHumanAttribute.objects.filter(last_updated__gte=two_minutes_ago)
    recent_attribute_ids = set(recently_updated_attributes.values_list("wikidata_id", flat=True))
    try:
        is_first_batch = True
        for attribute_batch in chunk_optional_fields(OPTIONAL_FIELD_BATCH_SIZE):
            sparql_query = NotableHuman.get_sparql_query(titles, attribute_batch, is_first_batch)

            query_lock_key = get_query_lock_key(sparql_query)

            # Prevent duplicate queries from running simultaneously
            if not REDIS_CLIENT.set(query_lock_key, "locked", ex=60, nx=True):
                logger.info(f"Query already in progress with : {titles}")
                return

            sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)
            sparql.setReturnFormat(JSON)
            sparql.setQuery(sparql_query)
            sparql.method = "POST"
            sparql.addCustomHttpHeader("User-Agent", "NotableHumans/1.0 (mailto:jcmeyer23@gmail.com)")

            for attempt in range(max_retries):
                try:
                    results = sparql.query().convert()

                    for result in results["results"]["bindings"]:
                        wikidata_id = result["item"]["value"].split("/")[-1]
                        if wikidata_id and wikidata_id in recent_human_ids:
                            continue

                        # Parse main fields
                        name = result["itemLabel"]["value"]
                        human_data = {"name": name}
                        if is_first_batch:
                            wikipedia_url = result.get("article", {}).get("value")
                            birth_date, is_birth_bc = NotableHuman.parse_date(
                                result.get("dobValues", {}).get("value"), result.get("dobStatements", {}).get("value")
                            )
                            death_date, is_death_bc = NotableHuman.parse_date(
                                result.get("dodValues", {}).get("value"), result.get("dodStatements", {}).get("value")
                            )
                            human_data.update(
                                {
                                    "wikipedia_url": wikipedia_url,
                                    "birth_date": birth_date,
                                    "is_birth_bc": is_birth_bc,
                                    "death_date": death_date,
                                    "is_death_bc": is_death_bc,
                                    "birth_place_id": result.get("birthPlaceID", {}).get("value"),
                                    "death_place_id": result.get("deathPlaceID", {}).get("value"),
                                }
                            )
                            # Parse and store places
                            places_to_create.update(Place.parse_and_update(result, recent_place_ids, existing_places))

                        attribute_ids = []
                        for attribute_choice in ATTRIBUTE_CHOICES:
                            # Needs to ensure that the attribute_choice matches the field in the SPARQL query
                            attr_data = result.get(attribute_choice, {}).get("value")
                            if attr_data:
                                for attr_pair in attr_data.split("@@"):
                                    attr_id, attr_label = attr_pair.split("||")
                                    if attr_id:
                                        attribute_ids.append(attr_id)
                                        if attr_id not in recent_attribute_ids:
                                            category_value = AttributeType(attribute_choice).value
                                            if attr_id in existing_attributes:
                                                # Update existing attribute
                                                attribute = existing_attributes[attr_id]
                                                if (
                                                    attribute.label != attr_label
                                                    or attribute.category != category_value
                                                ):
                                                    attribute.label = attr_label
                                                    attribute.category = category_value
                                                    attribute.last_updated = now()  # Mark as updated
                                                    attribute.save()
                                            elif attr_id not in attributes_to_create:
                                                # Prepare new gender for creation
                                                attributes_to_create[attr_id] = NotableHumanAttribute(
                                                    wikidata_id=attr_id, label=attr_label, category=category_value
                                                )

                        if wikidata_id in existing_humans:
                            # Update existing human
                            human = existing_humans[wikidata_id]
                            if human not in humans_to_update_dict:
                                humans_to_update_dict[human] = human_data
                                humans_to_update_dict[human]["attributes"] = attribute_ids
                            else:
                                humans_to_update_dict[human].update(human_data)
                                humans_to_update_dict[human]["attributes"] += attribute_ids
                        elif wikidata_id not in humans_to_create_dict:
                            # Create new human
                            if wikidata_id not in humans_to_create_dict:
                                humans_to_create_dict[wikidata_id] = human_data
                                humans_to_create_dict[wikidata_id]["attributes"] = attribute_ids
                            else:
                                humans_to_create_dict[wikidata_id].update(human_data)
                                humans_to_create_dict[wikidata_id]["attributes"] += attribute_ids

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
            is_first_batch = False
        # Perform bulk operations inside a transaction
        try:
            with transaction.atomic():
                Place.objects.bulk_create(places_to_create.values(), ignore_conflicts=True)
                existing_places = {p.wikidata_id: p for p in Place.objects.all()}

                NotableHumanAttribute.objects.bulk_create(attributes_to_create.values(), ignore_conflicts=True)
                existing_attributes = {a.wikidata_id: a for a in NotableHumanAttribute.objects.all()}

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
                    h.wikidata_id: h for h in NotableHuman.objects.filter(wikidata_id__in=humans_to_create_dict.keys())
                }

                # Handle ManyToMany: Attributes ↔ Humans
                human_attribute_relations = []
                for wikidata_id, human_data in humans_to_create_dict.items():
                    human = humans_created_id_map.get(wikidata_id)
                    if human:
                        for attr_id in human_data["attributes"]:
                            attribute = existing_attributes.get(attr_id)
                            if attribute:
                                human_attribute_relations.append(
                                    human.attributes.through(notablehuman=human, notablehumanattribute=attribute)
                                )

                # Bulk create attribute relationships
                NotableHuman.attributes.through.objects.bulk_create(human_attribute_relations, ignore_conflicts=True)

                humans_to_update = []
                for human, human_data in humans_to_update_dict.items():
                    human.name = human_data["name"]
                    human.wikipedia_url = human_data["wikipedia_url"]
                    human.birth_date = human_data["birth_date"]
                    human.is_birth_bc = human_data["is_birth_bc"]
                    human.death_date = human_data["death_date"]
                    human.is_death_bc = human_data["is_death_bc"]
                    human.birth_place = (
                        existing_places.get(human_data["birth_place_id"]) if human_data["birth_place_id"] else None
                    )
                    human.death_place = (
                        existing_places.get(human_data["death_place_id"]) if human_data["death_place_id"] else None
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

                # Handle ManyToMany: Attributes ↔ Updated Humans
                for human, human_data in humans_to_update_dict.items():
                    attribute_objs = [
                        existing_attributes[attr_id]
                        for attr_id in human_data["attributes"]
                        if attr_id in existing_attributes
                    ]
                    human.attributes.add(*attribute_objs)  # This ensures the M2M field is updated

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
    except Exception as e:
        logger.error(f"Error in get_human_details: {e}")
    finally:
        REDIS_CLIENT.delete(lock_key)  # Release lock after processing

        # Decrement the batch counter
        remaining = REDIS_CLIENT.decr(f"wiki_batches:{task_id}")

        # Log when all batches are done
        if remaining > 0:
            logger.info(f"{remaining} batches left for {month} {day} to process for task {task_id}.")
        else:
            logger.info(f"All Wikipedia batches have been queried for {month} {day} task {task_id}!")


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
        for day in range(20, 21):
            lock_key = f"wiki_task:{month}:{day}"
            # Check if a task is already scheduled
            if REDIS_CLIENT.exists(lock_key):
                logger.info(f"Skipping {month} {day}, already scheduled.")
                continue  # Skip duplicate tasks

            tasks.append(get_linked_titles_from_day.s(month, day))

    if tasks:
        # Distribute tasks as a group
        group(tasks).apply_async()

    return "Scheduled fetching tasks for all days of the year."
