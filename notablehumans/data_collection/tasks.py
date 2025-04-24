import hashlib
import json
import time
from datetime import timedelta
from http import HTTPStatus
from dateutil import parser as dateparser
from dateutil.parser import ParserError
import re

import redis
import requests
import random
from bs4 import BeautifulSoup
from celery import group
from celery import shared_task
from celery.exceptions import Ignore
from celery.utils.log import get_task_logger
from django.db import transaction
from django.db.models import Q
from django.utils.timezone import get_default_timezone
from django.utils.timezone import make_aware
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
RATE_LIMIT = "20/m"
OPTIONAL_FIELD_BATCH_SIZE = 5  # Chunk size of optional fields (attributes) per SPARQL query

# Create a shared session object for reusing HTTP connections
session = requests.Session()


@shared_task(rate_limit=RATE_LIMIT)
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


@shared_task(rate_limit=RATE_LIMIT)
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
    batch_count = 1
    for batch in batches:
        # Generate a unique hash for this batch
        batch_hash = hashlib.sha256(json.dumps(batch, sort_keys=True).encode()).hexdigest()
        lock_key = f"batch_task:{batch_hash}"  # :{int(time.time() // 60)}

        # Try to set lock (if key exists, batch is already processing)
        if not REDIS_CLIENT.set(lock_key, "processing", ex=LOCK_EXPIRE_TIME, nx=True):
            logger.info(f"Batch already scheduled: skipping {batch_hash}")
            continue

        get_human_details.apply_async(args=(month, day, batch, task_id))
        batch_count += 1

    logger.info(f"Started processing {len(batches)} batches for {month} {day}. Task ID: {task_id}")


def get_query_lock_key(query):
    """Generate a unique lock key for each SPARQL query."""
    return f"sparql_lock:{hashlib.sha256(query.encode()).hexdigest()}"  # {int(time.time() // 60)}


def chunk_optional_fields(n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(ATTRIBUTE_CHOICES), n):
        yield ATTRIBUTE_CHOICES[i : i + n]


@shared_task(rate_limit=RATE_LIMIT, time_limit=120, soft_time_limit=100)
def get_human_details(month, day, titles, task_id):
    max_retries = 5
    base_delay = 2
    """
    Query Wikidata SPARQL endpoint for human details for a batch of titles.
    """
    # Generate a unique hash for this batch
    batch_hash = hashlib.sha256(json.dumps(titles, sort_keys=True).encode()).hexdigest()
    lock_key = f"human_details_task:{batch_hash}"  # {int(time.time() // 60)}

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
        last_wikidata_update__gte=two_minutes_ago, last_wikidata_update__lt=one_minute_ago
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
                    time.sleep(random.uniform(0.8, 1.4))
                    for result in results["results"]["bindings"]:
                        wikidata_id = result["item"]["value"].split("/")[-1]
                        if wikidata_id and wikidata_id in recent_human_ids:
                            continue

                        # Parse main fields
                        name = result["itemLabel"]["value"]
                        human_data = {"name": name}
                        if is_first_batch:
                            raw_url = result.get("article", {}).get("value")
                            wikipedia_url = raw_url.rsplit("/", 1)[-1] if raw_url else ""
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
            time.sleep(1)  # To avoid hammering endpoint with batches back to back

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
                        last_wikidata_update=now(),
                    )
                    humans_to_create.append(human)
                NotableHuman.objects.bulk_create(humans_to_create, ignore_conflicts=True)
                # if humans_to_create:
                #     logger.info(f"
                #     {month} {day}: Created {len(humans_to_create)} humans including {humans_to_create[0]}")

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
                    human.last_wikidata_update = now()
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
                        "last_wikidata_update",
                    ],
                )
                # if humans_to_update:
                #     logger.info(f"
                #     {month} {day}: Updated {len(humans_to_update)} humans including {humans_to_update[0]}")

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
            logger.info(f"### ALL BATCHES COMPLETE for {month} {day} task {task_id}! ###")
            # logger.info(f"Batch {batch_count} out of {num_batches} complete for {month} {day} task {task_id}!")


@shared_task
def schedule_wikidata_data_collection():
    """
    Schedule tasks to fetch Wikipedia article content for all days of the year
    using the Wikipedia API.
    """
    tasks = []
    for month in [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July", "August", "September", "October", "November", "December"
    ]:
        daily_tasks = []
        for day in range(1, 32):
            lock_key = f"wiki_task:{month}:{day}"
            # Check if a task is already scheduled
            if REDIS_CLIENT.exists(lock_key):
                logger.info(f"Skipping {month} {day}, already scheduled.")
                continue  # Skip duplicate tasks

            delay_seconds = random.randint(0, 600)  # delay up to 10 minutes
            daily_tasks.append(get_linked_titles_from_day.s(month, day))

        if daily_tasks:
            # Add the month's tasks as a group of tasks to month_tasks
            tasks.append(group(daily_tasks))  # Group the tasks for this month

    if tasks:
        # Distribute tasks as a group
        group(*tasks).apply_async()  # Apply all month groups simultaneously

    return "Scheduled fetching tasks for all days of the year by month."


@shared_task
def schedule_wikipedia_data_collection():
    # Get all NotableHuman instances with Wikipedia URLs
    cutoff = now() - timedelta(weeks=4)

    print(NotableHuman.objects.filter(description__exact="").count())
    print("That was the number of humans with no description!\n\n")
    # Log how many humans need a Wikipedia update
    humans_to_update = NotableHuman.objects.filter(wikipedia_url__isnull=False, wikipedia_url__gt="").filter(
        Q(last_wikipedia_update__lt=cutoff) | Q(last_wikipedia_update__isnull=True)
    )

    logger.info(f"Total NotableHumans needing Wikipedia update: {humans_to_update.count()}")

    all_ids = list(humans_to_update.values_list("wikidata_id", flat=True))
    batch_size = 50
    human_ids_batches = [
        all_ids[i: i + batch_size] for i in range(0, len(all_ids), batch_size)
    ]

    # check for any missing
    scheduled_ids = {wid for batch in human_ids_batches for wid in batch}
    missing = set(all_ids) - scheduled_ids
    if missing:
        logger.error(f"‼️  Missing from scheduling: {missing}")

    logger.info(
        f"Split into {len(human_ids_batches)} batches, "
        f"{sum(len(b) for b in human_ids_batches)} scheduled IDs"
    )

    tasks = [process_wikipedia_batch.s(batch) for batch in human_ids_batches]
    group(*tasks).apply_async()


@shared_task(time_limit=150, soft_time_limit=120)
def process_wikipedia_batch(human_ids):
    humans = NotableHuman.objects.filter(wikidata_id__in=human_ids)

    for human in humans:
        try:
            page_title = human.wikipedia_url.split("/")[-1]
            scraped_page_title, metadata = fetch_wikipedia_metadata(page_title)

            if metadata:
                human.wikipedia_url = scraped_page_title
                human.description = metadata.get("description")
                human.article_length = metadata.get("page_length")
                human.article_recent_views = metadata.get("page_views_30_days")
                human.article_total_edits = metadata.get("edit_count")
                human.article_recent_edits = metadata.get("recent_edits_30_days")
                if metadata.get("good_article"):
                    human.article_quality = NotableHuman.GOOD_ARTICLE
                elif metadata.get("featured_article"):
                    human.article_quality = NotableHuman.FEATURED_ARTICLE
                human.article_created_date = metadata.get("created_date")
                human.last_wikipedia_update = now()
                human.save()
                logger.info(f"Saved Wikipedia metadata for {human.wikipedia_url}")
        except Exception as e:
            logger.error(f"Error updating human {human.wikidata_id}: {e}")
    time.sleep(2)


def fetch_wikipedia_metadata(page_title):
    """
    Fetches metadata from the Wikipedia Info page for a given page title.
    """
    page_title = page_title.split("#", 1)[0]
    url = f"https://en.wikipedia.org/w/index.php?title={page_title.replace(' ', '_')}&action=info"

    # Send a GET request to the Wikipedia Info page
    response = requests.get(url, timeout=10)

    if response.status_code == HTTPStatus.OK:
        soup = BeautifulSoup(response.text, "html.parser")

        metadata = {}

        redirect_row = soup.find("tr", {"id": "mw-pageinfo-redirectsto"})
        if redirect_row:
            # If a redirect exists, extract the redirected article's title
            redirect_link = redirect_row.find_all("td")[1].find("a")["href"]
            redirected_page_title = redirect_link.split("/")[-1]

            # Fetch metadata from the redirected page
            return fetch_wikipedia_metadata(redirected_page_title)

        description = ""
        for row in soup.find_all("tr"):
            first_td = row.find("td")  # Get the first <td>
            if first_td and "Central description" in first_td.text:
                description_td = row.find_all("td")[1]  # Second <td> contains the description
                description = description_td.text.strip()
                break  # Stop after finding the first match
        else:
            for row in soup.find_all("tr"):
                first_td = row.find("td")  # Get the first <td>
                if first_td and "Local description" in first_td.text:
                    description_td = row.find_all("td")[1]  # Second <td> contains the description
                    description = description_td.text.strip()
                    break  # Stop after finding the first match

        metadata["description"] = description

        # Check for the presence of the "Category:Good articles" link
        good_articles_link = soup.find("a", {"title": "Category:Good articles"})
        metadata["good_article"] = bool(good_articles_link)

        # Check for the presence of the "Category:Featured articles" link
        featured_articles_link = soup.find("a", {"title": "Category:Featured articles"})
        metadata["featured_article"] = bool(featured_articles_link)

        # Mapping of the IDs to metadata labels
        metadata_ids = {
            "mw-pageinfo-length": "page_length",
            "mw-pvi-month-count": "page_views_30_days",
            "mw-pageinfo-firsttime": "created_date",
            "mw-pageinfo-edits": "edit_count",
            "mw-pageinfo-recent-edits": "recent_edits_30_days",
        }

        # Loop through each id in the mapping and extract the data
        for row_id, label in metadata_ids.items():
            row = soup.find("tr", {"id": row_id})
            if not row:
                logger.warning(f"No row for {row_id}")
                continue

            cells = row.find_all("td")
            if len(cells) < 2:
                logger.warning(f"Expected 2 <td> in {row_id}, found {len(cells)}")
                continue

            text = cells[1].get_text(strip=True)  # <<–– now the *value* cell

            if label == "created_date":
                try:
                    dt = dateparser.parse(text)
                    metadata[label] = make_aware(dt, get_default_timezone())
                except (ParserError, ValueError, TypeError):
                    logger.warning(f"Failed to parse date '{text}' in {row_id}")
            else:
                digits = re.sub(r"\D", "", text)
                try:
                    metadata[label] = int(digits)
                except ValueError:
                    logger.warning(f"No digits found in '{text}' for {row_id}")

        return page_title, metadata
    return page_title, None  # Return None if the page request fails


@shared_task
def reprocess_missing_created_dates():
    # 1) Get all Wikidata IDs still lacking a created date
    qids = list(
        NotableHuman.objects
        .filter(article_created_date__isnull=True, wikipedia_url__isnull=False)
        .values_list("wikidata_id", flat=True)
    )

    # 2) Split into batches
    batches = [qids[i : i + BATCH_SIZE] for i in range(0, len(qids), BATCH_SIZE)]

    # 3) Fire off one process_wikipedia_batch job per batch
    job = group(process_wikipedia_batch.s(batch) for batch in batches)
    job.apply_async()
