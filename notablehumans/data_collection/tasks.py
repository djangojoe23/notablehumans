import requests
from celery import group
from celery import shared_task
from SPARQLWrapper import JSON
from SPARQLWrapper import SPARQLWrapper

from .models import NotableHuman
from .tasks_helper import is_probably_human

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
    day_title = f"{month}_{day}"
    params = {
        "action": "query",
        "format": "json",
        "prop": "links",
        "titles": day_title,
        "pllimit": "max",
    }

    try:
        all_titles = []
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

        print(
            f"Extracted {len(all_titles)} potential human-related links for {day_title}.",
        )

        get_human_details_in_batches.apply_async(args=(all_titles,))

        return f"Processed {day_title}, found {len(all_titles)} titles."

    except requests.RequestException as e:
        print(f"Error fetching content for {day_title}: {e}")
        return f"Error processing {day_title}: {e}"


@shared_task
def get_human_details_in_batches(titles):
    """
    Batch process titles and schedule SPARQL queries.
    """
    # Split titles into batches
    batches = [titles[i : i + BATCH_SIZE] for i in range(0, len(titles), BATCH_SIZE)]

    # Schedule tasks for each batch
    for batch in batches:
        get_human_details.apply_async(args=(batch,))


@shared_task(rate_limit="10/m")
def get_human_details(titles):
    """
    Query Wikidata SPARQL endpoint for human details for a batch of titles.
    """
    sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)

    articles_str = " ".join(
        [f"<https://en.wikipedia.org/wiki/{title.replace(' ', '_')}>" for title in titles],
    )
    sparql_query = f"""
    SELECT ?item ?itemLabel ?article ?wikipediaUrl ?dob ?dod
           ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
           ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates
           (GROUP_CONCAT(DISTINCT CONCAT(?genderID, "||", ?gender); SEPARATOR="|") AS ?genderData)
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
      OPTIONAL {{ ?item wdt:P569 ?dob. }}
      OPTIONAL {{ ?item wdt:P570 ?dod. }}
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
    GROUP BY ?item ?itemLabel ?article ?wikipediaUrl ?dob ?dod
             ?birthPlace ?birthPlaceLabel ?birthPlaceID ?birthPlaceCoordinates
             ?deathPlace ?deathPlaceLabel ?deathPlaceID ?deathPlaceCoordinates
    """

    try:
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        humans_to_create = []
        humans_to_update = []
        genders_to_create = set()
        places_to_create = set()

        existing_humans = set(
            NotableHuman.objects.values_list("wikidata_id", flat=True),
        )

        for result in results["results"]["bindings"]:
            wikidata_id = result["item"]["value"].split("/")[-1]
            wikipedia_url = result.get("article", {}).get("value").split("/")[-1]
            name = result["itemLabel"]["value"]
            birth_date = result.get("dob", {}).get("value")
            death_date = result.get("dod", {}).get("value")
            birth_place_id = result.get("birthPlaceID", {}).get("value")
            death_place_id = result.get("deathPlaceID", {}).get("value")
            birth_place = result.get("birthPlaceLabel", {}).get("value")
            death_place = result.get("deathPlaceLabel", {}).get("value")
            birth_place_coordinates = result.get("birthPlaceCoordinates", {}).get(
                "value",
            )
            death_place_coordinates = result.get("deathPlaceCoordinates", {}).get(
                "value",
            )
            gender_data = result.get("genderData", {}).get("value")
            # Parse and store gender data
            # gender_instances = parse_and_store_m2m_field(
            #     gender_data,
            #     Gender,
            #     id_field="wikidata_id",
            #     label_field="label"
            # )

            print(gender_data)
            return

    except Exception as e:
        print(f"Error fetching SPARQL data: {e}")
        return


@shared_task
def schedule_data_collection():
    """
    Schedule tasks to fetch Wikipedia article content for all days of the year
    using the Wikipedia API.
    """
    tasks = [
        get_linked_titles_from_day.s(month, day)
        for month in [
            "January",  # "February", "March", "April", "May", "June",
            # "July", "August", "September", "October", "November", "December"
        ]
        for day in range(1, 2)
    ]

    # Distribute tasks as a group
    group(tasks).apply_async()

    return "Scheduled fetching tasks for all days of the year."
