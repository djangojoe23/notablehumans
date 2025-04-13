from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils.timezone import get_default_timezone
from django.utils.timezone import make_aware
from django.utils.timezone import now

from notablehumans.data_collection.models import NotableHuman
from notablehumans.data_collection.tasks import fetch_wikipedia_metadata
from notablehumans.data_collection.tasks import schedule_wikipedia_data_collection


class WikipediaDataCollectionTests(TestCase):
    @patch("notablehumans.data_collection.tasks.fetch_wikipedia_metadata")
    def test_schedule_wikipedia_data_collection(self, mock_fetch_wikipedia_metadata):
        # Create test NotableHuman
        human = NotableHuman.objects.create(
            wikidata_id="Q12345",
            wikipedia_url="https://en.wikipedia.org/wiki/Some_Human",
            last_wikidata_update=now() - timedelta(minutes=3),  # Simulating old last update
            last_wikipedia_update=now() - timedelta(minutes=3),  # Simulating old last update
        )

        expected_created_date = make_aware(datetime(2021, 1, 1, 0, 0, 0), timezone=get_default_timezone())

        # Mock the fetch_wikipedia_metadata method
        mock_fetch_wikipedia_metadata.return_value = (
            "Some_Human",
            {
                "description": "A notable human",
                "page_length": 1000,
                "page_views_30_days": 5000,
                "edit_count": 150,
                "recent_edits_30_days": 10,
                "good_article": True,
                "created_date": expected_created_date,
            },
        )

        # Run the task
        schedule_wikipedia_data_collection()

        # Fetch the updated NotableHuman
        human.refresh_from_db()

        # Assert the human object was updated
        assert human.description == "A notable human", (
            f"Expected human.description to be 'A notable human', but got {human.description}"
        )
        assert human.article_length == 1000, (
            f"Expected human.article_length to be 1000, but got {human.article_length}"
        )
        assert human.article_recent_views == 5000, (
            f"Expected human.article_recent_views to be 5000, but got {human.article_recent_views}"
        )
        assert human.article_total_edits == 150, (
            f"Expected human.article_total_edits to be 150, but got {human.article_total_edits}"
        )
        assert human.article_recent_edits == 10, (
            f"Expected human.article_recent_edits to be 10, but got {human.article_recent_edits}"
        )
        assert human.article_quality == NotableHuman.GOOD_ARTICLE, (
            f"Expected human.article_quality to be {NotableHuman.GOOD_ARTICLE}, but got {human.article_quality}"
        )
        assert human.article_created_date == expected_created_date, (
            f"Expected human.article_created_date to be {expected_created_date}, but got {human.article_created_date}"
        )


class FetchWikipediaMetadataTests(TestCase):
    @patch("notablehumans.data_collection.tasks.requests.get")
    def test_fetch_wikipedia_metadata(self, mock_get):
        # Mock the response from Wikipedia
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.text = (
            "<html><body><tr id='mw-pageinfo-length'><td>Page Length</td><td>1000</td></tr></body></html>"
        )

        # Test the metadata fetching
        page_title, metadata = fetch_wikipedia_metadata("Some_Human")

        # Assert the correct data is extracted
        assert page_title == "Some_Human", f"Expected 'Some_Human', got {page_title}"
        assert metadata["page_length"] == 1000, (
            f"Expected metadata['page_length'] to be 1000 but got {metadata['page_length']}"
        )
