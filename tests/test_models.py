from datetime import date

from django.core.exceptions import ValidationError
from django.utils.timezone import now

from notablehumans.data_collection.models import AttributeType
from notablehumans.data_collection.models import NotableHuman
from notablehumans.data_collection.models import NotableHumanAttribute
from notablehumans.data_collection.models import Place


# Test for required fields: wikidata_id is required.
def test_required_fields():
    """Test that wikidata_id is required"""
    human = NotableHuman(name="Test Human")  # Missing wikidata_id
    try:
        human.full_clean()  # Trigger validation
    except ValidationError:
        pass
    else:
        raise AssertionError("Expected ValidationError when wikidata_id is missing.")


# Test for the string representation of NotableHuman
def test_str_representation():
    """Test the string representation of NotableHuman"""
    human = NotableHuman(wikidata_id="Q12345", name="Albert Einstein")
    expected_str = "Albert Einstein (Q12345)"
    assert str(human) == expected_str, f"Expected '{expected_str}', got '{human!s}'."

    human_without_name = NotableHuman(wikidata_id="Q67890")
    expected_str_no_name = "Unknown (Q67890)"
    assert str(human_without_name) == expected_str_no_name, (
        f"Expected '{expected_str_no_name}', got '{human_without_name!s}'."
    )


# Test date fields (birth_date and death_date)
def test_date_fields():
    """Test birth_date and death_date behavior"""
    human = NotableHuman(
        wikidata_id="Q123",
        birth_date=date(1879, 3, 14),
        death_date=date(1955, 4, 18),
    )
    assert human.birth_date.year == 1879, f"Expected birth_date year 1879, got {human.birth_date.year}."
    assert human.death_date.year == 1955, f"Expected death_date year 1955, got {human.death_date.year}."


# Test relationships such as foreign keys and many-to-many associations
def test_relationships():
    """Test foreign key and many-to-many relationships"""
    place = Place.objects.create(name="Ulm, Germany")
    human = NotableHuman.objects.create(wikidata_id="Q12345", birth_place=place, last_wikidata_update=now())
    assert human.birth_place.name == "Ulm, Germany", (
        f"Expected birth_place.name to be 'Ulm, Germany', got '{human.birth_place.name}'."
    )

    attr1 = NotableHumanAttribute.objects.create(
        wikidata_id="Q12345678", label="Physicist", category=AttributeType.OCCUPATION
    )
    attr2 = NotableHumanAttribute.objects.create(
        wikidata_id="Q98765432", label="Nobel Laureate", category=AttributeType.OCCUPATION
    )
    human.attributes.add(attr1, attr2)
    assert human.attributes.count() == 2, f"Expected attribute count to be 2, got {human.attributes.count()}."


# Test auto_now_add functionality for created_at field
def test_auto_now_add_fields():
    """Test that created_at is set automatically"""
    human = NotableHuman.objects.create(wikidata_id="Q98765", last_wikidata_update=now())
    assert human.created_at is not None, "Expected created_at to be set automatically."
    # Allow a delta of one second between created_at and now()
    time_diff = abs((human.created_at - now()).total_seconds())
    assert time_diff <= 1, f"Expected created_at to be within 1 second of now; difference was {time_diff} seconds."


# Test creating a Place with minimal fields
def test_create_place_minimal_fields():
    """Test creating a Place with only required fields"""
    place = Place.objects.create(wikidata_id="Q123", name="Pittsburgh")
    assert place.name == "Pittsburgh", f"Expected place.name to be 'Pittsburgh', got '{place.name}'."
    assert place.wikidata_id == "Q123", f"Expected place.wikidata_id to be 'Q123', got '{place.wikidata_id}'."
    assert place.latitude is None, f"Expected place.latitude to be None, got {place.latitude}."
    assert place.longitude is None, f"Expected place.longitude to be None, got {place.longitude}."


# Test auto_now behavior for a Place's last_updated field
def test_last_updated_auto_now():
    """Test that last_updated defaults to current time"""
    place = Place.objects.create(wikidata_id="Q123", name="Los Angeles")
    time_diff = abs((place.last_updated - now()).total_seconds())
    assert time_diff <= 1, f"Expected last_updated to be within 1 second of now; difference was {time_diff} seconds."


# Test the string representation of a Place
def test_string_representation_place():
    """Test that __str__ returns the name"""
    place = Place.objects.create(wikidata_id="Q123", name="Chicago")
    assert str(place) == "Chicago", f"Expected str(place) to be 'Chicago', got '{place!s}'."


# Test creating a NotableHumanAttribute with valid data
def test_create_attribute():
    """Test creating a NotableHumanAttribute with valid data"""
    attribute = NotableHumanAttribute.objects.create(
        wikidata_id="Q123", label="Mathematician", category=AttributeType.OCCUPATION
    )
    assert attribute.label == "Mathematician", (
        f"Expected attribute.label to be 'Mathematician', got '{attribute.label}'."
    )
    assert attribute.category == AttributeType.OCCUPATION, (
        f"Expected attribute.category to be {AttributeType.OCCUPATION}, got {attribute.category}."
    )


# Test that invalid category choices raise a ValidationError
def test_category_choices():
    """Test that category must be a valid choice"""
    try:
        NotableHumanAttribute.objects.create(wikidata_id="Q124", label="Invalid Category", category="NotAValidChoice")
    except ValidationError:
        pass
    else:
        raise AssertionError("Expected ValidationError for invalid category, but no error was raised.")


# Test auto_now behavior for a NotableHumanAttribute's last_updated field
def test_last_updated_auto_now_attribute():
    """Test that last_updated defaults to current time for attribute"""
    attribute = NotableHumanAttribute.objects.create(
        wikidata_id="Q125", label="Philosopher", category=AttributeType.OCCUPATION
    )
    time_diff = abs((attribute.last_updated - now()).total_seconds())
    assert time_diff <= 1, (
        f"Expected attribute.last_updated to be within 1 second of now; difference was {time_diff} seconds."
    )


# Test string representation of a NotableHumanAttribute
def test_string_representation_attribute():
    """Test that __str__ returns the expected format"""
    attribute = NotableHumanAttribute.objects.create(
        wikidata_id="Q126", label="Physicist", category=AttributeType.OCCUPATION
    )
    expected_str = f"Q126: Physicist ({attribute.get_category_display()})"
    assert str(attribute) == expected_str, f"Expected string representation '{expected_str}', got '{attribute!s}'."


# Test that wikidata_id must be unique for NotableHumanAttribute
def test_unique_wikidata_id():
    """Test that wikidata_id must be unique"""
    NotableHumanAttribute.objects.create(wikidata_id="Q127", label="Scientist", category=AttributeType.OCCUPATION)
    try:
        NotableHumanAttribute.objects.create(wikidata_id="Q127", label="Biologist", category=AttributeType.OCCUPATION)
    except Exception:
        pass
    else:
        raise AssertionError("Expected an exception for duplicate wikidata_id, but none was raised.")
