from datetime import datetime

from django.utils.timezone import get_current_timezone


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


def parse_coordinates(coord_string):
    """
    Parse coordinate string into latitude and longitude.
    """
    if coord_string:
        coord_parts = coord_string.replace("Point(", "").replace(")", "").split()
        return float(coord_parts[1]), float(coord_parts[0])
    return None, None


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
