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


def parse_and_store_m2m_field(
    data,
    model,
    id_field="id",
    label_field="label",
):
    """
    Parses concatenated field data and stores it in the specified model.

    Args:
        data (str): Concatenated string of data in the format `id||label|id||label`.
        model (Model): The Django model class to store the data.
        id_field (str): The field in the model representing the unique ID.
        label_field (str): The field in the model representing the label.
        delimiter (str): The delimiter between multiple entries.
        pair_separator (str): The separator between ID and label in each entry.

    Returns:
        list: A list of model instances created or fetched.
    """
    delimiter = "|"
    pair_separator = "||"
    instances = []
    if data:
        items = data.split(delimiter)
        for item in items:
            try:
                item_id, item_label = item.split(pair_separator)
                instance, _ = model.objects.get_or_create(
                    **{id_field: item_id},
                    defaults={label_field: item_label},
                )
                instances.append(instance)
            except ValueError:
                continue  # Skip malformed entries
    return instances
