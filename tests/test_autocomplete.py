from onprem_recommenders.services.autocomplete import AutocompleteIndex, SuggestionRow


def test_prefix_and_category_boost():
    index = AutocompleteIndex()
    index._rows = [
        SuggestionRow("smart plug", 139, ""),
        SuggestionRow("smart speaker", 10, "Electronics > Audio"),
        SuggestionRow("smartphone case", 110, ""),
        SuggestionRow("hdmi cable", 107, ""),
    ]

    anonymous, personalized = index.suggest("sm", limit=5)
    assert personalized is False
    assert [row.query_text for row in anonymous] == ["smart plug", "smartphone case"]

    known, is_personalized = index.suggest(
        "sm", user_categories=["Electronics > Audio"], limit=5
    )
    assert is_personalized is True
    assert known[0].query_text == "smart speaker"
    assert known[0].category_match == "Electronics > Audio"


def test_empty_prefix():
    index = AutocompleteIndex()
    index._rows = [SuggestionRow("smart plug", 1, "")]
    suggestions, personalized = index.suggest("   ")
    assert suggestions == []
    assert personalized is False
