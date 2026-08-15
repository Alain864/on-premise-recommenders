from onprem_recommenders.ranking import affinity_score, apply_personalization, compute_ranking_score


def test_affinity_weights():
    assert affinity_score(1, 0, 0) == 3.0
    assert affinity_score(0, 1, 0) == 2.0
    assert affinity_score(0, 0, 1) == 1.0
    assert affinity_score(2, 1, 3) == 11.0


def test_ranking_prefers_relevance_and_stock():
    in_stock = compute_ranking_score(1.0, 0.0, 0.0, 3.0, True)
    out_of_stock = compute_ranking_score(1.0, 0.0, 0.0, 3.0, False)
    assert in_stock > out_of_stock

    relevant = compute_ranking_score(1.0, 0.0, 0.0, 3.0, True)
    popular_only = compute_ranking_score(0.0, 5.0, 0.0, 3.0, True)
    assert relevant > popular_only


def test_personalization_boosts_matching_category():
    products = [
        {"category_path": "Electronics > Headphones", "final_score": 1.0},
        {"category_path": "Electronics > TVs", "final_score": 1.0},
    ]
    apply_personalization(products, [("Electronics > Headphones", 20.0)])
    assert products[0]["final_score"] > products[1]["final_score"]
