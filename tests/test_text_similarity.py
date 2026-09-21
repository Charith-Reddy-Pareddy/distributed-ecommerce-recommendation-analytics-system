from experiments.recommendation.text_similarity import add_vectors, build_tfidf, cosine, tokenize


def test_tokenize_lowercases_and_splits_on_punctuation():
    assert tokenize("Guitar Strings, D'Addario!") == ["guitar", "strings", "d", "addario"]


def test_build_tfidf_gives_common_terms_lower_weight_than_rare_ones():
    docs = {1: "guitar string acoustic", 2: "guitar amp electric", 3: "guitar pick"}
    vectors = build_tfidf(docs)
    # "guitar" appears in every doc (low idf); "acoustic" appears in one (high idf)
    assert vectors[1]["acoustic"] > vectors[1]["guitar"]


def test_cosine_one_for_identical_vectors():
    v = {"a": 1.0, "b": 2.0}
    assert abs(cosine(v, v) - 1.0) < 1e-9


def test_cosine_zero_for_disjoint_vectors():
    assert cosine({"a": 1.0}, {"b": 1.0}) == 0.0


def test_cosine_zero_for_empty_vector():
    assert cosine({}, {"a": 1.0}) == 0.0


def test_add_vectors_weights_each_input():
    profile = add_vectors([{"a": 1.0}, {"a": 1.0, "b": 2.0}], weights=[1.0, 2.0])
    assert profile == {"a": 3.0, "b": 4.0}
