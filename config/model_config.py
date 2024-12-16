from sfn_blueprint import MODEL_CONFIG

MODEL_CONFIG["aggregation_advisor"] = {
    "model": "gpt-4",
    "temperature": 0.3,
    "max_tokens": 2000,
    "n": 1,
    "stop": None
}

MODEL_CONFIG["aggregation_column_mapping"] = {
    "model": "gpt-4",
    "temperature": 0.3,
    "max_tokens": 500,
    "n": 1,
    "stop": None
}

MODEL_CONFIG["clean_suggestions_generator"] = {
    "model": "gpt-4o-mini", #"gpt-3.5-turbo",
    "temperature": 0.5,
    "max_tokens": 500,
    "n": 1,
    "stop": None
}

MODEL_CONFIG["join_suggestions_generator"] = {
    "model": "gpt-4o-mini",
    "temperature": 0.3,
    "max_tokens": 1000,
    "n": 1,
    "stop": None
}

MODEL_CONFIG["category_identifier"] = {
        "model": "gpt-4o-mini",
        "temperature": 0.3,
        "max_tokens": 100,
        "n": 1,
        "stop": None

}

MODEL_CONFIG["column_mapper"] = {
        "model": "gpt-4o-mini",
        "temperature": 0.3,
        "max_tokens": 500,
        "n": 1,
        "stop": None
}