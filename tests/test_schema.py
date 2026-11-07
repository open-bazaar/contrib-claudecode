"""The configuration form the server draws from the schema."""

from bazaar_contrib_claude_code import Config


def test_every_field_is_worded_in_both_languages_and_models_come_from_inspect():
    schema = Config.model_json_schema()
    assert {"en", "zh-CN"} <= schema["x-locales"].keys()
    for texts in schema["x-locales"].values():
        for name in schema["properties"]:
            assert texts[f"{name}.title"] and texts[f"{name}.description"]
            assert texts[f"{name}.empty"]
    assert schema["properties"]["api_key"]["writeOnly"] is True
    assert schema["properties"]["bedrock_api_key"]["writeOnly"] is True
    assert schema["properties"]["model"]["x-inspect"] == "models"
    assert schema["properties"]["effort"]["x-inspect"] == "efforts"
