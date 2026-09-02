"""
Tests our ability to serialize/deserialize the background settings.
"""


def test_background_settings_full(test_server):
    from librarian_background.settings import BackgroundSettings

    BackgroundSettings.model_validate(
        {
            "check_integrity": [
                {
                    "task_name": "check",
                    "every": "01:00:00",
                    "age_in_days": 7,
                    "store_name": "test",
                }
            ],
            "create_archive": [
                {
                    "task_name": "archive",
                    "every": "24:00:00",
                    "archivist_name": "test_archivist",
                    "age_in_days": 30,
                    "filesize_per_run": 1099511627776,
                    "match_query": "%.g3",
                }
            ],
            "create_local_clone": [
                {
                    "task_name": "clone",
                    "every": "22:23:02",
                    "age_in_days": 7,
                    "clone_from": "test",
                    "clone_to": "test",
                }
            ],
        }
    )


def test_background_settings_empty(test_server):
    from librarian_background.settings import BackgroundSettings

    BackgroundSettings()
