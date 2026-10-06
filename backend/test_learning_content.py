import json
import re
import unittest
from collections import Counter
from pathlib import Path


SEED_PATH = Path(__file__).with_name("learning.seed.json")
REQUIRED_CATEGORIES = {
    "Measurement & Data",
    "Engineering Practice",
    "Sustainable Systems",
    "Academic Communication",
}
REQUIRED_FIELDS = {
    "id",
    "deck",
    "category",
    "skill",
    "prompt",
    "answer",
    "example",
    "explanation",
    "source",
    "tags",
}
ALLOWED_SKILLS = {"recognition", "production", "cloze"}
GERMAN_FIELD_NAMES = {"german", "translation_de", "deutsch"}
GERMAN_MARKERS = re.compile(
    r"\b(?:der|die|das|eine|einer|einem|einen|und|oder|ist|sind|mit|für|"
    r"beispiel|bedeutung|übersetzung|erklärung)\b",
    flags=re.IGNORECASE,
)


def load_seed():
    if not SEED_PATH.exists():
        raise AssertionError(f"missing learning seed: {SEED_PATH}")
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


class LearningContentTests(unittest.TestCase):
    def test_seed_contains_exactly_120_unique_english_cards(self):
        cards = load_seed()

        self.assertEqual(120, len(cards))
        ids = [card["id"] for card in cards]
        self.assertEqual(120, len(set(ids)))
        self.assertEqual(
            [f"eng-sse-{index:03d}" for index in range(1, 121)],
            ids,
        )
        for card in cards:
            self.assertFalse(GERMAN_FIELD_NAMES.intersection(card))
            text = " ".join(
                str(card.get(field, ""))
                for field in ("prompt", "answer", "example", "explanation")
            )
            self.assertIsNone(
                GERMAN_MARKERS.search(text),
                msg=f"German marker found in {card['id']}: {text}",
            )
            self.assertNotRegex(text, r"[äöüÄÖÜß]")

    def test_seed_balances_required_categories(self):
        cards = load_seed()

        counts = Counter(card["category"] for card in cards)
        self.assertEqual({category: 30 for category in REQUIRED_CATEGORIES}, dict(counts))

    def test_seed_records_have_complete_fields_and_tags(self):
        cards = load_seed()

        for card in cards:
            self.assertTrue(REQUIRED_FIELDS.issubset(card), msg=card.get("id"))
            self.assertEqual("Engineering English", card["deck"])
            self.assertIn(card["category"], REQUIRED_CATEGORIES)
            self.assertIn(card["skill"], ALLOWED_SKILLS)
            for field in REQUIRED_FIELDS - {"tags"}:
                self.assertIsInstance(card[field], str, msg=f"{card['id']}:{field}")
                self.assertTrue(card[field].strip(), msg=f"{card['id']}:{field}")
            self.assertIsInstance(card["tags"], list, msg=card["id"])
            self.assertGreaterEqual(len(card["tags"]), 2, msg=card["id"])
            self.assertEqual(len(card["tags"]), len(set(card["tags"])), msg=card["id"])
            self.assertTrue(all(isinstance(tag, str) and tag.strip() for tag in card["tags"]))
            self.assertEqual("Holocron Engineering English v1", card["source"])


if __name__ == "__main__":
    unittest.main()
