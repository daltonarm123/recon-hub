import unittest

import nw_poll
import rankings_poll


class RankingsPipelineTests(unittest.TestCase):
    def test_payload_builder_preserves_zero_start_number(self):
        payload = rankings_poll._build_rankings_payload(
            account_id=123,
            token="abc",
            kingdom_id=456,
            continent_id=-1,
            start_number=0,
        )

        self.assertEqual(payload["startNumber"], 0)
        self.assertEqual(payload["accountId"], 123)
        self.assertEqual(payload["kingdomId"], 456)
        self.assertEqual(payload["continentId"], -1)

    def test_rankings_offsets_top_100(self):
        self.assertEqual(rankings_poll._rankings_offsets(top_n=100, page_size=20), [0, 20, 40, 60, 80])

    def test_merge_dedup_keeps_best_rank(self):
        merged = rankings_poll._merge_rankings_pages(
            [
                {"kingdom_id": 1, "kingdom": "A", "ranking": 10, "networth": 5000},
                {"kingdom_id": 2, "kingdom": "B", "ranking": 5, "networth": 9000},
                {"kingdom_id": 1, "kingdom": "A", "ranking": 8, "networth": 5100},
                {"kingdom_id": 2, "kingdom": "B", "ranking": 7, "networth": 8900},
            ],
            limit=100,
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["kingdom_id"], 2)
        self.assertEqual(merged[0]["ranking"], 5)
        self.assertEqual(merged[1]["kingdom_id"], 1)
        self.assertEqual(merged[1]["ranking"], 8)

    def test_delta_calculation_between_snapshots(self):
        snapshot = [
            (101, "Alpha", 1, 25000),
            (202, "Beta", 2, 12000),
            (303, "Gamma", 3, 7500),
        ]
        previous = {101: 23000, 202: 13000}

        rows = nw_poll._calculate_deltas(snapshot, previous)
        delta_map = {r[0]: r[4] for r in rows}

        self.assertEqual(delta_map[101], 2000)
        self.assertEqual(delta_map[202], -1000)
        self.assertEqual(delta_map[303], 0)


if __name__ == "__main__":
    unittest.main()
