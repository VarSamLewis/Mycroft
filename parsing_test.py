import json
import sys
from time import time
from typing import Dict

import test_strings
from src.backend.parsing import extract_graph, extract_graph_from_python


def test_parse(name: str, input: str) -> Dict:
    str_length: int = len(input)
    start: float = time()

    if name.startswith("PYSPARK") or name.startswith("MIXED"):
        result = extract_graph_from_python(input)
    else:
        result = extract_graph(input)

    end: float = time() - start
    return {
        "input_name": name,
        "time_taken": end,
        "num_chars": str_length,
        "chars_per_sec": str_length / end if end > 0 else 0,
        "mb_per_sec": sys.getsizeof(input) / end if end > 0 else 0,
    }


if __name__ == "__main__":
    all_tests = [
        ("SQL_1", test_strings.SQL_1),
        ("SQL_2", test_strings.SQL_2),
        ("SQL_3", test_strings.SQL_3),
        ("SQL_4", test_strings.SQL_4),
        ("SQL_5", test_strings.SQL_5),
        ("PYSPARK_1", test_strings.PYSPARK_1),
        ("PYSPARK_2", test_strings.PYSPARK_2),
        ("PYSPARK_3", test_strings.PYSPARK_3),
        ("MIXED_1", test_strings.MIXED_1),
        ("MIXED_2", test_strings.MIXED_2),
    ]

    results = []
    for name, input_str in all_tests:
        result = test_parse(name, input_str)
        results.append(result)
        print(f"{name}: {result['time_taken']:.4f}s, {result['num_chars']} chars")

    print("\n" + "=" * 50)
    print(json.dumps(results, indent=2))
