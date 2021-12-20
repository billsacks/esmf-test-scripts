def sanitize_test_results(values):
    results = []
    for value in values:
        f_value = value.lower().strip()
        print("f_value: ", f_value)
        if f_value == "-1":
            results.append("\tQUEUED")
            continue
        if f_value == "fail":
            results.append("\tFAIL")
            continue
        try:
            int(f_value)
        except ValueError:
            # Don't raise an exception but warn in the log
            print(f"WARNING: Unknown value type {value}")
        results.append(value)
    return results


def testSanitizeResults_ReturnsCorrectValues():
    a, b, c, d, e, f, g = sanitize_test_results(
        ["-1", "FAIL", "fail", "Fail", "2", "4", "bumble"]
    )
    assert a is "\tQUEUED"
    assert b is "\tFAIL"
    assert c is "\tFAIL"
    assert d is "\tFAIL"
    assert e is "2"
    assert f is "4"
    assert g is "bumble"
