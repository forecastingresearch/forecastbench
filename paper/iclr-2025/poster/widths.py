"""Calculate the widths of the poster columns given the number of entries in the sizes dict."""

sizes = {
    1: {
        "name": "one",
    },
    2: {
        "name": "two",
    },
    3: {
        "name": "three",
    },
    4: {
        "name": "four",
    },
    5: {
        "name": "five",
    },
}
sepwid = 0.024
print(r"\setlength{\sepwid}{" + str(sepwid) + r"\paperwidth}")
n = len(sizes)
for key in sizes:
    sizes[key]["width"] = (
        1 / n * (1 - (n + 1) * sepwid) if key == 1 else key * sizes[1]["width"] + (key - 1) * sepwid
    )
    print("\\newlength{\\" + sizes[key]["name"] + "colwid}")
    print(
        "\\setlength{\\"
        + sizes[key]["name"]
        + r"colwid}{"
        + str(round(sizes[key]["width"], 3))
        + r"\paperwidth}"
    )
