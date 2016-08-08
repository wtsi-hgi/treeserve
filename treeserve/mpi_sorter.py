import base64
import csv


def slash_count(row):
    return base64.b64decode(row[0]).decode().strip("/").count("/")


def sort(f_obj):
    reader = csv.reader(f_obj, delimiter="\t")
    sorted_rows = sorted((row for row in reader), key=slash_count, reverse=True)
    return sorted_rows


if __name__ == "__main__":
    with open("samples/largesampledata.dat") as f:
        sorted_rows = sort(f)
    with open("samples/largesampledata_sorted.dat", "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerows(sorted_rows)
