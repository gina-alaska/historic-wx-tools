import re


def extract_and_sort_hours(file_path):
    pattern = r"akurma\.(\d{8}): \{([^\}]+)\}"

    with open(file_path, "r") as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                date = match.group(1)
                hours_str = match.group(2)
                hours = sorted(
                    hours_str.replace("'", "").split(", "), key=lambda x: int(x)
                )
                print(f"{date}: {hours}")


if __name__ == "__main__":

    file_path = "./extract_urma_pts.log"
    extract_and_sort_hours(file_path)
