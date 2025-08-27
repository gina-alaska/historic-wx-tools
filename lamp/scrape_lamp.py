import argparse


def parse_lamp_guidance_file(input_file_path, output_csv_path):
    import csv

    with open(input_file_path, "r") as file:
        lines = [line.strip() for line in file if line.strip()]

    data = []
    i = 0
    while i < len(lines):
        if "GFS LAMP" in lines[i]:
            station_info = lines[i].split()
            station = station_info[0]
            date = station_info[-3]
            time = station_info[-2] + " " + station_info[-1]

            utc_line = lines[i + 1].split()
            hours = utc_line[1:]

            vars = {}
            next_line = 2
            while i + next_line < len(lines) and "GFS LAMP" not in lines[i + next_line]:
                var_line = lines[i + next_line]
                if var_line:
                    var = var_line[:3]
                    trimmed = var_line[4:]
                    vars[var] = [trimmed[i : i + 3] for i in range(0, len(trimmed), 3)]
                next_line += 1

            for j in range(len(hours)):
                row = {
                    "Station": station,
                    "Date": date,
                    "Time": time,
                    "FxTime": hours[j],
                }
                for var, values in vars.items():
                    row[var] = values[j] if j < len(values) else None
                data.append(row)

            i += next_line
        else:
            i += 1

    all_vars = set()
    for row in data:
        all_vars.update(row.keys())
    fieldnames = ["Station", "Date", "Time", "FxTime"] + sorted(
        v for v in all_vars if v not in {"Station", "Date", "Time", "FxTime"}
    )

    with open(output_csv_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse LAMP guidance file and export to CSV."
    )
    parser.add_argument("input_file", help="Path to the input text file")
    parser.add_argument("output_file", help="Path to the output CSV file")
    args = parser.parse_args()

    parse_lamp_guidance_file(args.input_file, args.output_file)
