def remove_duplicates(input_file, output_file):
    unique_lines = set()
    header = ""

    # check input file is exist or not, if not, create a new file
    try:
        infile = open(input_file, 'r')
    except FileNotFoundError:
        print("mywords.txt is not exist, create and setup a new dictionary file")
        with open(output_file, 'w', encoding='ascii') as outfile:
            outfile.write("personal_ws-1.1 en 30 utf-8\n")
            return

    with open(input_file, 'r') as infile:
        # check the file is empty or not
        first_line = infile.readline()
        if first_line  == "":
            print("mywords.txt is empty, setup a new dictionary")
            with open(output_file, 'w', encoding='ascii') as outfile:
                outfile.write("personal_ws-1.1 en 30 utf-8\n")
                return

        header = first_line
        if header != "personal_ws-1.1 en 30 utf-8\n":
            if header[-1] == "\n":
                header = header.strip()
            print(f"{header} is not correct format, set a header as 'personal_ws-1.1 en 30 utf-8'")
            unique_lines.add(header)
            header = "personal_ws-1.1 en 30 utf-8\n"

        for line in infile:
            line = line.strip()  # Remove leading/trailing whitespace and newline characters
            if line not in unique_lines:
                unique_lines.add(line)


    # output file is opened in write mode with utf-8 encoding
    with open(output_file, 'w', encoding='ascii') as outfile:
        outfile.write(header)  # Write the header back to the output file
        # sort unique_lines with alphabetically
        unique_lines = sorted(unique_lines)
        for unique_line in unique_lines:
            if unique_line == "":
                continue
            # check the part of unique line is "Checking typos in:"
            if unique_line[0:18] == "Checking typos in:":
                continue
            outfile.write(unique_line + "\n")


if __name__ == "__main__":
    input_file = "mywords.txt"  # Change this to the path of your input file
    output_file = "mywords.txt"  # Change this to the path of your output file

    remove_duplicates(input_file, output_file)
    print("Redundant lines removed and unique lines saved to", output_file)

