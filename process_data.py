import pandas as pd
from neo4j import GraphDatabase


def get_formatted_row(line):
    row = line.split('|,|')
    row[0] = ''.join(list(row[0])[1:])
    row[-1] = ''.join(list(row[-1])[:-1])
    return row


def get_formatted_row_w_numeric(line):
    split_row = line.split(',')
    row = []
    merging = False
    for s in split_row:
        list_s = list(s)
        if s == "||" or s == "":
            row.append("")
        elif merging:
            if list_s[-1] == "|":
                row[len(row)-1] = row[len(row)-1] + ''.join(list_s[:-1])
                merging = False
            else:
                row[len(row)-1] = row[len(row)-1] + s
        elif list_s[0] == "|" and list_s[-1] == "|":
            row.append(''.join(list_s[1:-1]))
        elif list_s[0] != "|" and list_s[-1] != "|":
            row.append(s)
        elif list(s)[0] == "|" and not list(s)[-1] == "|":
            row.append(''.join(list_s[1:]))
            merging = True
        else:
            print("Error, unkown: " + s)
    return row


def read_in_file(file_name, col_names, has_numeric=False):
    data_array = []
    with open(file_name) as f:
        data = f.read().splitlines()
        offset = 0
        for i in range(len(data)):
            if not data[i]:
                pass  # empty line
            elif list(data[i])[0] != "|":
                data_array[len(data_array) - 1] = ''.join([data_array[len(data_array) - 1], data[i]])
            else:
                data_array.append(data[i])
        if has_numeric:
            formatted_data = list(map(get_formatted_row_w_numeric, data_array))
        else:
            formatted_data = list(map(get_formatted_row, data_array))
        return pd.DataFrame(formatted_data, columns=col_names)


# lobbyists_df = read_in_file("data/Lobby/lob_lobbyist.txt", [
#                             "UniqID", "Lobbyist_raw", "Lobbyist", "Lobbyist_id", "Year", "OfficialPosition", "CID", "Formercongmem"])
# print(lobbyists_df)

lobbying_df = read_in_file("data/Lobby/lob_lobbying.txt", ["UniqID", "Registrant_raw", "Registrant", "IsFirm", "Client_raw", "Client",
                           "Ultorg", "Amount", "Catcode", "Source", "Self", "IncludeNSFS", "Use", "Ind", "Year", "Type", "Typelong", "Affiliate"], True)
print(lobbying_df)

issue_df = read_in_file("data/Lobby/lob_issue.txt",
                        ["SI_ID", "UniqID", "IssueID", "Issue", "SpecificIssue", "Year"])
print(issue_df)

issue_NoSpecificIssue_df = read_in_file("data/Lobby/lob_issue_NoSpecificIssue.txt", [
                                        "SI_ID", "UniqID", "IssueID", "Issue", "SpecificIssue", "Year"])
print(issue_NoSpecificIssue_df)

agency_df = read_in_file("data/Lobby/lob_agency.txt",
                         ["UniqID", "AgencyID", "Agency"])
print(agency_df)

bills_df = read_in_file("data/Lobby/lob_bills.txt",
                        ["B_ID", "SI_ID", "CongNo", "Bill_Name"])
print(bills_df)

indus_df = read_in_file("data/Lobby/lob_indus.txt",
                        ["Client", "Sub", "Total", "Year", "Catcode"])
