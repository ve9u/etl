from extract import connect_db, extract_data


def main():
    """main method starts a pipeline, extracts data,
    transforms it and loads it into a mongo client"""
    # print ("Coding start from here")

    # Connect to the database
    mysql = connect_db()

    #Extract all prdoucts informations
    cur = extract_data(mysql, "SELECT * FROM products")

    for i in range(0, cur.rowcount):
        row = cur.fetchone()
        print(row['productCode'], row['productName'])

    print("dummy")

main()