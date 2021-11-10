from extract import connect_db, extract_data
from load import connect_mongodb, load_data


def main():
    """main method starts a pipeline, extracts data,
    transforms it and loads it into a mongo client
    """
    # print ("Coding start from here")

    # Connect to the database
    mysql = connect_db()
    mongodb = connect_mongodb()

    #Extract all prdoucts informations
    cur = extract_data(mysql, "SELECT * FROM products")
    collection_name = "products";  

    for i in range(0, cur.rowcount):
        fetched_row = cur.fetchone()
        object_id = load_data(mongodb, collection_name, fetched_row)
        print(object_id.acknowledged)

main()